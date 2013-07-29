package com.emar.recsys.user.zonerank;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.emar.recsys.user.log.LogParse;
import com.emar.recsys.user.util.UtilStr;

/**
 * 基于 无具体消费行为的uid:[ip,zone], ip:rank(优先, 第一次执行)或zone:rank(次优, 第二次执行) 生成完整的uid:
 * mrank信息； 没有链接上的uid继续输出。
 * 
 * @in uid-[ip,zone]
 * @in 过滤器输入 ip-rank 或 zone-rank.
 * @out 1. uid-rank, 2. 没有匹配到的uid-[ip, zone] 继续为下一步处理
 * 
 * @after GLogIP
 * @author zhoulm
 * 
 */
public class GUserExtend extends Configured implements Tool {
	// 定义Map & Reduce 中通用的对象
	private static final String SEPA = LogParse.SEPA,
			SEPA_MR = LogParse.SEPA_MR, SEMP = "";
	private static final String ConfFilter = "filterpath";
	public static final String PUid2IP = "", PIP = "", PZone = "",
			PUid2Rank = "", Hit = "hit", PHit = "uid_hit/", Unhit = "unhit",
			PUnhit = "uid_unhit/"; // 若干文件路径中的关键字

	public static class MapCProduce extends
			Mapper<LongWritable, Text, Text, Text> {

		private HashMap<String, String> ipfilter;
		private Text okey = new Text(), oval = new Text();
		private MultipleOutputs<Text, Text> mos;

		private static enum Counters {
			MoHit, MoUnhit,
			ErrMo
		};

		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			mos = new MultipleOutputs(context);
			ipfilter = new HashMap<String, String>(1000, 0.95f);

			// 将 ip:rank 或 zone:rank 加载到ipfilter。
			FSDataInputStream in;
			String line;
			String[] atom;
//			System.out.println("[Info] GUserExtend::setup() input=" + conf.get(ConfFilter));
			Path cpath = new Path(conf.get(ConfFilter));
			FileSystem cfs = FileSystem.get(conf);
			FileStatus[] cfstat = cfs.globStatus(cpath);
			for(FileStatus cfsi: cfstat) {
				in = cfs.open(cfsi.getPath());
				while ((line = in.readLine()) != null) {
					atom = line.trim().split(SEPA_MR);
					ipfilter.put(atom[0], atom[1]);
				}
				in.close();
//				System.out.println("[Info] GUserExtend::setup() " + cfsi.getPath().toString());
			}
			
			System.out
					.println("[Info] GUserExtend::Map::setup() ipfilter-size="
							+ ipfilter.size());
			if(ipfilter.size() == 0) 
				System.exit(-1);
		}

		public void map(LongWritable key, Text val, Context context) {
			String[] s = val.toString().split(SEPA_MR);

			okey.set(s[0]);
//			oval.set(s[1]);
			// 抽取 ip or zone 列表
			s[1] = s[1].replace('[', ' ');
			s[1] = s[1].replace(']', ' ');
			String[] atom = s[1].split("\u0001|, ");
			for(int i = 0; i < atom.length; ++i) 
				atom[i] = atom[i].trim();

			try {
				int i = 0;
				String soval = SEMP, stype = SEMP;
//				oval.set(SEMP);
				if (ipfilter.containsKey(atom[0])) {
//					oval.set(ipfilter.get(atom[0]));
					soval = ipfilter.get(atom[0]);
					stype = atom[0];
				} else {
					for (i = atom.length - 1; i != 0; --i) {  // small zone first.
						if(ipfilter.containsKey(atom[i])) {  
//							oval.set(ipfilter.get(atom[i]));
							soval = ipfilter.get(atom[i]);
							stype = atom[i]; 
							break;
						}
					}
				}
				if(soval.equals(SEMP)) {
					oval.set(s[1]);
					mos.write(Unhit, okey, oval, PUnhit);
					context.getCounter(Counters.MoUnhit).increment(1);
				} else {
					int isize = UtilStr.SubStrCnt(soval, "=");
					if(isize > 100) {  // 裁剪
						int iatom;
						for(iatom = -1; iatom < isize; iatom = soval.indexOf('=', iatom+1));
						iatom = soval.indexOf(',', iatom);
						soval = soval.substring(0, iatom) + ']';
					}
					oval.set(soval + SEPA_MR + String.format("[source=%s]", stype));
					mos.write(Hit, okey, oval, PHit);
					context.getCounter(Counters.MoHit).increment(1);
				}
			} catch (Exception e) {
				context.getCounter(Counters.ErrMo).increment(1);
			}
		}

		public void cleanup(Context context) {
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length < 3) {
			System.out.println("Usage: <out> <in-uid2ip> filterpath ");
			System.exit(3);
		}

		conf.set(ConfFilter, args[2]);

		Job job = new Job(conf, "[filter userid by ip&zone rankinfo]");
		job.setJarByClass(GUserExtend.class);
		job.setMapperClass(MapCProduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(0);

		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[0]));
		Path[] input_tot = FileInputFormat.getInputPaths(job);
		for (Path p : input_tot) {
			FileSystem fsystem = p.getFileSystem(job.getConfiguration());
			// FileStatus pstat = fsystem.getFileStatus(p); // For test.
			if (fsystem.isFile(p)) {
				System.out.println("[Info] inPath:" + p.toString());
			} else {
				FileStatus[] input_fs = fsystem.globStatus(p);
				for (FileStatus ps : input_fs) {
					System.out.println("[Info] inPath:" + ps.getPath());
				}
			}
		}

		MultipleOutputs.addNamedOutput(job, Hit, TextOutputFormat.class,
				Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, Unhit, TextOutputFormat.class,
				Text.class, Text.class);

		Date startTime = new Date();
		job.waitForCompletion(true);
		Date end_time = new Date();
		System.out.println("The job took "
				+ (end_time.getTime() - startTime.getTime()) / 1000
				+ " seconds.");
		return 0;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		int res;
		try {
			res = ToolRunner.run(new Configuration(), new GUserExtend(), args);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

}
