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

/**
 * 基于 无具体消费行为的uid:[ip,zone], ip:rank(优先)或zone:rank(次优) 生成完整的uid: mrank信息；
 * 没有链接上的uid继续输出。
 * 
 * @after GLogIP
 * @author zhoulm
 * 
 */
public class PClickUser extends Configured implements Tool {
	// 定义Map & Reduce 中通用的对象
	private static final String SEPA = LogParse.SEPA,
			SEPA_MR = LogParse.SEPA_MR;
	private static final String ConfFilter = "filterpath";
	public static final String PUid2IP = "", PIP = "", PZone = "",
			PUid2Rank = "", Hit = "hit", PHit = "uid_hit", Unhit = "unhit",
			PUnhit = "uid_unhit"; // 若干文件路径中的关键字

	public static class MapCProduce extends
			Mapper<LongWritable, Text, Text, Text> {

		private HashMap<String, String> ipfilter;
		private Text okey = new Text(), oval = new Text();
		private MultipleOutputs<Text, Text> mos;

		private static enum Counters {
			ErrMo
		};

		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			mos = new MultipleOutputs(context);

			// 将 ip:rank 或 zone:rank 加载到ipfilter。
			FSDataInputStream in;
			String line;
			String[] atom;

			Job job = new Job();
			FileInputFormat.addInputPath(job, new Path(conf.get(ConfFilter)));
			Path[] input_tot = FileInputFormat.getInputPaths(job);
			for (Path p : input_tot) {
				FileSystem fsystem = p.getFileSystem(conf);
				// FileStatus pstat = fsystem.getFileStatus(p); // For test.
				if (fsystem.isFile(p)) {
					in = fsystem.open(p);
					while ((line = in.readLine()) != null) {
						atom = line.trim().split(SEPA_MR);
						ipfilter.put(atom[0], atom[1]);
					}
					in.close();
				}
			}

		}

		public void map(LongWritable key, Text val, Context context) {
			String[] s = val.toString().split(SEPA_MR);

			okey.set(s[0]);
			oval.set(s[1]);
			try {
				if (ipfilter.containsKey(s[0])) {
					mos.write(Hit, okey, oval, PHit);
				} else {
					mos.write(Unhit, okey, oval, PUnhit);
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
		List<String> arr = Arrays.asList(args);
		System.out.println("[Info] input-args: " + arr);
		if (otherArgs.length < 3) {
			System.out.println("Usage: <in> <out> filterpath ");
			System.exit(3);
		}

		conf.set(ConfFilter, args[2]);

		Job job = new Job(conf, "[filter ip&zone rankinfo]");
		job.setJarByClass(GLogIP.class);
		job.setMapperClass(MapCProduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		// FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
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
			res = ToolRunner
					.run(new Configuration(), new GClickOrderIP(), args);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

}
