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
import com.emar.recsys.user.util.PriorPair;
import com.emar.recsys.user.util.UtilObj;
import com.emar.recsys.user.util.UtilStr;
import com.emar.util.Ip2AreaUDF;
import com.emar.util.exp.UrlPair;

/**
 * 聚会各个地域的IP， zone 对应的 不同Rank（相同时保留1个）.
 * 
 * @after GLogOrder
 * @author zhoulm
 * 
 */
public class GLogIP extends Configured implements Tool {
	// 定义Map & Reduce 中通用的对象
	private static final String SEPA = LogParse.SEPA, SEPA_MR = LogParse.SEPA_MR;

	public static final String moIp = "ip", IpDir = "ip/", moZone = "zone",
			ZoneDir = "zone/";

	public static class MapFilter extends Mapper<LongWritable, Text, Text, Text> {

		private Text okey = new Text(), oval = new Text();

		private static enum Counters {
			ErrMo
		};

		public void setup(Context context) {
		}

		public void map(LongWritable key, Text val, Context context) {
			String[] s = val.toString().split(SEPA_MR);
			okey.set(s[0]);
			oval.set(s[1]);
			try {
				context.write(okey, oval);
			} catch (Exception e) {
				context.getCounter(Counters.ErrMo).increment(1);
			}
		}

		public void cleanup(Context context) {
		}
	}

	public static class ReduceFilter extends Reducer<Text, Text, Text, Text> {

		private static enum Counters {
			ErrRo
		};

		private MultipleOutputs<Text, Text> mos = null;
		private Text okey = new Text(), oval = new Text();
		private int THRELD; // 当前 KEY 被作为排名出现的最小次数

		public void setup(Context context) throws IOException, InterruptedException {
			mos = new MultipleOutputs(context);
			super.setup(context);
			Configuration conf = context.getConfiguration();
			THRELD = conf.getInt("minfreq", 1);
		}

		public void reduce(Text key, Iterable<Text> values, Context context) {
			String tmp;
			// String[] arrTmp;
			Map<String, Integer> rankcnt = new HashMap<String, Integer>();
			int tot = 0;

			for (Text t : values) {
				tmp = t.toString();
				rankcnt.put(tmp, rankcnt.containsKey(tmp) ? (rankcnt.get(tmp) + 1) : 1);
				tot += 1;
			}
			if (tot < THRELD) {
				return;
			}

			Map<String, Float> mrank = new HashMap<String, Float>();
			for (String s : rankcnt.keySet()) {
				Map<String, Float> mtmp = UtilObj.Str2Map(s, "[]", ", "); // 数组转MAP
				for (Entry<String, Float> kv : mtmp.entrySet()) {
					mrank.put(
							kv.getKey(),
							mrank.containsKey(kv.getKey()) ? (mrank.get(kv.getKey()) + kv
									.getValue()) : kv.getValue());
				}
			}
			List<Entry<String, Float>> rlist = UtilObj.entrySortFloat(mrank, true);
			oval.set(rlist.toString());
			try {
				if (key.toString().indexOf('.') != -1) {
					mos.write(moIp, key, oval, IpDir);
				} else {
					mos.write(moZone, key, oval, ZoneDir);
				}
			} catch (Exception e) {
				context.getCounter(Counters.ErrRo).increment(1);
			}
		}

		public void cleanup(Context context) {
			if (mos == null) {
				return;
			}
			try {
				mos.close();
				super.cleanup(context);
			} catch (Exception e) {
				e.printStackTrace();
			}
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
			System.out.println("Usage: <in> <out> minFreq ");
			System.exit(3);
		}

		conf.setInt("minfreq", Integer.parseInt(otherArgs[2]));

		Job job = new Job(conf, "[filter ip&zone rankinfo]");
		job.setJarByClass(GLogIP.class);
		job.setMapperClass(MapFilter.class);
		job.setReducerClass(ReduceFilter.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(16);

		FileInputFormat.addInputPath(job, new Path(args[0]));
//		FileInputFormat.addInputPath(job, new Path(args[1]));
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

		MultipleOutputs.addNamedOutput(job, moIp, TextOutputFormat.class,
				Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, moZone, TextOutputFormat.class,
				Text.class, Text.class);

		Date startTime = new Date();
		job.waitForCompletion(true);
		Date end_time = new Date();
		System.out.println("The job took "
				+ (end_time.getTime() - startTime.getTime()) / 1000 + " seconds.");
		return 0;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		int res;
		try {
			res = ToolRunner.run(new Configuration(), new GClickOrderIP(), args);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

}
