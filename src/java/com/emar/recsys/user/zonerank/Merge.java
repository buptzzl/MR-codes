package com.emar.recsys.user.zonerank;

import java.io.IOException;
import java.text.ParseException;
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

/**
 * 将两个文件中 相同的key（比如： uid）， 按自定义方法合并交集部分， 差集分开保存。
 * 比如： 可用于增量合并前后两次 uid-rank 数据。
 * @author zhoulm
 * 
 */
public class Merge extends Configured implements Tool {
	// 定义Map & Reduce 中通用的对象
	private static final String SEPA = LogParse.SEPA,
			SEPA_MR = LogParse.SEPA_MR, PLAT = LogParse.PLAT,
			EMAR = LogParse.EMAR, MAGIC = LogParse.MAGIC;

	// Reduce &MO setting info.
	public static final String first = "first", firstDir = "first/",
			second = "second", secondDir = "second/", both = "both",
			bothDir = "both/";
	public static String CIsFirst = "fpath";

	public static class MapCombine extends
			Mapper<LongWritable, Text, Text, PriorPair> {
		private Text okey = new Text();
		private PriorPair oval = new PriorPair();

		private String PType;

		private static enum Counters {
			Mo1, Mo2, ErrInit, ErrMo1, ErrMo2
		};

		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			PType = conf.get(CIsFirst);
		}

		public void map(LongWritable key, Text val, Context context) {
			String[] rankinfo = val.toString().split(SEPA_MR);
			oval.setFirst(rankinfo[1]); // campid or userid.
			if(rankinfo[0].indexOf("chuchuang") != -1) {
				rankinfo[0].replace("chuchuang", "yiqifa");
			}
			okey.set(rankinfo[0]); // rank-info.
			String path = ((FileSplit) context.getInputSplit()).getPath()
					.toString();
			if (path.indexOf(PType) >= 0) {
				oval.setFlag(false); // 标示第一部分
				try {
					context.write(okey, oval);
					context.getCounter(Counters.Mo1).increment(1);
				} catch (Exception e) {
					e.printStackTrace();
					context.getCounter(Counters.ErrMo1).increment(1);
				}
			} else {
				oval.setFlag(true);
				try {
					context.write(okey, oval);
					context.getCounter(Counters.Mo2).increment(1);
				} catch (Exception e) {
					e.printStackTrace();
					context.getCounter(Counters.ErrMo2).increment(1);
				}
			}
		}

		public void cleanup(Context context) {
		}

	}

	public static class ReduceCombine extends
			Reducer<Text, PriorPair, Text, Text> {

		private static enum Counters {
			ErrRo, ErrEmp, ErrMoIP, ErrRoIP, ErrRoZone, ErrMROIP, ErrRoBoth, MoRank, MoRankMul, MoIP, 
			RoBoth, RoIP, RoZone, RoUnrank
		};

		private MultipleOutputs<Text, Text> mos = null;
		private Text okey = new Text(), oval = new Text();
		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			mos = new MultipleOutputs(context);
			super.setup(context);
		}

		@Override
		public void reduce(Text key, Iterable<PriorPair> values,
				Context context) {
			String tmp;
			Set<String> set1 = new HashSet<String>();
			Set<String> set2 = new HashSet<String>();

			for (PriorPair t : values) {
				tmp = t.getFirst().toString();
				if (t.getFlag().get() == true) {
					set2.add(tmp);
				} else {
					set1.add(tmp);
				}

			}
			if (set2.size() == 0) {
				for (String s : set1) {
					oval.set(s);
					try {
						mos.write(first, key, oval, firstDir); // only key1
						context.getCounter(Counters.RoIP).increment(1);
					} catch (Exception e) {
						e.printStackTrace();
						context.getCounter(Counters.ErrRoIP).increment(1);
					}
				}
			} else if (set1.size() == 0) {
				for (String s : set2) {
					oval.set(s);
					try {
						mos.write(second, key, oval, secondDir); // only key2
						context.getCounter(Counters.RoZone).increment(1);
					} catch (Exception e) {
						e.printStackTrace();
						context.getCounter(Counters.ErrRoZone).increment(1);
					}
				}
			} else {
				// / 处理两者中的交集部分
				Set<String> resMerge = new HashSet<String>();
				UtilMR.mergeIPRank(set1, set2, resMerge); // set1必须为IP
//				oval.set("");
				for (String s : resMerge) {
					oval.set(s);
					try {
						mos.write(both, key, oval, bothDir);
						context.getCounter(Counters.RoBoth).increment(1);
					} catch (Exception e) {
						e.printStackTrace();
						context.getCounter(Counters.ErrRoBoth).increment(1);
					}
				}
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
		String[] oargs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (oargs.length != 4) {
			System.out.println("Usage: <out> <in1> <in2> <first-path-str>.");
			System.exit(4);
		}
		conf.set(CIsFirst, oargs[3]);

		Job job = new Job(conf, "[merge two part together]");
		job.setJarByClass(Merge.class);
		job.setMapperClass(MapCombine.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(PriorPair.class);
		job.setReducerClass(ReduceCombine.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(32);
		
		FileOutputFormat.setOutputPath(job, new Path(args[0]));
		FileInputFormat.addInputPaths(job, args[1]);
		FileInputFormat.addInputPaths(job, args[2]);
//		boolean setin = HdfsIO.setInput(oargs[2], oargs[3], oargs[4], job);
//		if (!setin) {
//			System.exit(1);
//		}
		// FileInputFormat.addInputPath(job, new Path(args[2]));

		MultipleOutputs.addNamedOutput(job, first, TextOutputFormat.class,
				Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, second, TextOutputFormat.class,
				Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, both, TextOutputFormat.class,
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
			res = ToolRunner.run(new Configuration(), new Merge(), args);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

}
