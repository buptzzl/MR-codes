package com.emar.recsys.user.count;

import java.io.IOException;
import java.lang.reflect.Field;
import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.emar.recsys.user.log.BaseLog;
import com.emar.recsys.user.log.LogParse;
import com.emar.recsys.user.util.MRPair;
import com.emar.util.HdfsIO;

/**
 * 
 * @author zhoulm
 * 
 *         改进Frequence：直接读入两份日志，stripe变为两个， value 采用pair的方式区别开，在R中做归并 Ratio计算。
 */
public class FrequenceRatio extends Configured implements Tool {
	// 定义Map & Reduce 中通用的对象
	private static final String SEPA = LogParse.SEPA,
			SEPA_MR = LogParse.SEPA_MR;
	private static final String KIDX = "kidx", IDX = "idx", CDenom = "Denom";

	public static class MapFreq
			extends
			Mapper<LongWritable, Text, Text, MRPair<VIntWritable, BooleanWritable>> {

		private String[] idx;
		private String kidx;
		private String denomPath;

		// private StringBuffer sbu = new StringBuffer();
		private HashMap<String, Integer> stripeaction,
				stripe = new HashMap<String, Integer>(1 << 11, 0.9f),
				stripeDenom = new HashMap<String, Integer>(1 << 11, 0.9f);
		private LogParse lparse;
		private Text okey;
		// private IntWritable oval = new IntWritable();
		private MRPair<VIntWritable, BooleanWritable> oval;

		private static enum Cnts {
			ErrMo, ErrParse, ErrRefKeyNull, ErrReflect
		}

		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			denomPath = conf.get(CDenom);
			kidx = conf.get(KIDX, "camp_id"); // 主 KEY
			// 可选的组合KEY
			String[] idxs = conf.getStrings(IDX, "");
			idx = null;
			if (idxs != null && idxs.length != 0) {
				idx = new String[idxs.length];
				for (int i = 0; i < idxs.length; ++i) {
					idx[i] = idxs[i].trim();
				}
			}

			try {
				lparse = new LogParse();
			} catch (ParseException e) {
				System.out
						.println("[ERROR] FrequenceRatio::MAP::setup init failed.");
			}
			okey = new Text();
			oval = new MRPair<VIntWritable, BooleanWritable>(
					new VIntWritable(0), new BooleanWritable());
		}

		public void map(LongWritable key, Text val, Context context) {
			String line = val.toString();
			String nk;
			String path = ((FileSplit) context.getInputSplit()).getPath()
					.toString();
			if (path.indexOf(denomPath) != -1) {
				stripeaction = stripeDenom;
			}
			try {
				// this.logparse.base.isdug = true;
				this.lparse.parse(line, path);
			} catch (ParseException e1) {
				System.out.println("[ERROR] IclassifyMap2::map() "
						+ this.lparse);
			}
			if (!this.lparse.base.status) {
				context.getCounter(Cnts.ErrParse).increment(1);
				return; // 中断
			}
			try {
//				Field field;
				String fval, pval;
//				field = BaseLog.class.getField(kidx);
//				pval = (String) field.get(this.lparse.base);
				pval = (String) this.lparse.getField(kidx);
				if (pval == null || pval.length() == 0) {
					context.getCounter(Cnts.ErrParse).increment(1);
					System.out.println("[ERROR] IclassifyMap2::map() "
							+ this.lparse);
					return; // 中断
				} else {
					stripe.put(pval,
							stripe.containsKey(pval) ? (stripe.get(pval) + 1)
									: 1);
				}
				for (String ks : idx) {
//					field = BaseLog.class.getField(ks);
//					fval = (String) field.get(this.lparse.base);
					fval = (String) this.lparse.getField(ks);
					if (fval == null || fval.length() == 0) {
						context.getCounter(Cnts.ErrRefKeyNull).increment(1);
						continue;
					}
					fval = pval + SEPA_MR + fval;
					stripe.put(fval,
							stripe.containsKey(fval) ? (stripe.get(fval) + 1)
									: 1);
				}
			} catch (Exception e) {
				e.printStackTrace();
				context.getCounter(Cnts.ErrReflect).increment(1);
			}
			this.writeout(context, 1 << 10);
		}

		public void cleanup(Context context) {
			this.writeout(context, stripe.size());
		}

		private void writeout(Context context, int size) {
			if (stripe.size() < size && stripeDenom.size() < size) {
				return;
			}
			for (Entry<String, Integer> ei : stripe.entrySet()) {
				okey.set(ei.getKey());
				oval.getFirst().set(ei.getValue());
				oval.getSecond().set(false);// 分子部分
				try {
					context.write(okey, oval);
				} catch (Exception e) {
					context.getCounter(Cnts.ErrMo).increment(1);
				}
			}
			stripe.clear();
			for (Entry<String, Integer> ei : stripeDenom.entrySet()) {
				okey.set(ei.getKey());
				oval.getFirst().set(ei.getValue());
				oval.getSecond().set(true);// 分母部分
				try {
					context.write(okey, oval);
				} catch (Exception e) {
					context.getCounter(Cnts.ErrMo).increment(1);
				}
			}
			stripeDenom.clear();
		}

	}

	public static class ReduceFreq
			extends
			Reducer<Text, MRPair<IntWritable, BooleanWritable>, Text, FloatWritable> {
		public static enum Cnts {
			RO, ROErr
		}

		public void setup(Context context) {
		}

		public void reduce(Text key,
				Iterable<MRPair<IntWritable, BooleanWritable>> values,
				Context context) {
			float cnt = 0, cntDenom = 0;

			for (MRPair<IntWritable, BooleanWritable> iwbl : values) {
				if (iwbl.getSecond().get() == true) {
					cnt += iwbl.getFirst().get();
				} else {
					cntDenom += iwbl.getFirst().get();
				}
			}

			try {
				context.write(key, new FloatWritable(cnt / cntDenom));
			} catch (Exception e) {
				context.getCounter(Cnts.ROErr).increment(1);
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
		if (otherArgs.length < 8) {
			System.out
					.println("Usage: <out> <data-range> <time-FMT-in-path1> <path1-fmt> "
							+ "<time-FMT-in-path2> <path2-fmt> "
							+ "<denom-path-str> <primary-key> [key-name-list sepa-by ,]");
			System.exit(8);
		}
		conf.set(CDenom, otherArgs[6]);
		conf.set(KIDX, otherArgs[7]);
		if (otherArgs.length == 9) {
			conf.setStrings(IDX, otherArgs[8].trim());
		}

		Job job = new Job(conf, "[count ratio]");
		job.setJarByClass(FrequenceRatio.class);
		job.setMapperClass(MapFreq.class);
		// job.setCombinerClass(.class);
		job.setReducerClass(ReduceFreq.class);
		job.setNumReduceTasks(8);

		// FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[0]));
		boolean setin = HdfsIO.setInput(otherArgs[1], otherArgs[2],
				otherArgs[3], job);
		setin &= HdfsIO.setInput(otherArgs[1], otherArgs[4], otherArgs[5], job);
		if (!setin) {
			System.out
					.println("[ERROR] FrequenceRatio::run() failed to add input-data.");
			System.exit(1);
		}

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MRPair.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);

		Date startTime = new Date();
		job.waitForCompletion(true);
		Date end_time = new Date();
		System.out.println("The job took "
				+ (end_time.getTime() - startTime.getTime()) / 1000
				+ " seconds.");

		return 0;
	}

	public static void main(String[] args) {
		int res;
		try {
			res = ToolRunner.run(new Configuration(), new FrequenceRatio(), args);
			System.exit(res);
//			System.out.println("[test] FrequenceRatio");
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

}
