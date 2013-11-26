package com.emar.recsys.user.util.itemclassify;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.emar.recsys.user.log.LogParse;
import com.emar.recsys.user.util.UtilObj;
import com.emar.recsys.user.util.mr.HdfsIO;

/**
 * 将若干历史 用户的数据 合并到一起。
 * 
 * @author zhoulm
 * 
 */
public class KVCombiner extends Configured implements Tool {
	private static Logger log = Logger.getLogger(KVCombiner.class);
	// 定义Map & Reduce 中通用的对象
	private static final String SEPA = LogParse.SEPA,
			SEPA_MR = LogParse.SEPA_MR, PLAT = LogParse.PLAT,
			EMAR = LogParse.EMAR, MAGIC = LogParse.MAGIC;

	// Reduce &MO setting info.
	public static String CIsFirst = "fpath", S_Wrap = "sWrap", S_KV = "sKV",
			S_KVPair = "sPair";

	public static class MapCombine extends
			Mapper<LongWritable, Text, Text, Text> {
		private Text okey = new Text();
		private Text oval = new Text();

		private String PType;

		private static enum Counters {
			Mo, ErrMo
		};

		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			PType = conf.get(CIsFirst);
			log.info("map::setup PType=" + PType);
		}

		public void map(LongWritable key, Text val, Context context) {
			String[] rankinfo = val.toString().split(SEPA_MR);
			okey.set(rankinfo[0]);
			oval.set(rankinfo[1]);

			try {
				context.write(okey, oval);
				context.getCounter(Counters.Mo).increment(1);
			} catch (Exception e) {
				log.error("map write out failed." + e.getMessage());
				context.getCounter(Counters.ErrMo).increment(1);
			}
		}

		public void cleanup(Context context) {
		}

	}

	public static class ReduceCombine extends Reducer<Text, Text, Text, Text> {
		String s_wrap, s_kv, s_kvPair;

		private static enum Counters {
			Ro, ErrRo
		};

		private Text okey = new Text(), oval = new Text();

		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			s_wrap = conf.get(S_Wrap, "[]{}");
			s_kv = conf.get(S_KV, "=");
			s_kvPair = conf.get(S_KVPair, ",");
			log.info("reduce::setup MAP structure parse separator of wrap="
					+ s_wrap + ", s_kv" + s_kv + ", s_kvPair=" + s_kvPair);
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) {
			String tmp, m_key;

			HashMap<String, Float> kvTmp = null, idWeight = new HashMap<String, Float>(
					16, 0.9f);
			for (Text t : values) { // 合并MAP 格式的结果
				try {
					kvTmp = (HashMap<String, Float>) UtilObj.Str2Map(
							t.toString(), s_wrap, s_kvPair, s_kv);
					if (kvTmp == null)
						throw new NullPointerException("null data.");
				} catch (Exception e) {
					log.error("convert str to map failed. MSG="
							+ e.getMessage() + "\t input=" + t);
					continue;
				}
				for (Map.Entry<String, Float> ei : kvTmp.entrySet()) {
					m_key = ei.getKey();
					idWeight.put(m_key,
							idWeight.containsKey(m_key) ? idWeight.get(m_key)
									+ ei.getValue() : ei.getValue());
				}
			}
			if (idWeight.size() == 0) {
				return;
			}
			List<Entry<String, Float>> lrank = UtilObj.entrySortFloat(idWeight,
					true);
			try {
				context.write(key, new Text(lrank.toString()));
			} catch (Exception e) {
				context.getCounter(Counters.ErrRo).increment(1);
				log.error("reduce::write failed." + e.getMessage());
			}
		}

		public void cleanup(Context context) {
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] oargs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (oargs.length < 5) {
			System.out
					.println("Usage: <out> <time-range> <time-FMT> <path-FMT>"
							+ " <num-reduce> [<s_wrap> <s_kvpair> <s_kv>]");
			System.exit(5);
		}
		if (oargs.length == 8) {
			conf.set(S_Wrap, oargs[5]);
			conf.set(S_KVPair, oargs[6]);
			conf.set(S_KV, oargs[7]);
		}
		Job job = new Job(conf, "[merge multi-user data together]");
		job.setJarByClass(KVCombiner.class);
		job.setMapperClass(MapCombine.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(ReduceCombine.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(Integer.parseInt(oargs[4]));

		HdfsIO.setInput(oargs[1], oargs[2], oargs[3], job);
		FileInputFormat.setMinInputSplitSize(job, 134217728 * 2);// 128MB
		HdfsIO.removeDir(oargs[0], job);
		FileOutputFormat.setOutputPath(job, new Path(oargs[0]));

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
			KVCombiner kvc = new KVCombiner();
			res = ToolRunner.run(new Configuration(), kvc, args);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

}
