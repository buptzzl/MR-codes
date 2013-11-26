package com.emar.recsys.user.log;

import java.io.IOException;
import java.lang.reflect.Field;
import java.text.ParseException;
import java.util.Arrays;
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
import org.json.JSONArray;

import com.emar.recsys.user.util.mr.HdfsIO;
import com.emar.recsys.user.util.mr.PairFloatInt;

/**
 * 
 * 统计给定路径下的 给定主键出现的频率。
 *   1 支持指定副键列表 与主键组成 key， 
 *   2 支持 比率计算， 指定分母数据源即可 
 *     2.1 支持统计 两个数据源之间 指定KEY 的交集数，建议单个key求交集。 
 *     2.2 分子 分母 可统计各自的key 与组合key, 不建议使用 
 * 注： 
 *   1 采用 stripe 模式缓存频数; 
 *   2 统计的 key 直接使用对应的字段名;
 * 
 * @author zhoulm
 *         直接读入两份日志，stripe变为两个， value 采用pair的方式区别开，在R中做归并 Ratio计算。
 * 
 */
public class FrequenceRatio extends Configured implements Tool {
	// 定义Map & Reduce 中通用的对象
	private static final String SEPA = LogParse.SEPA,
			SEPA_MR = LogParse.SEPA_MR;
	private static final String IdxPKey = "KIdx", IdxOtherKey = "idx",
			IdxDenom = "KIdxDenum", IdxOtherDenum = "idxDenum",
			CDenom = "Denom";
	public static boolean debug = true;

	public static class MapFreq extends
			Mapper<LongWritable, Text, Text, PairFloatInt> {

		private String[] auxKeys, otherNum, otherDenum;
		private String kidx, knum, kdenum;
		private String denomPath;

		// private StringBuffer sbu = new StringBuffer();
		private HashMap<String, Integer> stripeaction,
				stripe = new HashMap<String, Integer>(1 << 11, 0.9f),
				stripeDenom = new HashMap<String, Integer>(1 << 11, 0.9f);
		private LogParse lparse;
		private Text okey;
		// private IntWritable oval = new IntWritable();
		private PairFloatInt oval;

		private static enum Cnts {
			ErrMo, ErrParse, ErrRefKeyNull, ErrReflect, ErrRefPrim
		}

		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			denomPath = conf.get(CDenom);
			// kidx = conf.get(IdxPKey, "camp_id"); // 主 KEY
			knum = conf.get(IdxPKey, "plat_user_id");
			kdenum = conf.get(IdxDenom, knum);
			// 可选的组合KEY
			String[] idxs = conf.getStrings(IdxOtherKey, "");
//			idx = null;
			if (idxs != null && idxs.length != 0) {
				// idx = new String[idxs.length];
				otherNum = new String[idxs.length];
				for (int i = 0; i < idxs.length; ++i) {
					// idx[i] = idxs[i].trim();
					otherNum[i] = idxs[i].trim();
				}
			} else 
				otherNum = new String[0];
			idxs = conf.getStrings(IdxOtherDenum, "");
			if (idxs != null && idxs.length != 0) {
				otherDenum = new String[idxs.length];
				for (int i = 0; i < idxs.length; ++i) {
					otherDenum[i] = idxs[i].trim();
				}
			} else 
				otherDenum = new String[0];

			try {
				lparse = new LogParse();
			} catch (ParseException e) {
				System.out
						.println("[ERROR] FrequenceRatio::MAP::setup init failed.");
			}
			okey = new Text();
			oval = new PairFloatInt();
			
//			if (debug) {
				System.out.println(String.format("[Info] setup()\n denomPath=%s,knum=%s, "
						+ "kdenum=%s, otherK=%s, otherDenum=%s", denomPath, knum, 
						kdenum, Arrays.asList(otherNum), Arrays.asList(otherDenum)));
//			}
		}

		public void map(LongWritable key, Text val, Context context) {
			String line = val.toString();
			String nk;
			String path = ((FileSplit) context.getInputSplit()).getPath()
					.toString();
			if (path.indexOf(denomPath) != -1) {
				stripeaction = stripeDenom;
				kidx = kdenum;
				auxKeys = otherDenum;
			} else {
				stripeaction = stripe;
				kidx = knum;
				auxKeys = otherNum;
			}
			try {
				this.lparse.reset();
				// this.logparse.base.isdug = true;
				this.lparse.parse(line, path);
			} catch (ParseException e1) {
				e1.printStackTrace();
			}
			
			if (!this.lparse.base.status) {
				context.getCounter(Cnts.ErrParse).increment(1);
				System.out.println("[ERR] FrequenceRatio::map()" + this.lparse);
				return; // 中断
			}
			
			String fval = null, pval = null;
			try {
				pval = (String) this.lparse.getField(kidx);
				if (pval == null || pval.length() == 0) {
					System.out.println(String.format("[ERR] map()::primary-key\n"
							+ "file_path=%s,prim-key=%s,ref-val=%s,key-arr=%s,\nparse=%s",
							path,kidx,pval,Arrays.asList(auxKeys),this.lparse));
					context.getCounter(Cnts.ErrRefPrim).increment(1);
					return; // 中断
				}
				stripeaction.put(pval, stripeaction.containsKey(pval) ? 
						(stripeaction.get(pval) + 1) : 1);
				String ks;
				for (int i = 0; i < auxKeys.length; ++i) {
					ks = auxKeys[i];
					fval = (String) this.lparse.getField(ks);
					if (fval == null || fval.length() == 0) {
						context.getCounter(Cnts.ErrRefKeyNull).increment(1);
						continue;
					}
					fval = pval + SEPA_MR + fval;
					stripeaction.put(fval,
							stripeaction.containsKey(fval) ? (stripeaction
									.get(fval) + 1) : 1);
				}
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("[ERR] FrequenceRatio::map()" + this.lparse);
				context.getCounter(Cnts.ErrReflect).increment(1);
			}
			this.writeout(context, 1 << 10);
			
			if (debug) {
				System.out.println(String.format("[Info] mapper()\nsrc_path=%s,"
						+ " kidx=%s, prim-key=%s,key-arr=%s,cmb-arr=%s,last-cmb-key=%s",
						path,kidx,pval,Arrays.asList(auxKeys),
						pval, fval));
			}
		}

		public void cleanup(Context context) {
			this.writeout(context, stripe.size());
		}

		private void writeout(Context context, int size) {
			if (stripe.size() < size && stripeDenom.size() < size) {
				return;
			}
			
			if (debug) {
				System.out.println(String.format("[Info] writeout()\nstripe-sz=%d," +
						" denum-sz=%d", stripe.size(), stripeDenom.size()));
			}
			
			for (Entry<String, Integer> ei : stripe.entrySet()) {
				okey.set(ei.getKey());
				oval.getFirst().set(ei.getValue());
				oval.getSecond().set(0);// 分子部分
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
				oval.getSecond().set(1);// 分母部分
				try {
					context.write(okey, oval);
				} catch (Exception e) {
					context.getCounter(Cnts.ErrMo).increment(1);
				}
			}
			stripeDenom.clear();
		}

	}

	public static class ReduceFreq extends
			Reducer<Text, PairFloatInt, Text, Text> {
		public static enum Cnts {
			RO, ROErr,
			// 统计 两个数据源 的KEY的交集数，各种非0的键数 。
			ROIntersect, ROnum, RODenom
		}

		public void setup(Context context) {
		}

		public void reduce(Text key, Iterable<PairFloatInt> values,
				Context context) {
			float cnt = 0, cntDenom = 1;

			for (PairFloatInt iwbl : values) {
				if (iwbl.getSecond().get() == 0) {
					cnt += iwbl.getFirst().get();
				} else {
					cntDenom += iwbl.getFirst().get();
				}
			}

			if (cnt != 0)
				context.getCounter(Cnts.ROnum).increment(1);
			if (cntDenom != 1)
				context.getCounter(Cnts.RODenom).increment(1);
			if (cnt != 0 && cntDenom != 1)
				context.getCounter(Cnts.ROIntersect).increment(1);

			JSONArray jarr = new JSONArray();
			jarr.put(String.format("%6f", ((float) cnt) / cntDenom));
			jarr.put(cnt + "");
			jarr.put(cntDenom + "");
			String scores = jarr.toString(); 

			try {
				context.write(key, new Text(scores));
			} catch (Exception e) {
				context.getCounter(Cnts.ROErr).increment(1);
			}
			
			if (debug) {
				System.out.println(String.format("[Info] Reduce()\nRinKey=%s, " 
						+ "RoutKey=%s, RoutVal=%s", key,key,scores));
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
		final char paraSep = ',';
		if (otherArgs.length < 8) {
			System.out
					.println("Usage: <out> <data-range> <time-FMT-in-path1> <path1-fmt> "
							+ "<time-FMT-in-path2> <path2-fmt> <denom-path-str>"
							+ "<primary-key> <demon-prim-key> [key-name-list sepa-by ,] [demon-keys]");
			System.exit(8);
		}
		conf.set(CDenom, otherArgs[6]); // 指示分母源数据路径的串
		conf.set(IdxPKey, otherArgs[7]); // 分子的主键名
		conf.set(IdxDenom, otherArgs[8]);// 分母的主键名
		if (otherArgs.length >= 10) {
			conf.set(IdxOtherKey, otherArgs[9]); // 分子的附属键列表
			if (otherArgs.length > 10)
				conf.set(IdxOtherDenum, otherArgs[10]);

		}

		Job job = new Job(conf, "[count ratio]");
		job.setJarByClass(FrequenceRatio.class);
		job.setMapperClass(MapFreq.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(PairFloatInt.class);
		// job.setCombinerClass(.class);
		job.setReducerClass(ReduceFreq.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
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
		
		Date startTime = new Date();
		job.waitForCompletion(true);
		Date endTime = new Date();
		System.out.println("The job took "
				+ (endTime.getTime() - startTime.getTime()) / 1000
				+ " seconds.");

		return 0;
	}

	public static void main(String[] args) {
		int res;
		try {
//			FrequenceRatio.debug = true;  // 设置debug无效 
			FrequenceRatio myMR = new FrequenceRatio();
			res = ToolRunner.run(new Configuration(), myMR,
					args);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

}
