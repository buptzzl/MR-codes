package com.emar.recsys.user.demo.sex;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.JSONArray;
import org.json.JSONObject;

import com.emar.recsys.user.count.FrequenceRatio;
import com.emar.recsys.user.count.FrequenceRatio.MapFreq;
import com.emar.recsys.user.count.FrequenceRatio.ReduceFreq;
import com.emar.recsys.user.count.FrequenceRatio.ReduceFreq.Cnts;
import com.emar.recsys.user.demo.IKeywords;
import com.emar.recsys.user.log.LogParse;
import com.emar.recsys.user.util.mr.PairTInt;
import com.emar.util.HdfsIO;

/**
 * 归并用户的order, 记录性别
 * 
 * @author zhoulm M： 性别，并记录 
 * @R： part1/2(score==0?)
 * @fmt uid:prodCnt\x01[sub-arr-atom1, sub-arr-atom2...]\x01Sneg\x01Spos\x01\[s1@@@name1, s2@@@name2...(s*!=0)]
 * 
 */
public class GOrder extends Configured implements Tool {
	// 定义Map & Reduce 中通用的对象
	private static final String SEPA = LogParse.SEPA, SEPA_MAG = LogParse.MAGIC,
			SEPA_MR = LogParse.SEPA_MR;
	private static final String Sex = "sex", Unk = "unk";
	private static final int KIDX_NAME = 3;

	public static class MapFreq extends
			Mapper<LongWritable, Text, Text, PairTInt> {

		private String[] atom;
		private String kidx;

		private Text okey;
		private PairTInt oval;

		private static enum Cnts {
			ErrMIn, ErrMo
		}

		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			// denomPath = conf.get(CDenom);

			okey = new Text();
			oval = new PairTInt();
		}

		public void map(LongWritable key, Text val, Context context) {
			String sval = val.toString();
			atom = sval.split(SEPA_MR);
			if (atom.length < 2) {
				context.getCounter(Cnts.ErrMIn).increment(1);
				System.out.println("[ERROR] GOrder::map() line=\n" + val.toString());
				return;
			} 
			if(atom.length > 2) {  // 原始数据中 可能有多个\t字符
				StringBuffer sbuf = new StringBuffer();
				for(int i = 1; i < atom.length; ++i) {
					sbuf.append(atom[i] + " "); // replace \t with space.
				}
				atom[1] = sbuf.toString();
			}

			String[] mval = String.format("%s ", atom[1]).split(SEPA);
			int sex = SexWord.isman(mval[KIDX_NAME]);

			okey.set(atom[0]);
			JSONArray obj = new JSONArray();
			for(int i = 0; i < mval.length; ++i) 
				obj.put(mval[i]);
			// 添加空格，替换 \u0001为数组
			oval.setFirst(obj.toString());
//					Arrays.asList(String.format("%s ", atom[1]).split(SEPA)).toString());
			oval.setFlag(sex);
			try {
				context.write(okey, oval);
			} catch (Exception e) {
				e.printStackTrace();
				context.getCounter(Cnts.ErrMo).increment(1);
			}
		}

		public void cleanup(Context context) {
		}

	}

	public static class ReduceFreq extends Reducer<Text, PairTInt, Text, Text> {
		private MultipleOutputs mos;

		public static enum Cnts {
			Ro, RoErr
		}

		private Text okey, oval;

		public void setup(Context context) {
			mos = new MultipleOutputs(context);
			okey = new Text();
			oval = new Text();
		}

		public void reduce(Text key, Iterable<PairTInt> values, Context context) {
			int score_diff = 0, score_pos = 0, cnt_goods = 0;
			int tmp = 0;
//			List<String> scores = new ArrayList<String>();
//			List<String> rawdata = new ArrayList<String>();
			JSONArray scores = new JSONArray();
			JSONArray rawdata = new JSONArray();
			String stmp;
			
			for (PairTInt pi : values) {
				cnt_goods += 1;
				tmp = pi.getFlag().get();
				stmp = pi.getFirst().toString();
				
				if (tmp != 0) {
					score_diff += tmp;
					if(tmp > 0) 
						score_pos += tmp;
//					scores.add(String.format("%d%s%s", tmp, SEPA_MAG, stmp));
					scores.put(new JSONArray(new String[]{tmp+"", stmp}));
				}

//				rawdata.add(stmp);
				rawdata.put(new JSONArray(stmp));
			}
			JSONObject jobj = new JSONObject();
			jobj.put(IKeywords.NGood, cnt_goods).put(IKeywords.RawLog, rawdata)
				.put(IKeywords.SReduce, score_diff).put(IKeywords.SPos, score_pos)
				.put(IKeywords.IScore, scores);
//			oval.set(String.format("%d\u0001%s\u0001%d\u0001%d\u0001%s", cnt_goods,
//					rawdata.toString(), score_sum, score_pos, scores.toString()));
			oval.set(jobj.toString());
			try {
				if (score_diff != 0 || score_pos != 0)
					mos.write(Sex, key, oval, Sex + "/");
				else
					mos.write(Unk, key, oval, Unk + "/");
			} catch (Exception e) {
				context.getCounter(Cnts.RoErr).increment(1);
			}
		}

		public void cleanup(Context context) {
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] oArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (oArgs.length < 2) {
			System.out.println("Usage: <out> <in> [in2,...]");
			System.exit(2);
		}

		Job job = new Job(conf, "[gather order&sex data]");
		job.setJarByClass(GOrder.class);
		job.setMapperClass(MapFreq.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(PairTInt.class);
		// job.setCombinerClass(.class);
		job.setReducerClass(ReduceFreq.class);
		job.setNumReduceTasks(8);

		// FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[0]));
		for (int i = 1; i < oArgs.length; ++i) {
			FileInputFormat.addInputPath(job, new Path(oArgs[i]));
		}
		HdfsIO.printInput(job);

		MultipleOutputs.addNamedOutput(job, Sex, TextOutputFormat.class,
				Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, Unk, TextOutputFormat.class,
				Text.class, Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

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
			res = ToolRunner.run(new Configuration(), new GOrder(), args);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

}
