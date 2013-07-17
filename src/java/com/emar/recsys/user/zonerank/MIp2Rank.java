package com.emar.recsys.user.zonerank;

/**
 * 按IP信息合并最终出按ip维度的排名
 * @author zhoulm
 *
 */

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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

import com.emar.recsys.user.util.UtilStr;
import com.emar.recsys.user.util.itemclassify.Iclassify;
import com.emar.recsys.user.util.itemclassify.IclassifyMapper;
import com.emar.recsys.user.util.itemclassify.IclassifyReducer;

public class MIp2Rank extends Configured implements Tool {
	// 定义Map & Reduce 中通用的对象
	private static final String MR_sepa = "\u0001";
	private static final String MR_kcfmt = "camp_", MR_kufmt = "user_";
	private static final SimpleDateFormat datefmt = new SimpleDateFormat(
			"yyyyMMddHHmmss");

	public static class Map extends Mapper<LongWritable, Text, Text, MapWritable> {
		// TODO: 设置Ip-Rankinfo  zone-Rankinfo对应的路径中的字段
		private static final String ByUid = "", ByCampid = "";
		private float wuser, wcamp, weight;

		private static Calendar c = Calendar.getInstance();
		private Text okey = new Text();
		private MapWritable oval = new MapWritable();
		private String PType;
		private String[] atom;

		private static enum Counters {
			Useless, ByUserFail, ByCampidFail, UnMout, ErrKV
		};

		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			wuser = conf.getFloat("WbyUserid", 1);
			wcamp = conf.getFloat("WbyCampid", 0);
			System.out
					.print("[Info] MIp2Rank::Reduce::setup() weight of rank linked by user="
							+ wuser
							+ "\tweight of rank linked by campid="
							+ wcamp);
		}
		
		// 按设置的权责合并 userRank 与 campRank.
		public void map(LongWritable key, Text val, Context context) {
			String inpath = ((FileSplit) context.getInputSplit()).getPath()
					.toString();
			atom = val.toString().split("\t"); // MR's KV output.
			if (atom.length != 2) {
				context.getCounter(Counters.Useless).increment(1);
				return;
			}
			if (inpath.indexOf(ByUid) >= 0) {
				weight = wuser;
//				oval.set(String.format(MR_kufmt + "%s", atom[1]));
			} else {
				weight = wcamp;
//				oval.set(String.format(MR_kcfmt + "%s", atom[1]));
			}
			String[] atoms = UtilStr.str2arr(atom[1]);
			String[] kvpair;
			Float tmpv;
			for (String s : atoms) {
				kvpair = s.split("=");
				if (kvpair.length != 2) {
					context.getCounter(Counters.ErrKV).increment(1);
				} else {
					tmpv = Float.parseFloat(kvpair[0])*weight;  // 按权重合并
					oval.put(new Text(kvpair[0]), new FloatWritable(tmpv));
				}
			}
			
			okey.set(atom[0]);
			try {
				context.write(okey, oval);
			} catch (Exception e) {
				context.getCounter(Counters.UnMout).increment(1);
			}
		}

		public void cleanup(Context context) {
		}
	}

	public static class Reduce extends Reducer<Text, MapWritable, Text, Text> {
		
		private String tmp;
		private String[] atom, kvpair;
		private static enum Counters {
			UnRout, ErrKV
		};

		private MultipleOutputs<Text, Text> mos = null;
		private Text okey = new Text(), oval = new Text();

		public void setup(Context context) {
		}

		public void reduce(Text key, Iterable<MapWritable> values, Context context) {
			HashMap<String, Float> mval = new HashMap<String, Float>();
//			FloatWritable fval, vval;
			float fval, vval;
			String skey;
			Text vkey;
			for(MapWritable v: values) {
				Iterator<Writable> vitr = v.keySet().iterator();
				while(vitr.hasNext()) {
					vkey = (Text) vitr.next();
					skey = vkey.toString();
					fval = ((FloatWritable) v.get(vkey)).get();
					if(!mval.containsKey(skey)) {
						vval = new Float(0);
					} else {
						vval = mval.get(skey);
					}
					mval.put(skey, vval + fval);
				}
			}
			
			try {
				oval.set(mval.entrySet().toString());
				context.write(key, oval);
			} catch (Exception e) {
				context.getCounter(Counters.UnRout).increment(1);
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
		if (otherArgs.length != 2) {
			System.out.println("Usage: <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "[user and camp itemclassify rank]");
		job.setJarByClass(MIp2Rank.class);
		job.setMapperClass(Map.class);
		// job.setCombinerClass(.class);
		job.setReducerClass(Reduce.class);
		job.setNumReduceTasks(16);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MapWritable.class);
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
		// TODO Auto-generated method stub

	}

}
