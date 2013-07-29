package com.emar.recsys.user.zonerank;

/**
 * 合并两种以 uid 为key 的 map-value 列表（gUser, gCamp）  
 * @author zhoulm
 *
 * @discasde
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
import org.apache.hadoop.util.ToolRunner;

import com.emar.recsys.user.util.UtilStr;

public class MIp2Rank extends Configured implements Tool {
	// 定义Map & Reduce 中通用的对象
	private static final String MR_sepa = "\u0001";
	public static final String MR_kcfmt = "camp_", MR_kufmt = "user_";
	private static final SimpleDateFormat datefmt = new SimpleDateFormat(
			"yyyyMMddHHmmss");

	public static class Map extends Mapper<LongWritable, Text, Text, MapWritable> {
		// TODO: 设置Ip-Rankinfo  zone-Rankinfo对应的路径中的字段
		private static final String ByUid = "gUser", ByCampid = "gCamp", 
				isUid = "uid", isIP = "ip";
		private float wuser, wcamp, weight;

		private static Calendar c = Calendar.getInstance();
		private Text okey = new Text();
//		private Text oval = new Text();
		private MapWritable oval = new MapWritable();
		
		private String PType;
		private String[] atom;

		private static enum Counters {
			Useless, ByUserFail, ByCampidFail, UnMout, ErrKV
		};

		public void setup(Context context) {
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
				// TODO 按uid合并的数据
				weight = wuser;
				oval.put(new Text(MR_kufmt), new FloatWritable());
//				oval.set(String.format(MR_kufmt + "%s", atom[1]));
			} else {
				weight = wcamp;
				oval.put(new Text(MR_kcfmt), new FloatWritable());
//				oval.set(String.format(MR_kcfmt + "%s", atom[1]));
			}
			
			String[] atoms = UtilStr.str2arr(atom[1]);
			String[] kvpair;
			Float tmpv;
			for (String s: atoms) {
				kvpair = s.split("=");
				if (kvpair.length != 2) {
					context.getCounter(Counters.ErrKV).increment(1);
				} else {
					tmpv = Float.parseFloat(kvpair[1]);  // 按权重合并
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
		public static enum Counters {
			UnRout, ErrRo, ErrKV, FmtMulti, MoMulti, MoZero, MoOne
		};

		private MultipleOutputs<Text, Text> mos = null;
		private Text okey = new Text(), oval = new Text();

		public void setup(Context context) {
		}
		
		public void reduce(Text key, Iterable<MapWritable> values, Context context) {
			HashMap<String, Float> tmap, mval = new HashMap<String, Float>(100, 0.95f);
			HashMap<String, Float> vcamp = new HashMap<String, Float>(100, 0.95f),
					vuser = new HashMap<String, Float>();
			float fval, vval;
			String skey;
			Text vkey;
			boolean isuser = false;
			
			for(MapWritable v: values) {
				Iterator<Writable> vitr = v.keySet().iterator();
				isuser = false;
				while(vitr.hasNext()) {
					vkey = (Text) vitr.next();
					skey = vkey.toString();
					if(skey.compareTo(MIp2Rank.MR_kufmt) == 0) {
						if(isuser) {
							context.getCounter(Counters.FmtMulti).increment(1);
						}
						isuser = true;
						continue;
					}
					fval = ((FloatWritable) v.get(vkey)).get();
					if(!mval.containsKey(skey)) {
						vval = new Float(0);
					} else {
						vval = mval.get(skey);
					}
					mval.put(skey, vval + fval);
				}
				// 将结果存储到对应的 map
				if(isuser) {
					for(String uk: mval.keySet()) {
						vuser.put(uk, vuser.containsKey(uk)? (vuser.get(uk) + mval.get(uk)): mval.get(uk));
					}
				} else if(vuser.size() == 0) {  // 仅仅在user为空时才存储camp得到的数据
					for(String uk: mval.keySet()) {
						vcamp.put(uk, vcamp.containsKey(uk)? (vcamp.get(uk) + mval.get(uk)): mval.get(uk));
					}
				}
				
				mval.clear();
			}
			
			skey = key.toString();
			try {
				if(vuser.size() != 0) {
					oval.set(vuser.entrySet().toString() + "\t[source=plat_user_id]");
					context.write(key, oval);
				} else if(vcamp.size() != 0) {
					oval.set(vcamp.entrySet().toString() + "\t[source=camp_id]");
					context.write(key, oval);
				} else {
					context.getCounter(Counters.UnRout).increment(1);
				}
				
			} catch (Exception e) {
				context.getCounter(Counters.ErrRo).increment(1);
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
			System.out.println("Usage: <in1> <in2> <out>");
			System.exit(3);
		}
		Job job = new Job(conf, "[merge two classify rank togather]");
		job.setJarByClass(MIp2Rank.class);
		job.setMapperClass(Map.class);
		// job.setCombinerClass(.class);
		job.setReducerClass(Reduce.class);
		job.setNumReduceTasks(32);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

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
		int res;
		try {
			res = ToolRunner.run(new Configuration(), new MIp2Rank(), args);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

}
