package com.emar.recsys.user.count;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.emar.recsys.user.zonerank.MIp2Rank;
import com.emar.recsys.user.zonerank.MIp2Rank.Map;
import com.emar.recsys.user.zonerank.MIp2Rank.Reduce;

/**
 * useage: input primary-key[sub-key-list], output 
 * 统计给定路径下的 给定列主键出现的频率，支持指定多个组合key成员
 * @author zhoulm
 * TODO
 */
public class Frequence  extends Configured implements Tool {
//定义Map & Reduce 中通用的对象
	private static final String SEPA = "\u0001";
	private static final String NLEN = "len", KIDX = "kidx", IDX = "idx";
	
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		public static enum Cnts {
			MO, MOErr, MIErr, MEmp
		}
		private int[] idx;
		private int kidx, len;
		
//		private StringBuffer sbu = new StringBuffer();
		private HashMap<String, Integer> stripe = new HashMap<String, Integer>(1<<10, 0.9f);
		private Text okey = new Text();
		private IntWritable oval = new IntWritable();
		
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			kidx = conf.getInt(KIDX, -1);
			len = conf.getInt(NLEN, -1);
			
			String[] idxs = conf.getStrings(IDX);
			idx = null;
			if(idxs != null && idxs.length != 0) {
				idx = new int[idxs.length];
				for(int i = 0; i < idxs.length; ++i) {
					idx[i] = Integer.parseInt(idxs[i].trim());
				}
			}
		}
		
		public void map(LongWritable key, Text val, Context context) {
			String s = val.toString();
			String nk;
			s = s.replace(SEPA, SEPA + " ");
			String[] atoms = s.split(SEPA);
			if(atoms.length < len) {
				context.getCounter(Cnts.MIErr).increment(1);
			} else {
				atoms[kidx] = atoms[kidx].trim();
				// last step do map-out. optimization.
				if(idx != null) {
					for(int i = 0; i < idx.length; ++i) {
						nk = String.format("%s\u0001%d\u0001%s", atoms[kidx], i, atoms[i].trim());
						stripe.put(nk, stripe.containsKey(nk)? stripe.get(nk) + 1: 1);
					}
				} else {
					stripe.put(atoms[kidx], 
							stripe.containsKey(atoms[kidx])? stripe.get(atoms[kidx])+1:1);
				}
			}
		}
		
		public void cleanup(Context context) {
			if(stripe.size() == 0) {
				context.getCounter(Cnts.MEmp).increment(1);
			}
			
			for(String s: stripe.keySet()) {
				okey.set(s);
				oval.set(stripe.get(s));
				try {
					context.write(okey, oval);
				} catch (Exception e) {
					context.getCounter(Cnts.MOErr).increment(1);
				}
			}
		}
		
	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		public static enum Cnts {
			RO, ROErr
		}
		
		public void setup(Context context) {
		}
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) {
			int cnt = 0;
			
			for(IntWritable iwbl: values) {
				cnt += iwbl.get();
			}
			
			try {
				context.write(key, new IntWritable(cnt));
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
		if (otherArgs.length < 4) {
			System.out.println("Usage: <in1> <out> <AtomLen> <adid> [idx-list sepa-by ,]");
			System.exit(4);
		}
		conf.setInt(KIDX, Integer.parseInt(otherArgs[3].trim()));
		conf.setInt(NLEN, Integer.parseInt(otherArgs[2].trim()));
		if(otherArgs.length == 5) {
			conf.setStrings(IDX, otherArgs[3].trim());
		}
		
		Job job = new Job(conf, "[count frequence]");
		job.setJarByClass(Frequence.class);
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
	
	public void main(String[] args) {
		int res;
		try {
			res = ToolRunner.run(new Configuration(), new Frequence(), args);
			System.exit(res);
		} catch(Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

}
