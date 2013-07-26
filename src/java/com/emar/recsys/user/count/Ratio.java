package com.emar.recsys.user.count;

import java.util.Date;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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

import com.emar.recsys.user.count.Frequence.MapFreq.Cnts;

/**
 * 统计 两个目录下的相同key之间的比率， 支持对value中存在 组合键进行与主键组合的比率统计
 * @author zhoulm
 * TODO 
 */
public class Ratio  extends Configured implements Tool {
//定义Map & Reduce 中通用的对象
	private static final String SEPA = "\u0001", SEPA_MR = "\t",
			PNumerator = null, PreNum = "_";
	
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private HashMap<String, Integer> tmap, 
				stripe = new HashMap<String, Integer>(1<<10, 0.9f), 
				stripeNumera = new HashMap<String, Integer>(1<<10, 0.9f);
		private String[] atoms;
		private Text okey = new Text(), oval = new Text();
		
		public static enum Cnts {
			MO, MOErr, MIErr, MEmp
		}
		
		public void setup(Context context) {
		}
		
		public void map(LongWritable key, Text val, Context context) {
			atoms = val.toString().split(SEPA_MR);
			if(atoms.length != 2) {
				context.getCounter(Cnts.MIErr).increment(1);
			} else {
				String inp = ((FileSplit)context.getInputSplit()).getPath().toString();
				if(inp.indexOf(PNumerator) != -1) {
					tmap = stripeNumera;
				} else {
					tmap = stripe;
				}
				
				Integer ival = Integer.parseInt(atoms[1]);
				tmap.put(atoms[0], tmap.containsKey(atoms[0])? tmap.get(atoms[0])+ival:ival);
			}
		}

		public void cleanup(Context context) {
			if(stripe.size() == 0) {
				context.getCounter(Cnts.MEmp).increment(1);
			}
			
			for(String s: stripe.keySet()) {
				okey.set(s);
				oval.set(stripe.get(s)+"");
				try {
					context.write(okey, oval);
				} catch (Exception e) {
					context.getCounter(Cnts.MOErr).increment(1);
				}
			}
			for(String s: stripeNumera.keySet()) {
				okey.set(s);
				oval.set(stripeNumera.get(s)+PreNum);  // 分子
				try {
					context.write(okey, oval);
				} catch (Exception e) {
					context.getCounter(Cnts.MOErr).increment(1);
				}
			}
		}
		
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, FloatWritable> {
		public static enum Cnts {
			RO, ROErr, MOPVEmp
		}
		
		public void setup(Context context) {
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context) {
			int cnt = 0, cntNume = 0;
			int valSize = 0;
			String s;
			
			for(Text twbl: values) {
				s = twbl.toString();
				if(s.indexOf(PreNum) != -1) {
					cntNume += Integer.parseInt(s.substring(0, s.length()-1));
				} else {
					cnt += Integer.parseInt(s);
				}
			}
			if(cnt == 0) {
				context.getCounter(Cnts.MOPVEmp).increment(1);
				return;
			}
			try {
				context.write(key, new FloatWritable(cntNume/cnt));
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
		if(otherArgs.length < 3) {
			System.out.println("Usage: <in1> <in2> <out>");
			System.exit(3);
		}
		
		Job job = new Job(conf, "[ratio]");
		job.setJarByClass(Ratio.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setNumReduceTasks(8);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
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
			res = ToolRunner.run(new Configuration(), new Ratio(), args);
			System.exit(res);
		} catch(Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}
	
}
