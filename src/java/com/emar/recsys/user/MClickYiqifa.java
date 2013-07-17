package com.emar.recsys.user;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.emar.recsys.user.util.DateParse;
import com.emar.recsys.user.util.UAParse;
import com.emar.recsys.user.util.UtilObj;
import com.emar.recsys.user.util.UtilStr;
import com.emar.util.Ip2AreaUDF;

/**
 *  
 * @author zhoulm
 */
public class MClickYiqifa extends Configured implements Tool {
	
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
//	public static class Map extends MapReduceBase implements Mapper<
//			LongWritable, Text, Text, Text> {
		/*
		static enum Counters {HourHour1, Hour2, Hour3, Hour4, Hour5, Hour6, Hour7, Hour8,
				Hour9, Hour10, Hour11, Hour12, Hour13, Hour14, Hour15, Hour16,
				Hour17, Hour18, Hour19, Hour20, Hour21, Hour22, Hour23};
				*/
		private Ip2AreaUDF Ip2Dst = Ip2AreaUDF.getInstance();
		
		private static enum Counters {
			Useless, UnUseDayweek, UnuseIP, UnuseUA, UnuseAD, UnMapout
		};
		public static int IP_DEF = -1, WEEK_DEF = -1, HOUR_DEF = 24; 
		public static String[] DEF_UA = new String[]{"", "", "", ""};
		
		private boolean caseLower, case2;
		private String inputFile;
		private static final int LenLog = 10;
		private static final int IdxTime=4, IdxIp=2, IdxUA=9, IdxUserP=3;
		
		//private MultipleOutputs mos; 
		
		private static Calendar c = Calendar.getInstance();
		private static SimpleDateFormat dateformat = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
				
		public void setup(Context context){
			/*
			//mos = new MultipleOutputs(job);
			//default is True. USE: -Dtest.case.Lower=true
			caseLower =  job.getBoolean("test.case.Lower", true); 
			inputFile = job.get("map.input.file");
			if(job.getBoolean("test.case.effective", false)) {
				Path[] patterns = new Path[0];
				try {
					patterns = DistributedCache.getLocalCacheArchives(job);
				} catch (IOException ioe) {
					System.err.println("Error in getting cached files" + 
							StringUtils.stringifyException(ioe));
				}
				for(Path p : patterns) {
					// BufferedReader fis = new BufferedReader(new FileReader(patternsFile.toString()));
				}
			}
			*/
		}

		public void map(LongWritable key, Text arg1, Context context) {
//		public void map(LongWritable arg0, Text arg1,
//				OutputCollector<Text, Text> arg2, Reporter arg3)
//				throws IOException {
			
			String line = arg1.toString();
			if(caseLower) {
				line = arg1.toString().toLowerCase();
			} 
			line = line.replace("\u0001", " \u0001");
			String[] atom = line.split("\u0001");
			if(atom.length < LenLog) {  // �ֽ׶�refer_url�ֶβ�����
//				arg3.incrCounter(Counters.Useless, 1);
				context.getCounter(Counters.Useless).increment(1);
			} else {
//				String oper_time = atom[4], camp_id = atom[5], idea_id = atom[6];
						
				int  week = WEEK_DEF, hour = HOUR_DEF;
				int[] weekhour = DateParse.getWeekHour(atom[IdxTime], "yyyyMMddHHmmss");
				if(weekhour != null) {
					week = weekhour[0];
					hour = weekhour[1];
				} else {
					context.getCounter(Counters.UnUseDayweek).increment(1);
				}
				
				Integer ipdst = Ip2Dst.evaluateStr(atom[IdxIp]);  // IP
				if(ipdst == null) {
					context.getCounter(Counters.UnuseIP).increment(1);
					ipdst = IP_DEF;
//					ipdst = Integer.parseInt(atom[2]);  // for test.
				}
				
				String[] useragent = UAParse.agentInfo(atom[IdxUA]); //useragent
				if(useragent == null || useragent.length == 0) {
					context.getCounter(Counters.UnuseUA).increment(1);
					useragent = DEF_UA;
				}
				
				try {
					context.write(new Text(atom[IdxUserP]), new Text(
							String.format("%d,%d,%d,%s,%s", 
							week, hour, ipdst, useragent[0], useragent[2])));
				} catch (Exception e) {
					context.getCounter(Counters.UnMapout).increment(1);
				}
				//arg2 = mos.getCollector("chrono", arg3);  
                //arg2.collect(NullWritable.get(), new Text("chrono"));
			}	
		}
		
		public void cleanup(Context context) {
			// TODO
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
//	public static class Reduce extends MapReduceBase implements Reducer<Text, Text,
//			Text, Text> {
		
		private static enum Counters { 
			noInfoSZ, noInfoSZ_ERR, InfoSZ, InfoSZ_ERR 
		};
		private MultipleOutputs<Text, Text> mos=null;
		
		public void setup(Context context) throws IOException, InterruptedException {
			mos = new MultipleOutputs(context);
			super.setup(context);
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//		public void reduce(Text key, Iterator<Text> values, 
//				OutputCollector<Text, Text> output, Reporter reporter) {
			int[] cnt_week = new int[]{0, 0};
			int[] hours = new int[]{0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0};
			HashMap<String, Integer> IP = new HashMap<String, Integer>(4);
			HashMap<String, Integer> OS = new HashMap<String, Integer>(4);
			HashMap<String, Integer> browse = new HashMap<String, Integer>(4);
			
			int tmp = 0;
			for(Text t : values) {
//			while(values.hasNext()) {
				String[] varr = t.toString().split(",");  // values.next().toString().split(",");
				tmp = Integer.parseInt(varr[0]);
				if(tmp != MClickYiqifa.Map.WEEK_DEF) {
					cnt_week[tmp] += 1;					
				}
				
				tmp = Integer.parseInt(varr[1]);
				if(tmp != MClickYiqifa.Map.HOUR_DEF) {
					hours[tmp] += 1;  // no dispatch by week.
				}
				
				if(IP.containsKey(varr[2])) {
					IP.put(varr[2], IP.get(varr[2])+1);
				} else {
					IP.put(varr[2], 1);
				}
				
				if(OS.containsKey(varr[3])) {
					OS.put(varr[3], OS.get(varr[3])+1);
				} else {
					OS.put(varr[3], 1);
				}
				
				if(browse.containsKey(varr[4])) {
					browse.put(varr[4], browse.get(varr[4])+1);
				} else {
					browse.put(varr[4], 1);
				}
				
			}
			
			String tmps = "";
			tmps = String.format("%d,info:%s", UtilObj.max(cnt_week), UtilStr.iarr2str(cnt_week));
			String s = tmps;
			String s_sim = tmps.substring(0, tmps.indexOf(","));
			
			tmps = String.format("%d,info:%s", UtilObj.max(hours),UtilStr.iarr2str(hours));
			s = String.format("%s,%s", s, tmps);
			s_sim = String.format("%s,%s", s_sim, tmps.substring(0, tmps.indexOf(",")));
			
			tmp = 0;
			for(String k : IP.keySet()) {
				if(IP.get(k) > tmp) {
					tmp = IP.get(k);
				}
			}
			tmps = String.format("%d,info:%s", tmp,IP);
			s = String.format("%s,%s", s, tmps);
			s_sim = String.format("%s,%s", s_sim, tmps.substring(0, tmps.indexOf(",")));
			
			tmp = 0;
			for(String k : OS.keySet()) {
				if(OS.get(k) > tmp) {
					tmp = OS.get(k);
				}
			}
			tmps = String.format("%d,info:%s", tmp,OS);
			s = String.format("%s,%s", s, tmps);
			s_sim = String.format("%s,%s", s_sim, tmps.substring(0, tmps.indexOf(",")));
			
			tmp = 0;
			for(String k : browse.keySet()) {
				if(browse.get(k) > tmp) {
					tmp = browse.get(k);
				}
			}
			tmps = String.format("%d,info:%s", tmp,browse);
			s = String.format("%s,%s", s, tmps);
			s_sim = String.format("%s,%s", s_sim, tmps.substring(0, tmps.indexOf(",")));
			
			try {
				mos.write("withInfo", key, new Text(s));
				context.getCounter(Counters.InfoSZ).increment(1);
			} catch (Exception e) {  // (IOException e) {
				context.getCounter(Counters.InfoSZ_ERR).increment(1);
				e.printStackTrace();
			}
			try {
				mos.write("noInfo",  key, new Text(s_sim));
				context.getCounter(Counters.noInfoSZ).increment(1);
			} catch (Exception e) {
				e.printStackTrace();
				context.getCounter(Counters.noInfoSZ_ERR).increment(1);
			}
		}

		public void cleanup(Context context) {
			if(mos != null) {
				try {
					mos.close();
					super.cleanup(context);
				} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public static class PartitionerClass extends Partitioner<Text, IntWritable> {
		public int getPartition(Text key, IntWritable value, int numPartitions) {
			if (numPartitions >= 2)//Reduce �����ж� loglevel ���� logmodule ��ͳ�ƣ����䵽��ͬ�� Reduce
				if (key.toString().startsWith("logLevel::"))
					return 0;
				else if(key.toString().startsWith("moduleName::"))
					return 1;
				else return 0;
			else return 0;
		}
	}
	
	//REF: http://hadoop.apache.org/docs/current/api/org/apache/hadoop/mapreduce/lib/output/MultipleOutputs.html
	public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length != 2) {
			System.out.println("Usage: <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "user info for click log1");  //Job(new Configuration());
		
		job.setJarByClass(MClickYiqifa.class); 
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
//		job.setPartitionerClass(PartitionerClass.class); 
		job.setNumReduceTasks(8);  // ǿ����Ϊ2�� �Զ���Partitioner�Ŀ�ѡReduce-worker.
		// HashPartitioner is the default Partitioner.
		
		FileInputFormat.setInputPaths(job,  new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		// Defines additional single text based output 'text' for the job
		MultipleOutputs.addNamedOutput(job, "withInfo", TextOutputFormat.class,
				 Text.class, Text.class);

		 // Defines additional sequence-file based output 'sequence' for the job
		 MultipleOutputs.addNamedOutput(job, "noInfo",
				 SequenceFileOutputFormat.class, Text.class, Text.class);
		
		 Date startTime = new Date(); 
		 job.waitForCompletion(true);    
		 Date end_time = new Date(); 
		 System.out.println("The job took " + 
		    (end_time.getTime() - startTime.getTime()) /1000 + " seconds."); 
		 return 0;
	}
	
	public static void main(String[] args) { 
		int res; 
		try {
			res = ToolRunner.run(new Configuration(),new MClickYiqifa(), args);
			System.exit(res); 
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}
}