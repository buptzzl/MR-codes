package com.emar.recsys.user.mahout;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.emar.classify.GoodsMark;
import com.emar.recsys.user.item.ItemAttribute;
import com.emar.recsys.user.log.LogParse;
import com.emar.recsys.user.util.UtilStr;
import com.emar.recsys.user.util.mr.HdfsDAO;
import com.emar.recsys.user.util.mr.HdfsIO;

public class ProducerItemRec extends Configured implements Tool {

	private static boolean debug = true;

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		private static LogParse logparse = null;

		private static enum Counters {
			Err, ErrLog, ErrMO, ErrInit, ErrPUid, ErrParse, ErrPName, ErrWSegment,
		};

		private ItemAttribute iseg = null;
		private String[] prodInfo = null;
		private String stmp;
		private Text okey = new Text(), oval = new Text();

		public void setup(Context context) {
			try {
				if (logparse == null)
					logparse = new LogParse();
				logparse.base.isdebug = debug;
				ItemAttribute.debug = debug; 
			} catch (ParseException e) {
				if (debug)
					System.out.println("[ERR] " + e.getMessage());
				System.exit(1);
			}
			
			Configuration conf = context.getConfiguration();
		}

		public void map(LongWritable key, Text value, Context context) {
			String line = value.toString();
			String path = ((FileSplit) context.getInputSplit()).getPath()
					.toString();
			logparse.reset();
			try {
				// this.logparse.base.isdug = true;
				logparse.parse(line, path);
			} catch (ParseException e1) {
			}
			if (!logparse.base.status) {
				context.getCounter(Counters.ErrParse).increment(1);
				if (debug)
					System.out.println("[ERROR] ProducerItemRec::map() "
							+ logparse);
				return;
			}
			if ((logparse.base.user_id == null || logparse.base.user_id
					.length() == 0)
					&& (logparse.base.plat_user_id == null || logparse.base.plat_user_id
							.length() == 0)) {
				context.getCounter(Counters.ErrPUid).increment(1);
				if (debug)
					System.out.println("[ERROR] ProducerItemRec::map() "
							+ logparse);
				return;
			}
			if ((logparse.base.prod_name == null)
					&& (logparse.base.prod_type_name == null)) {
				context.getCounter(Counters.ErrPName).increment(1);
				if (debug)
					System.out.println("[Err] ProducerItemRec::map() path="
							+ path + "\nparse-res=" + logparse + "\n"
							+ logparse.base.isdebug + "\nindata=" + line);
				return;
			} else {
				final int MinLen = 3;
				final String EMP = "";
				final Pattern cpatt = Pattern
						.compile("\\(|\\)|（|）|\\[|\\]|【|】|\\{|\\}| |,|\\?|;|\"|\\t|，|。|；|？|“|”|、|…|—|！|￥");
				prodInfo = new String[]{EMP, EMP};
				if (logparse.base.prod_type_name != null) {
					int[] winfo = UtilStr.strCharCnt(logparse.base.prod_type_name);
					if (winfo != null && winfo[0] > (logparse.base.prod_type_name.length()/3)) {
						String[] segments = cpatt.split(logparse.base.prod_type_name);
						if (3 < segments.length)
							prodInfo[0] = segments[segments.length - 1];
						else 
							prodInfo[0] = logparse.base.prod_type_name;
						prodInfo[1] = logparse.base.prod_name;
					}
				}  
				if (logparse.base.prod_name != null && prodInfo[0].equals(EMP)) {
					int[] winfo = UtilStr.strCharCnt(logparse.base.prod_name);
					if (winfo != null && winfo[0] > (logparse.base.prod_name.length()/3)) {
						String[] segments = cpatt.split(logparse.base.prod_name);
						if (3 < segments.length)
							prodInfo[0] = segments[segments.length - 1];
						else
							prodInfo[0] = logparse.base.prod_name;
					}
				}
				if (prodInfo[0].equals(EMP)){
					context.getCounter(Counters.ErrWSegment).increment(1);
					return;
				}
				/*
				try {
//					ItemAttribute.debug = debug;
					iseg = new ItemAttribute(String.format("%s %s",	
							logparse.base.prod_name, logparse.base.prod_type_name));
					prodInfo = iseg.getProd();
					if (prodInfo == null || prodInfo[0] == null 
							|| prodInfo[0].equals("null")) {
						context.getCounter(Counters.ErrWSegment).increment(1);
						if (debug)
							System.out.println("[ERR] prod-name-extract-is-null. \ninput="
								+ logparse.base.prod_name 
								+ "\nword="+iseg.getWord()+"\npos ="+iseg.getPos()
								+ "\nattr="+iseg.getAttribute()+"\nprod="+iseg.getProd());
						return;
					}
					// oval.set(prodInfo[0].hashCode()+"");
				} catch (ParseException e) {
					context.getCounter(Counters.ErrWSegment).increment(1);
					if (debug)
						System.out.println("[ERR] map() " + e.getMessage());
					return;
				}
				*/
			}
			stmp = logparse.buildUidKey();
			okey.set(String.format("%d,%d, %s, %s, %s, ", stmp.hashCode(), 
					prodInfo[0].hashCode(), stmp, prodInfo[0], prodInfo[1]));

			try {
				context.write(okey, oval);
			} catch (Exception e) {
				context.getCounter(Counters.ErrMO).increment(1);
			}
		}

		public void cleanup(Context context) {
			// TODO
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		private static enum Counters {
			HighFreq, RoutCnt
		};

		private static int TOPK;

		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			TOPK = conf.getInt("topk", 10);
		}

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

		}

		public void cleanup(Context context) {
		}
	}

	public int run(String[] args) throws IOException, InterruptedException,
			ClassNotFoundException {
		Configuration conf = new Configuration();
		conf.set("LEN", args[2]);
		String[] oargs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (oargs.length < 4) {
			System.out
					.println("Usage:<out> <data-range> <time-FMT-in-path> <path-fmt>");
			System.exit(4);
		}

		Job job = new Job(conf, "[order classid list]");
		job.setJarByClass(ProducerItemRec.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setNumReduceTasks(0);

		HdfsIO.setInput(oargs[1], oargs[2], oargs[3], job);
		HdfsDAO hdao = new HdfsDAO(conf);
		hdao.rmr(oargs[0]);
		FileOutputFormat.setOutputPath(job, new Path(oargs[0]));

		Date startTime = new Date();
		int res = job.waitForCompletion(true) ? 0 : 1;
		Date end_time = new Date();
		System.out.println("[INFO] The job took "
				+ (end_time.getTime() - startTime.getTime()) / 1000
				+ " seconds.");
		return res;
	}

	/**
	 * 
	 * @param args
	 *            input,output,LEN,key-list(order by importance)
	 */
	public static void main(String[] args) {
		int res;
		try {
			res = ToolRunner.run(new Configuration(), new ProducerItemRec(),
					args);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}