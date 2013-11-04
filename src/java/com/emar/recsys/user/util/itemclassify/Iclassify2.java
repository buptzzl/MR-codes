package com.emar.recsys.user.util.itemclassify;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.emar.recsys.user.util.mr.HdfsDAO;
import com.emar.recsys.user.util.mr.HdfsIO;

public class Iclassify2 {
	
	

	public static void main(String[] args) throws URISyntaxException,
			IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		// DistributedCache.addCacheFile(new
		// URI("hdfs://localhost:9000/user/ubuntu/cachefiles/KeyWord#KeyWord"),
		// conf);//这个是书上的例子
		// DistributedCache.addCacheFile(new
		// URI("hdfs://host116:9000/user/hadoop/wzcstop/stopword#stopword"),
		// conf);//这个可以正确实现
//		addClassCacheFile(conf);
		ItemSegmentClass.addClassCacheFile(conf);

		String[] oargs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (oargs.length != 5) {
			System.out.println("Usage:<out> <data-range> <time-FMT-in-path> " +
							"<path-fmt> <num-reduce>");
			System.exit(4);
		}

		Job job = new Job(conf, "[user and camp itemclassify rank]");
		job.setJarByClass(Iclassify2.class);
		job.setMapperClass(IclassifyMap2.class);
		// job.setCombinerClass(IclassifyReducer.class);
		job.setReducerClass(IclassifyReduce2.class);
		job.setNumReduceTasks(Integer.parseInt(oargs[4]));

		// FileInputFormat.addInputPath(job, new Path(args[0]));
		HdfsIO.setInput(oargs[1], oargs[2], oargs[3], job);
		
		HdfsDAO hdao = new HdfsDAO(conf);
		hdao.rmr(oargs[0]);
		FileOutputFormat.setOutputPath(job, new Path(oargs[0]));
		MultipleOutputs.addNamedOutput(job, "userClassrank",
				TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "userInfo", TextOutputFormat.class,
				Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "campClassrank",
				TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "campInfo", TextOutputFormat.class,
				Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "badclassify",
				TextOutputFormat.class, Text.class, Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		Date startTime = new Date();
		job.waitForCompletion(true);
		Date end_time = new Date();
		System.out.println("The job took "
				+ (end_time.getTime() - startTime.getTime()) / 1000
				+ " seconds.");
		return;

	}
}
