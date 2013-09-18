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

import com.emar.util.HdfsIO;

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
		final String pconf = "/recommend/user/zhouliaoming/conf/ItemClasses"; 
		DistributedCache.addCacheFile(new URI(
				pconf + "/fourthdict#fourthdict"), conf);// 这个也是可以的
		DistributedCache.addCacheFile(new URI(
				pconf + "/thirddict#thirddict"), conf);// 这个也是可以的
		DistributedCache.addCacheFile(new URI(
				pconf + "/seconddict#seconddict"), conf);// 这个也是可以的
		DistributedCache.addCacheFile(new URI(
				pconf + "/firstdict#firstdict"), conf);// 这个也是可以的
		DistributedCache.addCacheFile(new URI(
				pconf + "/brandwords#brandwords"), conf);// 这个也是可以的
		DistributedCache.addCacheFile(new URI(
				pconf + "/abcmap#abcmap"), conf);// 这个也是可以的
		DistributedCache.addCacheFile(new URI(
				pconf + "/districtwords#districtwords"), conf);// 这个也是可以的
		DistributedCache.addCacheFile(new URI(
				pconf + "/colorwords#colorwords"), conf);// 这个也是可以的
		DistributedCache.addCacheFile(new URI(
				pconf + "/packwords#packwords"), conf);// 这个也是可以的
		DistributedCache.addCacheFile(new URI(
				pconf + "/firstcate#firstcate"), conf);// 这个也是可以的
		DistributedCache.addCacheFile(new URI(
				pconf + "/secondcate#secondcate"), conf);// 这个也是可以的
		DistributedCache.addCacheFile(new URI(
				pconf + "/thirdcate#thirdcate"), conf);// 这个也是可以的
		DistributedCache.addCacheFile(new URI(
				pconf + "/fourthcate#fourthcate"), conf);// 这个也是可以的
		DistributedCache.addCacheFile(new URI(
				pconf + "/yanjingwords#yanjingwords"), conf);// 这个也是可以的
		DistributedCache.addCacheFile(new URI(
				pconf + "/brand2cate#brand2cate"), conf);// 这个也是可以的
		DistributedCache.addCacheFile(new URI(
				pconf + "/combinewords#combinewords"), conf);// 这个也是可以的
		DistributedCache.addCacheFile(new URI(
				pconf + "/catewords#catewords"), conf);// 这个也是可以的
		DistributedCache.addCacheFile(new URI(
				pconf + "/code2catewords#code2catewords"), conf);// 这个也是可以的
		DistributedCache.addCacheFile(new URI(
				pconf + "/brandworddf2#brandworddf2"), conf);// 这个也是可以的
		DistributedCache.addCacheFile(new URI(
				pconf + "/brandascate#brandascate"), conf);// 这个也是可以的
		DistributedCache.addCacheFile(new URI(
				pconf + "/landscapewords#landscapewords"), conf);// 这个也是可以的
		DistributedCache.addCacheFile(new URI(
				pconf + "/parentcate#parentcate"), conf);// 这个也是可以的
		DistributedCache.addCacheFile(new URI(
				pconf + "/genderjudgewords#genderjudgewords"),
				conf);// 这个也是可以的
		DistributedCache.addCacheFile(new URI(
				pconf + "/genderwords#genderwords"), conf);// 这个也是可以的
		DistributedCache.addCacheFile(new URI(
				pconf + "/adjwords#adjwords"), conf);// 这个也是可以的
		DistributedCache.addCacheFile(new URI(
				pconf + "/wordprob#wordprob"), conf);// 这个也是可以的
		DistributedCache.addCacheFile(new URI(
				pconf + "/nwords#nwords"), conf);// 这个也是可以的
		DistributedCache.addCacheFile(new URI(
				pconf + "/verbwords#verbwords"), conf);// 这个也是可以的

		String[] oargs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (oargs.length != 4) {
			System.out
					.println("Usage:<out> <data-range> <time-FMT-in-path> <path-fmt>");
			System.exit(4);
		}

		Job job = new Job(conf, "[user and camp itemclassify rank]");
		job.setJarByClass(Iclassify2.class);
		job.setMapperClass(IclassifyMap2.class);
		// job.setCombinerClass(IclassifyReducer.class);
		job.setReducerClass(IclassifyReduce2.class);
		job.setNumReduceTasks(32);

		// FileInputFormat.addInputPath(job, new Path(args[0]));
		HdfsIO.setInput(oargs[1], oargs[2], oargs[3], job);
		FileOutputFormat.setOutputPath(job, new Path(args[0]));

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
