package com.emar.recsys.user.util.mr;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import javax.management.RuntimeErrorException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 合并多个 小文件 做 Map 输入的示例代码。 
 * @author zhoulm
 *
 */
public class CombineSmallFilesMR {
	public static boolean debug = true;
	private static final String UTF8 = "UTF-8";
	private static final byte[] newline;
	
	static {
		try {
			newline = "\n".getBytes(UTF8);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e.getMessage());
		} 
	}
	
	public static class CombineSmallfileMapper 
			extends Mapper<LongWritable, BytesWritable, Text, BytesWritable> {

		private Text file = new Text();

		@Override
		protected void map(LongWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
			String fileName = context.getConfiguration().get("map.input.file.name");
			file.set(fileName);
			context.write(file, value);
			if (debug) 
				System.out.println("[info] k="+file+"\nv="+value);
		}
	}
	
	public static class IdentityReducer
			extends Reducer<Text, BytesWritable, Text, Text> {
		
		private Text oval = new Text();
		private byte[] bytes;
		
		@Override
		protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
			
			for (BytesWritable value : values) {
				bytes = value.getBytes();
				oval.set(bytes);
				oval.append(newline, 0, newline.length);// 恢复换行
				
				context.write(key, oval);
			}
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage:  <in> <out>");
			System.exit(2);
		}

		conf.setInt("mapreduce.job.max.split.locations", 100);
		conf.setInt("mapred.min.split.size", 1024000);
		conf.setLong("mapred.max.split.size", 26214400); // 25m

		conf.setInt("mapred.reduce.tasks", 5);

		Job job = new Job(conf, "combine smallfiles");
		job.setJarByClass(CombineSmallFilesMR.class);
		job.setMapperClass(CombineSmallfileMapper.class);
//		job.setNumReduceTasks(0);
		job.setReducerClass(IdentityReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(CombineFilesInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		HdfsDAO hdao = new HdfsDAO(conf);
		hdao.rmr(otherArgs[1]);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		int exitFlag = job.waitForCompletion(true) ? 0 : 1;
		System.exit(exitFlag);

	}

}