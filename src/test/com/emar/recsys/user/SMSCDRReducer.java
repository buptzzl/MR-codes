package com.emar.recsys.user;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class SMSCDRReducer extends
		Reducer<Text, IntWritable, Text, IntWritable>{
	
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws java.io.IOException, InterruptedException {
		int sum = 0;
		for(IntWritable value : values) {
			sum += value.get();
		}
		context.write(key, new IntWritable(sum));  // Reduce out.
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
