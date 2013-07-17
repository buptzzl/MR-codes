/**
 * @func learn MRUnit's use.
 * @author emar.zlm
 *
 */
package com.emar.recsys.user;

import java.io.*;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class SMSCDRMapper extends 
		Mapper<LongWritable, Text, Text, IntWritable>{
	
	private Text status = new Text();
	private final static IntWritable addOne = new IntWritable(1);
	
	static enum CDRCounter{
		NoSMSCDR; // used for counter.
	}
	
	protected void map(LongWritable key, Text value, Context context)
			throws java.io.IOException, InterruptedException {
		String[] line = value.toString().split(";");
		if (Integer.parseInt(line[1]) == 1) {
			status.set(line[4]);
			context.write(status, addOne);  // Map out.
		} else {  
			context.getCounter(CDRCounter.NoSMSCDR).increment(1);
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
	}

}