package com.emar.recsys.user;

import static org.junit.Assert.*;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import com.emar.recsys.user.SMSCDRMapper.CDRCounter;
//import com.emar.recsys.user.*;

public class SMSCSR_MRTest {

	MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
	
	Configuration conf = new Configuration();
	
	private Pair<LongWritable, Text> indata;
	//shuffled and sorted data
    private static List<Pair<Text, List<IntWritable>>> shuffledData; 
	
	private void initData() {
		Text value = new Text("655209;1;796764372490213;804422938115889;6");
		LongWritable key = new LongWritable(); 
		indata = new Pair<LongWritable, Text>(key, value);
		
	}
	
	@Before
	public void setUp() {
		initData();
		conf.set("io.serializations","org.apache.hadoop.io.serializer.JavaSerialization," 
		            + "org.apache.hadoop.io.serializer.WritableSerialization");
//		mapDriver.setConfiguration(conf);
		
		SMSCDRMapper mapper = new SMSCDRMapper();
		SMSCDRReducer reducer = new SMSCDRReducer();
		mapDriver = MapDriver.newMapDriver(mapper);
//		mapDriver.setConfiguration(conf);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
//		reduceDriver.setConfiguration(conf);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}
	
	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(indata);
		
		//expected output result
		mapDriver.withOutput(new Text("6"), new IntWritable(1));
		mapDriver.runTest();  // 执行测试的方法一
		// assert 失败后，下部代码不执行
//		assertEquals("Expected 1 counter increment", 1, 
//				mapDriver.getCounters().findCounter(CDRCounter.NoSMSCDR).getValue());
		
		Text mout1 = new Text("6"), mout2 = new Text("6"), mout3 = new Text("8");
		IntWritable mvout1 = new IntWritable(1), mvout2 = new IntWritable(2);
		//shuffle and sort
		List<Pair<Text, IntWritable>> pairs = 
				  new ArrayList<Pair<Text, IntWritable>>();
		pairs.add(new Pair<Text, IntWritable>(mout1, mvout1));
		pairs.add(new Pair<Text, IntWritable>(mout2, mvout1));
		pairs.add(new Pair<Text, IntWritable>(mout3, mvout2));
		shuffledData = mapReduceDriver.shuffle(pairs);  // size=2.
	}
	
	@Test
	public void testReduce() throws IOException {
		// feed input to one single reduce call
        Pair<Text, List<IntWritable>> pair = shuffledData.get(0);
        reduceDriver.withInput(pair.getFirst(), pair.getSecond());

        //reducer's output
        List<Pair<Text, IntWritable>> result = reduceDriver.run();

        Assert.assertEquals("Key mismatch!", new Text("6"), result.get(0).getFirst());
        Assert.assertEquals("Value mismatch!", new IntWritable(2), result.get(0).getSecond());
	}
	
	public void testMR() throws IOException {
		mapReduceDriver.withInput(new LongWritable(), new Text(
				"655209;1;796764372490213;804422938115889;6"));
		mapReduceDriver.withInput(new LongWritable(), new Text(
				"655209;1;796764372490213;804422938115889;6"));
		mapReduceDriver.withOutput(new Text("6"), new IntWritable(2));
		mapReduceDriver.runTest();
		assertEquals("Expected 1 counter increment", 2, 
				mapDriver.getCounters().findCounter(CDRCounter.NoSMSCDR).getValue());
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		SMSCSR_MRTest test = new SMSCSR_MRTest();
		test.testMapper();
		test.testReduce();
		test.testMR();
	}

}
