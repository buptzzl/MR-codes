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
//import org.apache.hadoop.mrunit.mapreduce.ReduceMultipleOutputsDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class TestUClickYiqifa {

	MapDriver<LongWritable, Text, Text, Text> mapDriver;
//	ReduceMultipleOutputsDriver<Text, Text, Text, Text> reduceDriver;
	ReduceDriver<Text, Text, Text, Text> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text,Text,Text,Text> mapReduceDriver;
	
	Configuration conf = new Configuration();
	
	private List<Pair<LongWritable, Text> > indata = new ArrayList<Pair<LongWritable, Text>>();
	private List<Pair<Text, Text>> mopairs = new ArrayList<Pair<Text, Text>>();
	//shuffled and sorted data
    private List<Pair<Text, List<Text> > > shuffledData = 
    		new ArrayList<Pair<Text, List<Text>>>(); 
	
	private void initData() {
		Text value = new Text("add69c2f2fd282210bbbeaf076d2b8d9\u0001\u0001112.236.22.25\u00016bdd89e9-14c3-31ea-8e8f-658929b69911\u000120130522210018\u000182510\u0001115736\u00011\u0001141985\u0001Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.1 (KHTM, like Gecko) Chrome/21.0.1180.89 Safari/537.1"),
				val2 = new Text("ec9ab7f6e605850b5eda0c18f1f7a4d4\u00014cf9474c1acca78b2d37b3b56452e3ef\u0001114.38.3.143\u00016a15edd3-12fa-32fb-8fd0-f1ab61d77756\u000120130525211925\u000179821\u0001103021\u00011\u0001143578\u0001Mozilla/5.0 (Windows NT 5.1; rv:20.0) Gecko/20100101 Firefox/20.0");
		Text moutk = new Text("6bdd89e9-14c3-31ea-8e8f-658929b69911"), 
				moutv = new Text("1,21,-1,CHROME,WINDOWS_XP"),
				moutk2 = new Text("6a15edd3-12fa-32fb-8fd0-f1ab61d77756"),
				moutv2 = new Text("0,21,-1,FIREFOX2,WINDOWS_XP");
		LongWritable key = new LongWritable(); 
		indata.add(new Pair<LongWritable, Text>(new LongWritable(1), value));
		indata.add(new Pair<LongWritable, Text>(new LongWritable(2), val2));
		mopairs.add(new Pair<Text, Text>(moutk, moutv));
		mopairs.add(new Pair<Text, Text>(moutk2, moutv2));
		shuffledData = mapReduceDriver.shuffle(mopairs);  // size=2.
	}
	
	@Before
	public void setUp() {
		
		conf.set("io.serializations","org.apache.hadoop.io.serializer.JavaSerialization," 
		            + "org.apache.hadoop.io.serializer.WritableSerialization");
//		mapDriver.setConfiguration(conf);
		
		MClickYiqifa.Map mapper = new MClickYiqifa.Map();
		MClickYiqifa.Reduce reducer = new MClickYiqifa.Reduce();
		mapDriver = MapDriver.newMapDriver(mapper);
//		mapDriver.setConfiguration(conf);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
//		reduceDriver.setConfiguration(conf);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
		
		initData();
	}
	
	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(indata.get(0))
				.withOutput(mopairs.get(0).getFirst(), mopairs.get(0).getSecond());
//		mapDriver.withInput(indata.get(1))
//				.withOutput(mopairs.get(1).getFirst(), mopairs.get(1).getSecond());
		//expected output result
		mapDriver.runTest();  // 执行测试的方法一
		// assert 失败后，下部代码不执行
//		assertEquals("Expected 1 counter increment", 1, 
//				mapDriver.getCounters().findCounter(CDRCounter.NoSMSCDR).getValue());

		
	}
	
	@Test
	public void testReduce() throws IOException {
		// feed input to one single reduce call
        Pair<Text, List<Text>> pair = shuffledData.get(0), pair2 = shuffledData.get(1);
        reduceDriver.withInput(pair.getFirst(), pair.getSecond())
        		.withInput(pair2.getFirst(), pair2.getSecond());

        //reducer's output
        List<Pair<Text, Text>> result = reduceDriver.run();

        Assert.assertEquals("Key mismatch!", new Text("6"), result.get(0).getFirst());
        Assert.assertEquals("Value mismatch!", new IntWritable(2), result.get(0).getSecond());
	}
	
	public void testMR() throws IOException {
		// TODO
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
