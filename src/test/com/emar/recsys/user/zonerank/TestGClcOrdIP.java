package com.emar.recsys.user.zonerank;

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

import com.emar.recsys.user.zonerank.GClickOrderIP.MapClick;

//import com.emar.recsys.user.*;
public class TestGClcOrdIP {

//	MapDriver<Text, Text, Text, Text> mapOrder;
	MapDriver<LongWritable, Text, Text, Text> mapDriver;
	ReduceDriver<Text, Text, Text, Text> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;
//	MapReduceDriver<Text, Text, Text, Text, Text, Text> mapOrderDriver;

	Configuration conf = new Configuration();

	private Pair<LongWritable, Text> inorder;
	private Pair<LongWritable, Text> indata;
	// shuffled and sorted data
	private static List<Pair<Text, List<Text>>> shuffledData;

	private void initData() {
		Text value = new Text("add69c2f2fd282210bbbeaf076d2b8d9\u0001\u0001112.236.22.25\u00016bdd89e9-14c3-31ea-8e8f-658929b69911\u000120130522210018\u000182510\u0001115736\u00011\u0001141985\u0001Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1");
		LongWritable key = new LongWritable(1);
		indata = new Pair<LongWritable, Text>(key, value);
		
		Text valord = new Text("6621\t[50011884=2.0]");
		LongWritable keyord = new LongWritable(100);
		inorder = new Pair<LongWritable, Text>(keyord, valord);
		
		List<Text> vallist = new ArrayList<Text>(2);
		vallist.add(value);
		vallist.add(valord);
		
	}

	@Before
	public void setUp() {
		initData();
		conf.set(
				"io.serializations",
				"org.apache.hadoop.io.serializer.JavaSerialization,"
						+ "org.apache.hadoop.io.serializer.WritableSerialization");
		// mapDriver.setConfiguration(conf);

		MapClick mapClick = new MapClick();
//		mapDriver = MapDriver.newMapDriver(mapper);
		mapOrder = MapDriver.newMapDriver(mapClick);

		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		// reduceDriver.setConfiguration(conf);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}

	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(indata);

		// expected output result
		mapDriver.withOutput(new Text("6"), new IntWritable(1));
		mapDriver.runTest(); // ִ�в��Եķ���һ
		// assert ʧ�ܺ��²����벻ִ��
		// assertEquals("Expected 1 counter increment", 1,
		// mapDriver.getCounters().findCounter(CDRCounter.NoSMSCDR).getValue());

		Text mout1 = new Text("6"), mout2 = new Text("6"), mout3 = new Text("8");
		IntWritable mvout1 = new IntWritable(1), mvout2 = new IntWritable(2);
		// shuffle and sort
		List<Pair<Text, IntWritable>> pairs = new ArrayList<Pair<Text, IntWritable>>();
		pairs.add(new Pair<Text, IntWritable>(mout1, mvout1));
		pairs.add(new Pair<Text, IntWritable>(mout2, mvout1));
		pairs.add(new Pair<Text, IntWritable>(mout3, mvout2));
		shuffledData = mapReduceDriver.shuffle(pairs); // size=2.
	}

	@Test
	public void testReduce() throws IOException {
		// feed input to one single reduce call
		Pair<Text, List<IntWritable>> pair = shuffledData.get(0);
		reduceDriver.withInput(pair.getFirst(), pair.getSecond());

		// reducer's output
		List<Pair<Text, IntWritable>> result = reduceDriver.run();

		Assert.assertEquals("Key mismatch!", new Text("6"), result.get(0)
				.getFirst());
		Assert.assertEquals("Value mismatch!", new IntWritable(2), result
				.get(0).getSecond());
	}

	public void testMR() throws IOException {
		mapReduceDriver.withInput(new LongWritable(), new Text(
				"655209;1;796764372490213;804422938115889;6"));
		mapReduceDriver.withInput(new LongWritable(), new Text(
				"655209;1;796764372490213;804422938115889;6"));
		mapReduceDriver.withOutput(new Text("6"), new IntWritable(2));
		mapReduceDriver.runTest();
		assertEquals("Expected 1 counter increment", 2, mapDriver.getCounters()
				.findCounter(CDRCounter.NoSMSCDR).getValue());
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
