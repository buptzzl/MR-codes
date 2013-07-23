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
import com.emar.recsys.user.zonerank.GClickOrderIP.ReduceClick;

//import com.emar.recsys.user.*;
public class TestGClcOrdIP {

//	MapDriver<Text, Text, Text, Text> mapOrder;
	MapDriver<LongWritable, Text, PriorPair, Text> mapDriver;
	ReduceDriver<PriorPair, Text, Text, Text> reduceDriver;
	MapReduceDriver<LongWritable, Text, PriorPair, Text, Text, Text> mapReduceDriver;

	Configuration conf = new Configuration();

	private Pair<LongWritable, Text> inorder;
	private Pair<LongWritable, Text> indata;
	// shuffled and sorted data
	private static List<Pair<PriorPair, List<Text>>> shuffledData;

	private void initData() {
		Text value = new Text("add69c2f2fd282210bbbeaf076d2b8d9\u0001\u0001112.236.22.25\u00016bdd89e9-14c3-31ea-8e8f-658929b69911\u000120130522210018\u000182510\u0001115736\u00011\u0001141985\u0001Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1");
		LongWritable key = new LongWritable(1);
		indata = new Pair<LongWritable, Text>(key, value);
		
		Text valord = new Text("6621\t[50022688=48.0, 50011980=47.0, 50010815=38.0, 50011978=33.0, 50011986=32.0, 50011977=27.0, 50011979=23.0, 50026439=16.0, 50008599=14.0, 50026441=13.0, 50010794=13.0, 50010797=10.0, 50013794=10.0, 50022677=10.0, 50011982=10.0, 50011990=9.0, 50016885=8.0, 50010808=7.0, 50016231=5.0, 50014259=5.0, 50022682=5.0, 50010798=4.0, 50010790=4.0, 50010810=3.0, 50019771=3.0, 50014763=3.0, 50010807=3.0, 50015193=3.0, 50006584=3.0, 1801=3.0, 50010789=3.0, 50014253=3.0, 50010788=3.0, 50010793=3.0, 50009845=2.0, 50023728=2.0, 50050431=2.0, 50026443=2.0, 50022689=2.0, 50010792=2.0, 50026957=2.0, 50024999=1.0, 50005774=1.0, 50023292=1.0, 50010817=1.0, 50009838=1.0, 50011998=1.0, 50012004=1.0, 50010805=1.0, 50011868=1.0, 50012392=1.0, 213202=1.0, 50022681=1.0, 50011983=1.0]");
		LongWritable keyord = new LongWritable(100);
		inorder = new Pair<LongWritable, Text>(keyord, valord);
		
		List<Text> vallist = new ArrayList<Text>(2);
		vallist.add(value);
		vallist.add(valord);
		PriorPair ppair = new PriorPair("6621", false);
		
	}

	@Before
	public void setUp() {
		initData();
		conf.set(
				"io.serializations", 
				"org.apache.hadoop.io.serializer.JavaSerialization,"
						+ "org.apache.hadoop.io.serializer.WritableSerialization");
		conf.setBoolean("isuid", true);
		// mapDriver.setConfiguration(conf);

		MapClick mapClick = new MapClick();
		mapDriver = MapDriver.newMapDriver(mapClick);
		
		ReduceClick reducer = new ReduceClick();
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		// reduceDriver.setConfiguration(conf);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapClick, reducer);
	}

	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(indata);

		// expected output result
		mapDriver.withOutput(new Text("6"), new IntWritable(1));
		mapDriver.runTest(); // ִ�в��Եķ���һ
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
