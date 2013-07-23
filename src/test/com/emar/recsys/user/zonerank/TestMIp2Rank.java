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

import com.emar.recsys.user.zonerank.MIp2Rank.Map;
import com.emar.recsys.user.zonerank.MIp2Rank.Reduce;

//import com.emar.recsys.user.*;
public class TestMIp2Rank {

	// MapDriver<Text, Text, Text, Text> mapOrder;
	MapDriver<LongWritable, Text, Text, MapWritable> mapDriver;
	ReduceDriver<Text, MapWritable, Text, Text> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, MapWritable, Text, Text> mapReduceDriver;

	Configuration conf = new Configuration();

	private Pair<LongWritable, Text> inorder;
	private Pair<LongWritable, Text> indata, indata2;
	private MapWritable map_mout;
	
	private List<Pair<Text, MapWritable>> movals;
	// shuffled and sorted data
	private static List<Pair<Text, List<MapWritable>>> shuffledData;

	private void initData() {
		Text value = new Text("16399\t[50013497=1.0]"), val2 = new Text("16399\t[50013497=3.0, 500=1.0]");
		LongWritable key = new LongWritable(1), key2 = new LongWritable(2);
		indata = new Pair<LongWritable, Text>(key, value);
		indata2 = new Pair<LongWritable, Text>(key2, val2);

		Text valord = new Text(
				"6621\t[50022688=48.0, 50011979=23.0, 50026957=2.0, 50024999=1.0]");
		LongWritable keyord = new LongWritable(100);
		inorder = new Pair<LongWritable, Text>(keyord, valord);

		movals = new ArrayList<Pair<Text, MapWritable>>();
		map_mout = new MapWritable();
		map_mout.put(new Text("50013497"), new FloatWritable(1));
		map_mout.put(new Text(MIp2Rank.MR_kcfmt), new FloatWritable());
		
		movals.add(new Pair<Text, MapWritable>(new Text("16399"), map_mout));
		MapWritable tmap = new MapWritable();
		tmap.put(new Text("50013497"), new FloatWritable(3));
		tmap.put(new Text("500"), new FloatWritable(1));
		tmap.put(new Text(MIp2Rank.MR_kufmt), new FloatWritable());
		movals.add(new Pair<Text, MapWritable>(new Text("16399"), tmap));
	}

	@Before
	public void setUp() {
		initData();
		conf.set("io.serializations",
				"org.apache.hadoop.io.serializer.JavaSerialization,"
						+ "org.apache.hadoop.io.serializer.WritableSerialization");

		Map mapClick = new Map();
		mapDriver = MapDriver.newMapDriver(mapClick);

		Reduce reducer = new Reduce();
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		// reduceDriver.setConfiguration(conf);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapClick, reducer);
	}

	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(indata);

		// expected output result
		mapDriver.withOutput(new Text("16399"), map_mout);

		mapDriver.runTest(); // ִ�в��Եķ���һ
		// mapDriver.getCounters().findCounter(CDRCounter.NoSMSCDR).getValue());

		shuffledData = mapReduceDriver.shuffle(this.movals); // size=1.
	}

	@Test
	public void testReduce() throws IOException {
		// feed input to one single reduce call
		Pair<Text, List<MapWritable>> pair = shuffledData.get(0);
		reduceDriver.withInput(pair.getFirst(), pair.getSecond());
		reduceDriver.withInput(shuffledData.get(1).getFirst(), shuffledData.get(1).getSecond());

		// reducer's output
		List<Pair<Text, Text>> result = reduceDriver.run();

		Assert.assertEquals("Key mismatch!", new Text("16399"), result.get(0)
				.getFirst());
		Assert.assertEquals("Value mismatch!", new Text("camp_[50013497=4.0, 500=1.0]"), result.get(0)
				.getSecond());
	}

	public void testMR() throws IOException {
		mapReduceDriver.withInput(new LongWritable(1), new Text(
				"16399\t[50013497=1.0]"));
		mapReduceDriver.withInput(new LongWritable(10), new Text(
				"6621\t[50022688=48.0, 50011979=23.0, 50026957=2.0, 50024999=1.0]"));
		mapReduceDriver.withOutput(new Text("16399"), new Text("camp_[50013497=4.0, 500=1.0]"));
		mapReduceDriver.runTest();
		assertEquals("Expected 1 counter increment", 1, mapReduceDriver.getCounters()
				.findCounter(Reduce.Counters.UnRout).getValue());
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		TestMIp2Rank test = new TestMIp2Rank();
		test.testMapper();
		test.testReduce();
		test.testMR();
	}

}
