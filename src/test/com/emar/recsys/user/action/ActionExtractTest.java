package com.emar.recsys.user.action;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ActionExtractTest {
	private static String[][] t_action = new String[][] {
			{ "", "", "false" }, // input, fmt-out, fmt-status
			{
					"emar@@@123@@@emar\t[\"{\\\"plat\\\":\\\"emarbox\\\",\\\"time\\\":\\\"20131124205720\\\",\\\"prod_name\\\":\\\"排放\\\"}\",\"{\\\"time\\\":\\\"20131124205721\\\",\\\"pagewords\\\":\\\"早孕试纸\\\"}\"]",
					"emar@@@123@@@emar\nprod_name=排放, time=20131124205720, pagewords=早孕试纸, time=20131124205721, ",
					"true" }, };
	private static ActionExtract tObj;

	@BeforeClass
	public static void Init() {
		List<String> t_data = new ArrayList<String>();
		for (int i = 0; i < t_action.length; ++i)
			t_data.add(t_action[i][0]);
		tObj = new ActionExtract(t_data);
	}

	@AfterClass
	public static void Destory() {
	}

	@Test
	public void testinit() {
		Assert.assertEquals(tObj.getInput(), null);
		for (int i = 0; i < tObj.size(); ++i)
			Assert.assertNotNull(tObj.getData(i));
	}

	@Test
	public void testbatchExtract() {
	}

	@Test
	public void testsingleExtract() {
		String res;
		int size;
		for (int i = 0; i < t_action.length; ++i) {
			size = tObj.size();
			Assert.assertEquals(
					ActionExtract.singleExtract(tObj, t_action[i][0]),
					t_action[i][1]);
			Assert.assertEquals(size, tObj.size());
		}
	}

	@Test
	public void testFilter() {
	}

	@Test
	public void testformat() {
		String res;
		for (int i = 0; i < t_action.length; ++i) {
			Assert.assertEquals("" + tObj.format(i), t_action[i][2]);
			res = tObj.getData(i);
			Assert.assertEquals(tObj.getData(i), t_action[i][1]);
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
