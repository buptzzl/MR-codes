package com.emar.recsys.user.action;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


public class ActionExtractTest {
	private static String[][] t_action = new String[][] {
		{"", ""}, // input, data2in, data2out, 
		{"emar@@@123@@@emar\t[\"{\"plat\":\"emarbox\",\"time\":\"20131124205720\"}\",\"{\"plat\":\"test\"}\"]", 
			""},
	}; 
	private static ActionExtract tObj;
	
	
	@BeforeClass
	public static void Init() {
		tObj = new ActionExtract();
	}
	@AfterClass
	public static void Destory() {
	}
	
	@Test
	public void testinit() {
		Assert.assertEquals(tObj.getInput(), null);
	}
	
	@Test
	public void testbatchExtract() {}
	@Test
	public void testsingleExtract() {
		String res;
		int size;
		for (int i = 0; i < t_action.length; ++i) {
			size = tObj.getData().size();
			res = ActionExtract.singleExtract(tObj, t_action[i][0]);
			Assert.assertEquals(res, t_action[i][1]);
			Assert.assertEquals(size, tObj.getData().size());
		}
	}
	@Test
	public void testFilter() { }
	@Test
	public void testformat() {
//		String res;
		for (int i = 0; i < t_action.length; ++i) {
			tObj.getData().add(t_action[i][0]);
			tObj.format(i);
			Assert.assertEquals(t_action[i][1], tObj.getData().get(i));
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
