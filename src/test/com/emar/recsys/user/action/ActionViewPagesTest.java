package com.emar.recsys.user.action;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.emar.util.UtilAccess;

public class ActionViewPagesTest {
	private static String[][] t_action = new String[][] {
		{"", "", "false"}, // input, fmt-out, batch-status
		{"emar\t[\"{\\\"time\\\":\\\"20131124205717\\\"}\"]", "emar\ntime=20131124205717, ", "false", "false"},
		{"emar\t[\"{\\\"time\\\":\\\"20131124205718\\\"}\"]", "emar\ntime=20131124205718, ", "true", "false"},
		{"emar\t[\"{\\\"time\\\":\\\"20131124205719\\\"}\"]", "emar\ntime=20131124205719, ", "true", "false"},
		{"emar@@@123@@@emar\t[\"{\\\"plat\\\":\\\"emarbox\\\",\\\"time\\\":\\\"20131124205720\\\",\\\"prod_name\\\":\\\"早孕\\\"}\",\"{\\\"time\\\":\\\"20131124205721\\\",\\\"pagewords\\\":\\\"早孕试纸\\\"}\"]", 
			"emar@@@123@@@emar\nprod_name=早孕, time=20131124205720, pagewords=早孕试纸, time=20131124205721, , [早孕]", "true", "true"},
		{"emar@@@123@@@emar\t[\"{\\\"plat\\\":\\\"emarbox\\\",\\\"time\\\":\\\"20131124205720\\\",\\\"prod_name\\\":\\\"早孕派\\\"}\"]",
			"emar@@@123@@@emar\nprod_name=早孕派, time=20131124205720, ", "false", "false"},
		{null},
	}; 
	private static ActionViewPages tObj;
	
	@BeforeClass
	public static void Init() throws IllegalAccessException, NoSuchFieldException {
		List<String> t_data = new ArrayList<String>();
		for (int i = 0; i < t_action.length; ++i) 
			t_data.add(t_action[i][0]);
		tObj = new ActionViewPages(t_data);
		Field tField = UtilAccess.getField(ActionViewPages.class, tObj, "WordsWhite");
		String[] words = (String[])tField.get(tObj);
		words[0] = "早孕";
		tField = UtilAccess.getField(ActionViewPages.class, tObj, "WordsBlack");
		words = (String[])tField.get(tObj);
		words[0] = "早孕派";
	}
	@AfterClass
	public static void Destory() {
	}
	
	@Test
	public void testBatchFormat() {
		Assert.assertEquals(tObj.BatchFormat(), true);
		for (int i = 0; i < t_action.length - 1; ++i) {
			Assert.assertEquals(tObj.getData(i), t_action[i][1]);
			Assert.assertEquals("" + tObj.getFlag(i), t_action[i][2]);
//			for (int j = i - 1; 0 <= j && (i - j) < tObj.getRange(); --j)
//				Assert.assertEquals(tObj.getFlag(i), "true"); // 被修改
		}
	}

	@Test
	public void testFilter() {
		// TODO
	}
	
	@Test
	public void testwhiteFilter() {
		// TODO
	}
	@Test
	public void testblackFilter() {
		
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
