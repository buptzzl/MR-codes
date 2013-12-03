package com.emar.recsys.user.model.ins;

import java.util.Arrays;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ParseActionTest {
	
	private static String[][] tAct = new String[][] {
		// input, out-feature, out-status,
		{"emar@@@1\t[{\"prod_name\":\"童装\"},{\"page_url\":\"http://www.uniqlo.cn\",\"time\":\"20131124103713\",\"pagewords\":\"优衣库, 官方, 旗舰店, 优衣库, @@@, metadescflag, @@@, metakeywordsflag, \"}]",
			"[官方, 旗舰店, 优衣库, 童装]", "true"},
		{"emar@@@2\t[{\"prod_name\":\"童装\"},{}]", "[童装]", "true"},
		{"emar@@@2\t[{\"prod_name\":\"服装\"}]", "[]", "false"},
		{"ee", "[]", "false"},
		{"", "[]", "false"},
	};
	private static String[] tOther = new String[] {
		"@attribute user_positive {0, 1}", // attrbiute.
		"2", // min act freq./
		"1", // label value.
		"[prod_name, prod_type_name]",
		"total user=2, action-distribution=[0, 1, 2,", 
		"feature-distribution=[0, 1, 0, 0, 1,",
	};
	private static ParseAction tObj;
	
	@BeforeClass
	public static void Init() {
		tObj = new ParseAction("learn.conf"); //"D:\\Data\\MR-codes\\data\\test\\learn.conf");
	}
	@AfterClass
	public static void Destory() {
		String stat = tObj.getStaticInfo();
		Assert.assertEquals(true, stat.startsWith(tOther[4]));
		Assert.assertEquals(true, stat.indexOf(tOther[5]) != -1);
		System.out.println(tObj + "\n");
	}

	@Test
	public void testinit() {
		Assert.assertEquals(tOther[0], tObj.getAttribute());
		Assert.assertEquals(tOther[1], tObj.getMinAction()+"");
		Assert.assertEquals(tOther[2], tObj.getClassify());
		Assert.assertEquals(tOther[3], Arrays.asList(tObj.getKeyatoms())+"");
	}
	
	@Test
	public void testparseLine() {
		for (int i = 0; i < tAct.length; ++i) {
			Assert.assertEquals(tAct[i][2], tObj.parseLine(tAct[i][0])+"");
			Assert.assertEquals(tAct[i][1], Arrays.asList(tObj.getFeatures())+"");
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
