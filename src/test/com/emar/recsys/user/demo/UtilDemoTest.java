package com.emar.recsys.user.demo;


import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.emar.recsys.user.util.UtilJson;


public class UtilDemoTest {
	private static String[][] t_parseJL = {
		{null, "{}", null},  // [input, json-res, key]
		{"", "{}", null},
		{"{\"RawLog\":[[\"20130620170623\",\"50023706\",\"耳麦\",\"漫步者（EDIFIER）K800 时尚高品质耳麦 黑色\",\"69.0\",\"yinzuo100.com \"],[\"20130620170623\",\"110520\",\"鼠标垫\\/贴\\/腕垫\",\"漫步者精美鼠标垫\",\"0.0\",\"yinzuo100.com \"]],\"Uid\":\"emar@1\"}",
			"{\"RawLog\":[[\"20130620170623\",\"50023706\",\"耳麦\",\"漫步者（EDIFIER）K800 时尚高品质耳麦 黑色\",\"69.0\",\"yinzuo100.com \"],[\"20130620170623\",\"110520\",\"鼠标垫/贴/腕垫\",\"漫步者精美鼠标垫\",\"0.0\",\"yinzuo100.com \"]],\"Uid\":\"emar@1\"}",
			"emar@1"},
		{"emar@2\t{\"RawLog\":[[\"20130620170623\",\"110520\",\"鼠标垫\\/贴\\/腕垫\",\"漫步者精美鼠标垫\",\"0.0\",\"yinzuo100.com \"]]}",
			"{\"RawLog\":[[\"20130620170623\",\"110520\",\"鼠标垫/贴/腕垫\",\"漫步者精美鼠标垫\",\"0.0\",\"yinzuo100.com \"]],\"Uid\":\"emar@2\"}",
			"emar@2"}
	};
	
	
	@BeforeClass
	public static void Init() {
	}
	@AfterClass
	public static void Destory() {
	}
	
	@Test
	public void testparseJsonLine() {
		StringBuffer key = new StringBuffer("Uid");
		final int idx_j = 1, idx_k = 0;
		final String sepa = "\t";
		for (int i = 0; i < t_parseJL.length; ++i) {
//			String tmp = UtilDemo.parseJsonLine(t_parseJL[i][0], key)+"";
//			for(int j = 0; j < tmp.length(); ++j) 
//				if (tmp.charAt(j) != t_parseJL[i][1].charAt(j))
//					System.out.print("i="+j+" s_t="+tmp.charAt(j)+" s_p="+t_parseJL[i][1].charAt(j));
			Assert.assertEquals(t_parseJL[i][1], 
					UtilJson.parseJsonLine(t_parseJL[i][0], key, sepa, idx_j, idx_k)+"");
			Assert.assertEquals(key.toString(),
					t_parseJL[i][2] == null ? key.toString():t_parseJL[i][2]);
			key = new StringBuffer("Uid");
		}
	}
	
	
}
