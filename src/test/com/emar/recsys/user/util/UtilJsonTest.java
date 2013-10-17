package com.emar.recsys.user.util;

import static org.junit.Assert.assertEquals;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class UtilJsonTest {
	/** 原始输入文件 */
	private static String [][] t_line = new String[][] {
		{"emar\t{\"page_url\":\"http\"}", "{\"page_url\":\"http\",\"Uid\":\"emar\"}" }, 
		{"{\"page_url\":\"http\"}", "{\"page_url\":\"http\"}"},
		{ "emar\t[{\"http\":\"value\",\"a\":1}]", "{\"Arr\":[{\"http\":\"value\",\"a\":1}],\"Uid\":\"emar\"}" } ,
	}, t_arr = new String[][] {
		{ "emar\t[\"{\"page_url\":\"http\"}\"]", 
			"{\"Arr\":[\"page_url\":\"http\"]; \"Uid\":\"emar\"}" } ,
		
	}, t_exp = new String[][] { {null, null}, };
	
	@BeforeClass
	public static void Init() {}
	@AfterClass
	public static void Destroy() {}

	@Test 
	public void testparseJsonLine() {
		StringBuffer sbuf = new StringBuffer();
		for (int i = 0; i < (t_line.length - 1); ++i) { 
			assertEquals(t_line[i][1], UtilJson.parseJsonLine(t_line[i][0],
						sbuf, "\t", 1, 0)+"");
		}
		for (int i = 0; i < t_exp.length; ++i) {
			assertEquals(t_exp[i][1], UtilJson.parseJsonLine(t_exp[i][0], 
					null, null, -1, -1));
		}
	}

	@Test 
	public void testparseJson() {
		StringBuffer sbuf = new StringBuffer();
		for (int i = 0; i < t_line.length; ++i) {
			String s = UtilJson.parseJson(t_line[i][0], "\t", "Arr", 1, 0)+"";
			assertEquals(t_line[i][1], 
					UtilJson.parseJson(t_line[i][0], "\t","Arr", 1, 0)+"");
		}
		for (int i = 0; i < t_exp.length; ++i) {
			assertEquals(t_exp[i][1], 
					UtilJson.parseJson(t_exp[i][0], null, null, -1, -1));
		}
	}
	
	

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
