package com.emar.recsys.user.services;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class RedisKeyItemClassifyTest {
	
	private static String [][] tData = new String[][] {
		{null, null, null, null},
		{"a", "PC\u0001a", "PC\u0001plat\u0001a", "PC\u0001user\u0001a"},
		{"燕京菠萝果汁啤酒330ml", "PC\u0001燕京菠萝果汁啤酒330ml", 
			"PC\u0001plat\u0001燕京菠萝果汁啤酒330ml", "PC\u0001user\u0001燕京菠萝果汁啤酒330ml"},
	};
	private static String[] tKeys = new String[] {
		null, "plat", "user",
	}, tData1 = new String[] {
		null, "PC\u0001a\u0001plat", "PC\u0001a\u0001user", "a",  
	};
	
	@BeforeClass
	public static void Init() {}
	@AfterClass
	public static void Destroy() {}
	
	@Test
	public void testKeyItemInfo() {
		for (int i = 0; i < tKeys.length; ++i)
			Assert.assertEquals(tData1[i], 
					RedisKeyItemclassify.keyItemInfo(tData1[3], tKeys[i]));
	}
	
	@Test
	public void testKeyItem() {
		for (int i = 0; i < tData.length; ++i) 
			Assert.assertEquals(tData[i][1], RedisKeyItemclassify.keyItem(tData[i][0]));
	}
	@Test
	public void testKeyItemPlatInfo() {
		for (int i = 0; i < tData.length; ++i) 
			Assert.assertEquals(tData[i][2], RedisKeyItemclassify.keyItemPlatInfo(tData[i][0]));
	}
	@Test
	public void testKeyItemUserInfo() {
		for (int i = 0; i < tData.length; ++i) 
			Assert.assertEquals(tData[i][3], RedisKeyItemclassify.keyItemUserInfo(tData[i][0]));
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("[test]");
	}

}
