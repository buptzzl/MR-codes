package com.emar.recsys.user.demo;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import sun.org.mozilla.javascript.internal.ObjArray;


public class ConsumerHieraryTest {
	private static ConsumerHierary t_obj;
	/** 原始输入文件 */
	private static double [] t_line = new double[] { 
		0.1, 0.05, 0.2, 0.23, 0.23, 0.24, 0.67, 0.45, 0.55, 0.89, 0.93, 0.95
	};
	private static int N = 11;  // 去重后的有效原子个数 
	private static Object [][] t_layer = new Object[][] {
		{0, 1}, {"?", -1}, 
		{0.1, 2}, {"0.1", 2},
		{0.3, 3}, {0.5, 3}, {0.8, 4}, {0.95, 5}
	};
	private static Object[][] t_update = new Object[][] {
		{null, false}, {"aa", false}, 
		{0, true}, {10, true}, {0.5, true}
	};
	
	
	@BeforeClass
	public static void Init() {
		// 目前不测试主方法。
		t_obj = new ConsumerHierary();
		for (int i = 0; i < t_line.length; ++i)
			t_obj.update(t_line[i]);
		Assert.assertEquals(N, t_obj.getDataSize());
	}
	@AfterClass
	public static void Destroy() {
	}
	
	@Test
	public void testgetDataSize() {
	}
	@Test
	public void testsetMaxLayer() {
	}
	
	@Test
	public void testupdate() {
		for (int i = 0; i < t_update.length; ++i) { 
			Assert.assertEquals(t_update[i][1], t_obj.update(t_update[i][0]));
			t_obj.delete(t_update[i][0]);
		}
	}
	
	@Test
	public void testgetLayer() {
		for (int i = 0; i < t_layer.length; ++i) 
			Assert.assertEquals(t_layer[i][1], t_obj.getLayer(t_layer[i][0]));
	}
	
	@Test
	public void testgetMaxLayer() {
		Assert.assertEquals(5, t_obj.getMaxLayer());
	}
	
}
