package com.emar.recsys.user.model;

import static org.junit.Assert.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * unit test.
 * @author zhoulm
 *
 */
public class PredictMergeTest {
	private static PredictMerge t_obj;
	/** 原始输入文件 */
	private static String [] t_line = new String[] {"", 
		"337319\t1:0\t3:2\t+\t0.021,0.037,*0.942", 
		"337319\t1:0\t3:2\t0.021,0.037,*0.942"};	
		
	private static String[][] c_line = new String[][] {
		{"", ""},
		{null, null}
		},
		/** 插入前后的json值 */
		t_json = new String[][] {
		{"", null}, {"{}", null},
		{"{\"SEX\":[]}", null}, 
		{"{\"At\":[1,2]}", null}},
		/** 待插入json */
		t_atom = new String[][] {
		{}, {null}, {""},
		};
	
	
	@BeforeClass
	public static void Init() {
		// 目前不测试主方法。
		t_obj = new PredictMerge(null, null, null);
	}
	@AfterClass
	public static void Destroy() {
	}
	
	/**
	 * 1 方法必须使用 Test 修饰
	 * 2 方法必须使用 public void 修饰，而且不能带有任何参数。 
	 */
	@Test
	public void testScorePredict() {
		// TODO 
		for (int i = 0; i < t_line.length; ++i) 
			assertArrayEquals(null, t_obj.ScorePredict(t_line[i]));
	}
	
	@Test
	public void testgetPredClass() {
		// TODO
		for (int i = 0; i < c_line.length; ++i) 
			assertEquals(null, t_obj.getPredClass(c_line[i][0]));
	}
	
	@Test
	public void testgetPredScore() {
		// TODO 
		for (int i = 0; i < c_line.length; ++i) 
			assertEquals(null, t_obj.getPredScore(c_line[i][1]));
	}
	
	@Test
	public void testJsonAdd() {
		// TODO
		for (int i = 0; i < t_json.length; ++i)
			assertEquals(null, t_obj.JsonAdd(t_json[i][0], null, t_json[i][2]));
	}

}
