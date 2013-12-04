package com.emar.recsys.user.action;

import junit.framework.Assert;

import org.json.JSONArray;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ActionCombinerTest {
	private static String[][] tData = new String[][] {
		{ // input, fmt-out, fmt-status
			"[{\"time\":\"20131124070217\",\"prod_name\":\"贝亲, 婴儿, 宝宝, 沐浴露, 200ml, ia111, \",\"pagewords\":\"强生, j&j, 婴儿, 天然, 舒, 润, 洗发露, 250ml, 强生, 官方, 授权, 旗舰店, 聚美优品, 专卖店, \"}]",
			"20131124070217, 强生, j&j, 婴儿, 天然, 舒, 润, 洗发露, 250ml, 强生, 官方, 授权, 旗舰店, 聚美优品, 专卖店, , 贝亲, 婴儿, 宝宝, 沐浴露, 200ml, ia111, , ",
			"true" 
		}, {
			
		}
	};
	private static ActionCombiner tObj;
	private static JSONArray jarr;
	
	@BeforeClass
	public static void Init() {
		jarr = new JSONArray(tData[0][0]);
		tObj = new ActionCombiner();
	}
	@AfterClass
	public static void Destory() {
	}

	@Test
	public void testinit() {
	}
	
	@Test
	public void testformatUserActions() {
		Assert.assertEquals(tData[0][1], tObj.formatUserActions("a", jarr));
	}
	

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
