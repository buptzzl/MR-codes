package com.emar.recsys.user.model.ins;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.emar.util.UtilAccess;

/** @deprecated */
public class DataMergeTest {
	private static String[][] tPos = new String[][] {
		// index, input, out-vecotr, 
		{
			"1", "emar@@@1\t[{\"prod_name\":\"童装\"},{\"page_url\":\"http://www.uniqlo.cn\",\"time\":\"20131124103713\",\"pagewords\":\"优衣库, 官方, 旗舰店, 优衣库, @@@, metadescflag, @@@, metakeywordsflag, \"}]", 
			"{1 1, 2 1, 3 1, 4 1, 5 1, 50 1}",
		}, {
			"1", "emar@@@2\t[{\"prod_name\":\"童装\"},{}]", "{24 1, 50 1}",
		}, {
			"1", "", ""
		}
	};
	private static String[] keyWords = {"购物","网站"}; //"童装","优衣库","官方","旗舰店","优衣库",
	private static Map<String, Integer> tFeatures = new HashMap<String, Integer>();
	private static String[][] tNeg = new String[][] {
		{
			"0", "emar@@@1\t[{\"prod_name\":\"易购\"},{\"page_url\":\"http://www.womai.com\",\"time\":\"20131124103713\",\"pagewords\":\"购物, 网站, @@@, 中粮, 我买网, 世界, 500, 强, @@@, 中粮, 我买网, \"}]", 
			"{41 1, 42 1, 45 1, 46 1}",
		}, {
			"0", "emar@@@2\t[{\"prod_name\":\"易购\"},{}]", "{41 1}",
		}
	};
	private static String[] tOther = new String[] {
		"parsers: [com.emar.recsys.user.model.ins.ParseAction, com.emar.recsys.user.model.ins.ParseAction], class labels: [0, 1],",
	};
	private static DataMerge tObj;
	
	@BeforeClass
	public static void Init() throws Exception {
		tObj = new DataMerge("learn.conf");
		// TODO add Feature raw words.
		Field tField = UtilAccess.getField(DataMerge.class, tObj, "insmerge");
		DataNormalize tFieldObj = (DataNormalize) tField.get(tObj);
		for (int i = 0; i < keyWords.length; ++i)
			tFieldObj.updateFeature(keyWords[i]);
//		tFieldObj
		System.out.println(tObj);
	}
	@AfterClass
	public static void Destory() {
	}
	
	@Test
	public void testinit() {
		Assert.assertEquals(true, tObj.toString().indexOf(tOther[0]) != -1);
	}
	
	@Test
	public void testprocess() throws IOException {
		Assert.assertEquals("0", ""+tObj.process());
	}
	
	@Test
	public void testparseLine() {
		for (int i = 0; i < tPos.length; ++i) {
			Assert.assertEquals(tPos[i][2], 
					tObj.parseLine(tPos[i][1], Integer.parseInt(tPos[i][0])));
		}
		for (int i = 0; i < tNeg.length; ++i) {
			Assert.assertEquals(tNeg[i][2], 
					tObj.parseLine(tNeg[i][1], Integer.parseInt(tNeg[i][0])));
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
