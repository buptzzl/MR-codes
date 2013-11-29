package com.emar.recsys.user.action;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.emar.util.UtilAccess;

public class ActionFeatureExtractTest {
	
	private static String[][] tAction = new String[][] {
		// input,Filter.out,format.out,format.data,format.flag,
		{  // 测试空串输入。
			"", "false", "false", "", "false", 
		}, {
			"emar@@@1@@@emar\t[\"{\\\"plat\\\":\\\"emarbox\\\",\\\"time\\\":\\\"20131124205720\\\",\\\"prod_name\\\":\\\"早孕派对\\\"}\",\"{\\\"pagewords\\\":\\\"早孕试纸@@@这个@@@c\\\"}\"]",
			"true", "true", 
			"emar@@@1@@@emar\t[{\"time\":\"20131124205720\",\"plat\":\"emarbox\",\"prod_name\":\"早孕派, 对, \"},{\"pagewords\":\"早孕试纸, @@@, 这个, @@@, c, @@@, \"}]\t{早孕试纸=[0]}", "true", 
		}, {// 测试  有效URL监测；黑白关键词识别；分词；
			"e2\t[\"{\\\"pagewords\\\":\\\"早孕试纸1\\\", \\\"page_url\\\":\\\"http://www.womai.com/sale/2013/hgh1311/\\\"}," +
			"{\\\"pagewords\\\":\\\"早孕试纸2\\\", \\\"page_url\\\":\\\"http://www.womai.com/\\\"}," +
			"{\\\"pagewords\\\":\\\"早孕试纸3\\\", \\\"page_url\\\":\\\"http://www.womai.com/?from=home_index_muti_p1\\\"}\"]",
			"true", "true", 
			"e2\t[{\"page_url\":\"http://www.womai.com/sale/2013/hgh1311/\",\"pagewords\":\"早孕试纸, 1, \"}]\t{早孕试纸=[0]}", "true", 
		}, {
			"e3\t[\"{\\\"prod_name\\\":\\\"悦活柠檬U格乳酸菌果汁饮品（瓶装 350ml）3瓶组合\\\"}\",\"{\\\"pagewords\\\":\\\"易购论坛 提供购物返现等全网购物优惠信息,轻松购物,快乐分享@@@网上购物分享网站首选易购论坛 @@@网上购物,易购论坛,购物分享网站,购物返现,分享社区,购物网@@@\\\"}\"]",
			"true", "true", 
			"e3\t[{\"prod_name\":\"悦活, 柠檬, u, 格, 乳酸菌, 果汁, 饮品, 瓶装, 350ml, 3, 瓶, 组合, \"},{\"pagewords\":\"易, 购, 论坛, 提供, 购物, 返, 现, 等, 全网, 购物, 优惠, 信息, 轻松, 购物, 快乐, 分享, @@@, 网上, 购物, 分享, 网站, 首选, 易, 购, 论坛, @@@, 网上, 购物, 易, 购, 论坛, 购物, 分享, 网站, 购物, 返, 现, 分享, 社区, 购物, 网, @@@, \"}]\t{}", "true", 
		},
	};
	private static ActionFeatureExtract tObj;
	
	@BeforeClass
	public static void Init() throws Exception {
		List<String> tData = new ArrayList<String>();
		for (int i = 0; i < tAction.length; ++i)
			tData.add(tAction[i][0]);
//		tObj = new ActionFeatureExtract(tData);
		ActionExtract tAct = new ActionExtract(tData);
		Field tField = UtilAccess.getField(ActionFeatureExtract.class, tAct, "WordsWhite");
		String[] words = (String[])tField.get(tAct);
//		words[0] = "早孕";
		tField.set(tAct, new String[] {"早孕", "妈妈", "宝宝"});
		tField = UtilAccess.getField(ActionFeatureExtract.class, tAct, "WordsBlack");
		words = (String[])tField.get(tAct);
		words[0] = "早孕派";
		tObj = new ActionFeatureExtract(tAct);
		tField = UtilAccess.getField(ActionFeatureExtract.class, tObj, "N_ACT_MIN");
		tField.set(tObj, 0);
	}
	@AfterClass
	public static void Destory() {
	}

	@Test
	public void testinitWords() {
		Assert.assertEquals(tObj.getBlackSize(), tObj.fastBlack.size());
		Assert.assertEquals(tObj.getWhiteSize(), tObj.fastWhite.size());
		Assert.assertEquals(0, tObj.hitBlack.size());
		Assert.assertEquals(0, tObj.hitWhite.size());
	}
	
	@Test
	public void testFilter() {
		for (int i = 0; i < tAction.length; ++i) {
			Assert.assertEquals(tAction[i][1], ""+tObj.Filter(i));
		}
	}
	
	@Test
	public void testformat() {
		for (int i = 0; i < tAction.length; ++i) {
			Assert.assertEquals(tAction[i][2], ""+tObj.format(i));
			Assert.assertEquals(tAction[i][3], tObj.getData(i));
			Assert.assertEquals(tAction[i][4], ""+tObj.getFlag(i));
		}
	}
	
	@Test
	@Deprecated
	public void testformatUserActions() {
		// 已有 testformat() 
		try {
			Init();
		} catch (Exception e) {
			System.out.println("error, [MSG]: " + e.getMessage());
		}
		tObj.BatchFormat();
		for (int i = 0; i <tAction.length; ++i) {
			Assert.assertEquals(tAction[i][3], tObj.getData(i));
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
