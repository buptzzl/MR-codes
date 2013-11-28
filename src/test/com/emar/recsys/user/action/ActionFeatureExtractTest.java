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
		// input,Filter.out,format.out,format.data,format.flag,FMTUserACT.out
		{"", "false", "true", "", "false","", }, {
			"emar@@@1@@@emar\t[\"{\\\"plat\\\":\\\"emarbox\\\",\\\"time\\\":\\\"20131124205720\\\",\\\"prod_name\\\":\\\"早孕派对\\\"}\",\"{\\\"pagewords\\\":\\\"早孕试纸\\\"}\"]",
			"true", "", "", "", "",
		}, {
			"e2\t[\"{\\\"pagewords\\\":\\\"早孕试纸1\\\", \\\"page_url\\\":\\\"http://www.womai.com/sale/2013/hgh1311/\\\"}," +
			"{\\\"pagewords\\\":\\\"早孕试纸2\\\", \\\"page_url\\\":\\\"http://www.womai.com/\\\"}," +
			"{\\\"pagewords\\\":\\\"早孕试纸3\\\", \\\"page_url\\\":\\\"http://www.womai.com/?from=home_index_muti_p1\\\"}\"]",
			"true", "", "", "", "",
		}, {
			"e3\t[\"{\\\"prod_name\\\":\\\"悦活柠檬U格乳酸菌果汁饮品（瓶装 350ml）3瓶组合\\\"}\",\"{\\\"pagewords\\\":\\\"易购论坛,国内最大的网上购物分享网站,提供购物返现等全网购物优惠信息,轻松购物,快乐分享@@@网上购物分享网站首选易购论坛,汇集了全网购物网站最新优惠资讯,数千万条网友购物心得,为您提供全方位的导购服务.而且我们热衷服务于热爱生活并喜欢网购的人群.我们的口号:轻松购物,快乐分享,最有价值的网购经验尽在易购论坛. @@@网上购物,易购论坛,购物分享网站,购物返现,分享社区,购物网@@@\\\"}\"]",
			"true", "", "", "", "",
		},
	};
	private static ActionFeatureExtract tObj;
	
	@BeforeClass
	public static void Init() throws IllegalAccessException, NoSuchFieldException {
		List<String> tData = new ArrayList<String>();
		for (int i = 0; i < tAction.length; ++i)
			tData.add(tAction[i][0]);
		tObj = new ActionFeatureExtract(tData);
		Field tField = UtilAccess.getField(ActionFeatureExtract.class, tObj, "WordsWhite");
		String[] words = (String[])tField.get(tObj);
		words[0] = "早孕";
		tField = UtilAccess.getField(ActionFeatureExtract.class, tObj, "WordsBlack");
		words = (String[])tField.get(tObj);
		words[0] = "早孕派";
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
			Assert.assertEquals(tAction[i][4], tObj.getFlag(i));
		}
	}
	
	@Test
	public void testformatUserActions() {
		tObj.BatchFormat();
		for (int i = 0; i <tAction.length; ++i) {
			Assert.assertEquals(tAction[i][5], tObj.getData(i));
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
