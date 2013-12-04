package com.emar.recsys.user.action;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.emar.util.ConfigureTool;

public class ActionFilterUtilTest {
	
	private static String[][] t_url = new String[][] {
		{"bj.jumei.com", "true"}, 
		{"http://www.sfbest.com/", "true"},
		{"http://www.sfbest.com/?etc_n=padnet&etc_s=yiqifa", "true"},
		{"http://sh.jumei.com/help/two_for_freeshipping?from=header", "false"},
		{"http://www.womai.com/productlist.do?mid=0&cid=6501&page=1&brand=970115&orderby=&ispromotions=&sellable=&allcolums=6501,970115&sellerid=", "false"},
		{"", "true"},
	};
	private static String[][] t_url2 = new String[][] {
		{"http://www.womai.com/index-0-0.htm", "true"},
		{"http://www.epetbar.com/goods/113470.html", "true"},
	};
	private static ConfigureTool conf;

	@BeforeClass
	public static void Init() {
		conf = new ConfigureTool();
		conf.addResource("user.conf");// 配置文件有对应的url
	}
	@AfterClass
	public static void destory() {
	}
	
	@Test 
	public void testisBadUrl() {
		for (int i = 0; i < t_url.length; ++i) {
			Assert.assertEquals(""+ActionFilterUtil.isBadUrl(t_url[i][0]), t_url[i][1]);
		}
	}
	
	@Test
	public void testisBadUrl2() {
		for (int i = 0; i < t_url2.length; ++i) {
			Assert.assertEquals(""+ActionFilterUtil.isBadUrl(t_url2[i][0], conf), t_url2[i][1]);
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
