package com.emar.recsys.user.action;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ActionFilterUtilTest {
	
	private static String[][] t_url = new String[][] {
		{"bj.jumei.com", "true"}, 
		{"http://www.sfbest.com/", "true"},
		{"http://www.sfbest.com/?etc_n=padnet&etc_s=yiqifa", "true"},
		
		{"http://sh.jumei.com/help/two_for_freeshipping?from=header", "false"},
	};

	@BeforeClass
	public static void Init() {
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

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
