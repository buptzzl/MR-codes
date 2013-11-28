package com.emar.recsys.user.action;

import java.net.MalformedURLException;
import java.net.URL;

/**
 * 基于 ActionExtract.class， 所有定制的过滤方法的集合。
 * @author zhoulm
 * 
 */
public class ActionFilterUtil {
	
	private static String DESC_SEPA = "@@@";
	/**  URL的host与Query之间的空白字符数 */
	private static int DEF_URL_DIS = 3;
	
	public static boolean isBadUrl(String url) {
		return isBadUrl(url, DEF_URL_DIS);
	}
	
	/** 使用默认的空白字符个数。 */
	public static boolean isBadUrl(String url, int Nuseless) {
		try {
			URL pageUrl = new URL(url);
			int phost = url.indexOf(pageUrl.getHost()) + pageUrl.getHost().length();
			if ((url.length() <= (phost + Nuseless)) 
					|| ((url.length() <= (phost + pageUrl.getQuery().length() + Nuseless)))) {
				return true;
			}
		} catch (MalformedURLException e) {
			return true;
		}
		return false;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
