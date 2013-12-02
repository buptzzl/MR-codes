package com.emar.recsys.user.action;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.emar.util.ConfigureTool;

/**
 * 基于 ActionExtract.class， 所有定制的过滤方法的集合。
 * @author zhoulm
 * 
 */
public class ActionFilterUtil {
	
	private static String DESC_SEPA = "@@@";
	/**  URL的host与Query之间的空白字符数 */
	private static int DEF_URL_DIS = 3;
	
	/** 基于配置 extract.badurl.[distance, urls] 过滤 */
	public static boolean isBadUrl(String url, ConfigureTool conf) {
		if (url == null || url.trim().length() == 0)
			return true;
		
		int urlDis = conf.getInt("extract.badurl.distance", DEF_URL_DIS);
		String[] badUrls = conf.getStrings("extract.badurl.urls", new String[]{});
		Set<String> furls = new HashSet<String>(Arrays.asList(badUrls));
		if (furls.contains(url))
			return true;
		return isBadUrl(url, urlDis);
	}
	
	public static boolean isBadUrl(String url) {
		return isBadUrl(url, DEF_URL_DIS);
	}
	
	/** 使用默认的空白字符个数。 */
	public static boolean isBadUrl(String url, int Nuseless) {
		if (url == null || url.trim().length() == 0)
			return true;
		try {
			URL pageUrl = new URL(url);
			int phost = url.indexOf(pageUrl.getHost()) + pageUrl.getHost().length();
			if ((url.length() <= (phost + Nuseless)) 
					|| (pageUrl.getQuery() != null && (url.length() <= (phost + pageUrl.getQuery().length() + Nuseless)))) {
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
