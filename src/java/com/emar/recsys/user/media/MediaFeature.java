package com.emar.recsys.user.media;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.emar.recsys.user.util.UrlSlice;

/**
 * 解析出URL中的特征 
 * @author zhoulm
 * TODO
 */
public class MediaFeature {
	
	public static final String ksrc = "l";
	public static final String kpara = "?";
	public static final String pval = "=";
	
	/**
	 * url 字符串中的信息抽取
	 * @param url
	 * @param list
	 */
	public static String extractMediaFeature(String url, List<String> list) {
		if(url == null || list == null) {
			return null;
		}
		
		int idxParam = url.indexOf(kpara);
		String trimurl = url;
		String tmp;
		if(idxParam != -1) {
			try {
				UrlSlice urls = new UrlSlice(url);
				tmp = urls.getParam(ksrc);
				if(tmp != null) {
					trimurl = tmp;
				}
			} catch (ParseException e) {
			}
		}
		UrlExtract.UrlFeature(trimurl, list);
		// 不做参数抽取
//		Map<String, String[]>  params = UrlSlice.getParams(url, "utf8");
		return trimurl;
	}
	
	public static String extractMediaFeature(String url) {
		if(url == null) {
			return null;
		}
		
		List<String> list = new ArrayList<String>();
		extractMediaFeature(url, list);
		return list.toString();
	}
	


	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
