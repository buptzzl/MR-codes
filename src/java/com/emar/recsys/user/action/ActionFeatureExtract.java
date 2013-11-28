package com.emar.recsys.user.action;

import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;

import com.emar.recsys.user.util.UrlSlice;

/**
 * 定义过滤规则、 若干数据格式化规则的子类。方便主类进行调用。
 * 抽取聚合用户的关键内容、目标行为（关键词） 作为候选训练集。
 * 
 * @author zhoulm
 *
 * @FMT class,feature.
 */
public final class ActionFeatureExtract extends ActionExtract {
	
	private static String DESC_SEPA = "@@@";
	/**  URL的host与Query之间的空白字符数 */
	private static int URL_DIS = 3;
	
	
	private UrlSlice urlInfo;
	
	@Override
	public boolean format(int index) {
		//TODO
		
	};
	
	@Deprecated
	public String getClassify() {
		// TODO 生成类别信息。仅仅完成正负后续样本集生成，
		
	}
	
	public boolean isBadUrl(String url) {
		// TODO 基于URL 是否有效 进行过滤。
		try {
			URL pageUrl = new URL(url);
			if ((url.length() <= (url.indexOf(pageUrl.getHost()) + pageUrl.getHost().length() + URL_DIS)) 
					|| (pageUrl.getQuery().length() != 0 
					&& (url.length() <= (url.indexOf(pageUrl.getQuery()) + pageUrl.getQuery().length() + URL_DIS)))) {
				return false;
			}
		} catch (MalformedURLException e) {
			return false;
		}
		return true;
	}


	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
