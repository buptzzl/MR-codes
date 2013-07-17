package com.emar.recsys.user.util;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.text.ParseException;

import junit.framework.Assert;

/**
 * url解析类， 前缀必须是 protocal
 * @author zhoulm
 *
 */
public class UrlSlice {

	private static final String SEPA = "\u0001";
	private String urlCode;
	private URL urlUa;
	private Map<String, String> paramMap;

	private UrlSlice() {
	}

	public UrlSlice(String url) throws ParseException {
		this(url, "utf8");
	}

	public UrlSlice(String url, String encode) throws ParseException {
		if (url == null || url.length() == 0) {
			throw new ParseException(
					"init UrlSlice with NULL or empty-str. url=" + url,
					IUserException.ErrUrlNull);
		}
		try {
			url = URLDecoder.decode(url, encode);
			this.setUrl(url);
			this.urlCode = encode;
		} catch (Exception e) {
			throw new ParseException(
					"[Error] UrlSlice::URLDecoder. url=" + url 
					+ "\terror message:\n"+ e.getMessage(), IUserException.ErrUrlMalForm);
		}
		paramMap = new HashMap<String, String>();
		this.getParamsMap();
	}

	private boolean setUrl(String str) throws MalformedURLException {
		urlUa = new URL(str);
		return true;
	}

	public String getQuery() {
		return this.urlUa.getQuery();
	}
	public String getHost() {
		final String SEPA = ".";
		String host = urlUa.getHost();

		int cnt = UtilStr.SubStrCnt(host, SEPA);
		if (cnt <= 1) {
			return host;
		} else {
			int idx = host.lastIndexOf(SEPA);
			idx = host.substring(0, idx).lastIndexOf(SEPA);
			return host.substring(idx + 1);
		}
	}

	public String getParam(String key) {
		if (key == null) {
			return null;
		}
		return this.paramMap.get(key);
	}

	public boolean hasKey(String key) {
		return this.paramMap.containsKey(key);
	}

	public String[] getParamKeys() {
		Set<String> objs = this.paramMap.keySet();
		return objs.toArray(new String[objs.size()]);
	}

	/**
	 * 检索URL参数中给定KEY, 后继分割符之间的内容
	 * 
	 * @param key
	 *            待检索的查询参数中的KEY
	 * @param sepa
	 *            下一个边界的分割符
	 */
	public String urlParse(String key, String sepa) {
		if (key == null || sepa == null) {
			return null;
		}

		final String qstr = "?";
		final String estr = "=";
		int mlen = 1 + key.length(); // ��߽��ַ���
		String durl = this.urlUa.getQuery();

		int bpos = durl.indexOf(key);
		if (bpos == -1) { // the Para pos at first parameter.
			bpos = durl.indexOf(qstr + key);
			mlen += 1; // ǰ�߽��ַ���
		} else {
			mlen += sepa.length();
		}
		if (bpos == -1) {
			return null;
		}
		int epos = durl.indexOf(sepa, bpos + mlen);
		epos = epos == -1 ? durl.length() : epos;
		return durl.substring(bpos + mlen, epos);
	}

	private Map<String, String> getParamsMap() {
		String queryString = this.urlUa.getQuery();
		Map<String, StringBuffer> paramsMap = new HashMap<String, StringBuffer>();
		StringBuffer tbuf;
		if (queryString != null && queryString.length() > 0) {
			int ampersandIndex, lastAmpersandIndex = 0;
			String subStr, param, value;
			String[] paramPair, values, newValues;
			do {
				ampersandIndex = queryString.indexOf('&', lastAmpersandIndex) + 1;
				if (ampersandIndex > 0) {
					subStr = queryString.substring(lastAmpersandIndex,
							ampersandIndex - 1);
					lastAmpersandIndex = ampersandIndex;
				} else {
					subStr = queryString.substring(lastAmpersandIndex);
				}
				int idxval = subStr.indexOf("=");
				if(idxval == -1) {
					param = subStr;
					value = "";
				} else {
					param = subStr.substring(0, idxval);
					value = (idxval == subStr.length()-1)? "": subStr.substring(idxval+1); 
//				paramPair = subStr.split("=");  // 不采用split的方式
//				param = paramPair[0];
//				value = paramPair.length == 1 ? "" : paramPair[1];
				}
				/*
				 * try { value = URLDecoder.decode(value, enc); } catch
				 * (UnsupportedEncodingException ignored) { }
				 */
				if (paramsMap.containsKey(param)) {
					tbuf = paramsMap.get(param);
					tbuf.append(SEPA + value);
					/*
					 * int len = values.length; newValues = new String[len + 1];
					 * System.arraycopy(values, 0, newValues, 0, len);
					 * newValues[len] = value;
					 */
				} else {
					// newValues = new String[] { value };
					tbuf = new StringBuffer(value);
				}
				paramsMap.put(param, tbuf);
			} while (ampersandIndex > 0);
		}
		for (String k : paramsMap.keySet()) {
			this.paramMap.put(k, paramsMap.get(k).toString());
		}
		return this.paramMap;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		UrlSlice us;
		String purl[] = {
				"ftp://www.shanghaidz.com&l=http%3A//www.shanghaidz.com/&ax=0",
				"f://www.shanghaidz.com&l=http%3A//www.shanghaidz.com/",
				"www.shanghaidz.com",
				"http://p.yigao.com/imprImg.jsp?tc=&cc=&bgc=&bc=D9D9D9&tb=0&cb=0&tu=0&cu=0&uid=78576&zid=141276&pid=2&w=728&h=90&t=1&a=1&c=1&sid=e21d8d2246c20609&ua=Mozilla/5.0%20%28Windows%20NT%205.1%29%20AppleWebKit/537.1%20%28KHTML%2C%20like%20Gecko%29%20Chrome/21.0.1180.89%20Safari/537.1&n=Netscape&p=http:&v=5.0%20%28Windows%20NT%205.1%29%20AppleWebKit/537.1%20%28KHTML%2C%20like%20Gecko%29%20Chrome/21.0.1180.89%20Safari/537.1&r=http%3A//s.x.cn.miaozhen.com/ad/banner%3Fpid%3D106&ho=undefined&l=http%3A//s.x.cn.miaozhen.com/ad/banner%3Fpid%3D106&ax=0&ay=0&rx=0&ry=0&os=WinXP&scr=1920_1080&ck=true&s=1&ww=728&wh=90&ym=miaozhen&fs=-1&pvid=31736bd4022d72305836aab0aecabb48&yhc=85889&msid=2085eb432053cbab" };
		for (String url : purl) {
			try {
				us = new UrlSlice(url, "utf8");
				String lhost = "shanghaidz.com";
				System.out.println("[Info] url:" + url + "\ndecode-url:"
						+ us.urlUa + "\ntotal-key:"
						+ Arrays.asList(us.getParamKeys()) 
						+ "\nkey:l, val:" + us.getParam("l")
						+ "\nhost: " + us.getHost());
				
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		// Assert.assertEquals(lhost, us.getDomain(purl[0], "utf8", "l", "&"));
		// Assert.assertEquals(lhost, us.getDomain(purl[1], "utf8", "l", "&"));

	}

}
