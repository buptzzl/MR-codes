package com.emar.util;

/**
 * 亿起发业务端 与 推荐组 的请求接口联调测试程序。 
 *
 */
public class HeadParams {
	private String qid = "0";
	private String platid = "0";
	private String uid = "0";
	private String atype = "0";
	private String url = "0";
	private String wid = "0";
	private String style = "0";
	private String cate = "0";

	public HeadParams() {
	}

	public HeadParams(String id) {
		this.qid = id;
		this.platid = id;
		this.uid = id;
		this.atype = id;
		this.url = id;
	}

	@Override
	public String toString() {
		String returns = "{\"qid\":\"" + qid + "\",\"platid\":\"" + platid
				+ "\",\"uid\":\"" + uid + "\",\"atype\":\"" + atype
				+ "\",\"url\":\"" + url + "\",\"data\":{\"wid\":\"" + wid
				+ "\",\"style\":\"" + style + "\",\"cate\"" + cate + "\"}}";
		while (returns.length() < 1024) {
			returns += '\0';
		}
		return returns;
	}

}
