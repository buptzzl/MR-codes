package com.emar.recsys.user.services;


/**
 * 通用的 Redis key 构造类。
 * @author zhoulm
 *
 * @UT done.
 */
public final class RedisKey {
	private static final String Table = "PC", Plat = "plat", User = "user";
	
	
	/** 商品分类结果的 key */
	public static String keyItem(String item) {
		String res = null;
		if (item != null)
			res = String.format("%s\u0001%s", Table, item);
		return res;
	}
	/** 商品分类结果的key + 容器域名; 构造key 的子域名 */
	public static String keyItemInfo(String item, String field) {
		if (field == null) 
			return null;
		item = keyItem(item);
		if (item != null) 
			item = String.format("%s\u0001%s", item, field);
		return item;
	}
	/** 商品出现的平台分布 
	 * @deprecated
	 */
	public static String keyItemPlatInfo(String item) {
		String res = null;
		if (item != null) 
			res = String.format("%s\u0001%s\u0001%s", Table, Plat, item);
		return res;
	}
	/** @deprecated */
	public static String keyItemUserInfo(String item) {
		String res = null;
		if (item != null)
			res = String.format("%s\u0001%s\u0001%s", Table, User, item);
		return res;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
