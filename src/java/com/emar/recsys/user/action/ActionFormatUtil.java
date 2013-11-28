package com.emar.recsys.user.action;

import java.util.List;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * 基于 ActionExtract.class， 所有定制的格式化写的方法集合。
 * @author zhoulm
 * 
 */
public final class ActionFormatUtil {
	// copy of ActionExtract;
//	protected String userID;
//	protected JSONArray userAction;
	
	public ActionFormatUtil(String uid, JSONArray action) {
//		this.userID = uid;
//		this.userAction = action;
	}
	/** 格式化用户与其行为： @FMT uid\tType1=Act1, Type2=Act2, \t... */
	public static String actionPerline(String uid, JSONArray action) {
		if (uid == null || action == null)
			return "";
		StringBuffer sbuf = new StringBuffer();
		sbuf.append(uid + "\t");
		JSONObject jObj;
		for (int i = 0; i < action.length(); ++i) {
			jObj = action.getJSONObject(i);
			for (String k : (Set<String>)jObj.keySet()) {
				sbuf.append(k + "=" + jObj.get(k) + ", ");
			}
			sbuf.append("\t");
		}
		return sbuf.toString();
	}

	// TODO 
}
