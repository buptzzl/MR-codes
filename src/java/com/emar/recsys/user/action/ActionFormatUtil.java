package com.emar.recsys.user.action;

import java.util.List;

import org.json.JSONArray;

/**
 * 基于 ActionExtract.class， 所有定制的格式化写的方法集合。
 * @author zhou
 *
 */
public final class ActionFormatUtil {
	// copy of ActionExtract;
	protected String userID;
	protected JSONArray userAction;
	
	public ActionFormatUtil(String uid, JSONArray action) {
		this.userID = uid;
		this.userAction = action;
	}

	// TODO 
}
