package com.emar.recsys.user.action;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * 将关键词KEY 的全部Value 合并，分割符为 ", ". 
 * @author zhoulm
 *
 */
public class ActionCombiner extends ActionExtract {
//	private String SEPA_COMB;
	
	public ActionCombiner() {
		super();
	}
	public ActionCombiner(List<String> data) {
		super(data);
	}
	public ActionCombiner(String[] args) throws FileNotFoundException {
		super(args);
	}
	
	@Override
	public String formatUserActions(String uid, JSONArray action) {
		if (action == null || action.length() == 0)
			return "";
		StringBuffer sbuf = new StringBuffer();
		JSONObject jobj = null;
		for (int i = 0; i < action.length(); ++i) {
			jobj = action.getJSONObject(i);
			for (Object ki : jobj.keySet()) {
				sbuf.append(jobj.get((String)ki) + ", ");
			}
		}
		return sbuf.toString();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ActionCombiner actComb;
		try {
			actComb = new ActionCombiner(args);
			actComb.batchExtract(actComb);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
