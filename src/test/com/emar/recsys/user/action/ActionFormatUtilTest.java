package com.emar.recsys.user.action;

import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class ActionFormatUtilTest {
	private static String[] t_action = new String[] {
		"emar@@@123@@@emar\t[\"{\\\"plat\\\":\\\"emarbox\\\",\\\"time\\\":\\\"20131124205720\\\",\\\"prod_name\\\":\\\"排放\\\"}\",\"{\\\"time\\\":\\\"20131124205721\\\",\\\"pagewords\\\":\\\"早孕试纸\\\"}\"]",
		
	};
	private static List<String> uids;
	private static List<JSONArray> actions;
	private static String[][] tActExp = new String[][] {
		// actionPerline. 
		{"", },// input:null
		{"\t", },// input: ""
		{"emar@@@123@@@emar\ttime=20131124205720, plat=emarbox, prod_name=排放, \ttime=20131124205721, pagewords=早孕试纸, \t"},
	};
	
	@BeforeClass
	public static void Init() {
		uids = new ArrayList<String>();
		actions = new ArrayList<JSONArray>();
		uids.add(null); uids.add("");
		actions.add(new JSONArray()); actions.add(new JSONArray());
		JSONObject jObj;
		JSONArray jArr, jActions;
		for (int i = 0; i < t_action.length; ++i) {
			String[] atoms = t_action[i].split("\t");
			uids.add(atoms[0]);
			jArr = new JSONArray(atoms[1]);
			jActions = new JSONArray();
			for (int j = 0; j < jArr.length(); ++j) {
				jObj = new JSONObject(jArr.getString(j));
				jActions.put(jObj);
			}
			actions.add(jActions);
		}
	}
	@AfterClass
	public static void Destory() {}
	
	@Test
	public void testactionPerline() {
		for (int i = 0; i < uids.size(); ++i) {
			String s = ActionFormatUtil.actionPerline(uids.get(i), actions.get(i));
			Assert.assertEquals(tActExp[i][0], 
					ActionFormatUtil.actionPerline(uids.get(i), actions.get(i)));
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
