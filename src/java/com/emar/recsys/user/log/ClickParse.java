package com.emar.recsys.user.log;

import java.text.ParseException;

/**
 * 按点击日志的格式解析一行内容
 * 
 * @author zhoulm
 * 
 */
public class ClickParse extends BaseLog {
	static {
		BaseLog.N = 9;
	}
	/*
	public String click_id;
	public String pv_id;
	public String ip;
	public String plat_user_id;
	public String oper_time;
	public String camp_id;
	public String idea_id;
	public String ad_modality;
	public String ad_zone_id;
	public String user_agent;
	public String user_id;
	*/
	public ClickParse(String line) throws ParseException {
		super(line);
		if (this.status) {

			atom[atom.length - 1] = atom[atom.length - 1].trim();

			click_id = atom[0];
			pv_id = atom[1];
			ip = atom[2];
			plat_user_id = atom[3];
//			oper_time = atom[4];
			time = atom[4];
			camp_id = atom[5];
			idea_id = atom[6];
			ad_modality = atom[7];
			ad_zone_id = atom[8];
			if (11 <= atom.length) {
				user_agent = atom[9];
				user_id = atom[10];
			} else {
				user_agent = "";
				user_id = "";
			}
		}
	}

	public String toString() {
		if (this.status) {
			return String
					.format(
							"click_id=%s\u0001pv_id=%s\u0001ip=%s\u0001"
									+ "plat_user_id=%s\u0001time=%s\u0001camp_id=%s\u0001idea_id=%s"
									+ "\u0001ad_modality=%s\u0001ad_zone_id=%s\u0001user_agent=%s\u0001user_id=%s",
							click_id, pv_id, ip, plat_user_id, time, camp_id, idea_id,
							ad_modality, ad_zone_id, user_agent, user_id);
		} else {
			return null;
		}
	}

	/**
	 * @param args
	 * @throws ParseException 
	 */
	public static void main(String[] args) {
		String s = "00571718cf0f7f8a4c4fbedd18ff8ed1\u0001\u0001223.241.176.231\u00019da54f58-f465-333b-870c-c0847a0edcd1\u000120130505040529\u000182511\u0001115737\u00011\u0001141783\u0001Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; QQDownload 732)";

		ClickParse opar;
		try {
			opar = new ClickParse(s);
			System.out.println(opar);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
