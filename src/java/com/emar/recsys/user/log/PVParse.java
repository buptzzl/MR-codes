package com.emar.recsys.user.log;

import java.text.ParseException;

public class PVParse extends BaseLog {
	static {
		BaseLog.N = 11; 
	}
	/*
	public String pv_id;
	public String plat_user_id;
	public String ip;
	public String oper_time;
	public String page_url;
	public String refer_url;
	public String user_agent;
	public String ad_modality;
	public String ad_zone_id;
	public String camp_id;
	public String idea_id;
	public String user_id;
	*/
	public PVParse(String line) throws ParseException {
		super(line);
		if(this.status) {
			pv_id = atom[0];
			plat_user_id = atom[1];
			ip = atom[2];
			time = atom[3];
			page_url = atom[4];
			refer_url = atom[5];
			user_agent = atom[6];
			ad_modality = atom[7];
			ad_zone_id = atom[8];
			camp_id = atom[9];
			idea_id = atom[10];
			if(11 < atom.length) {
				user_id = atom[11];
			}
		}
	}
	
	public String toString() {
		if(this.status) {
			return String.format("pv_id=%s\u0001plat_user_id=%s\u0001ip=%s\u0001"
					+ "time=%s\u0001page_url=%s\u0001refer_url=%s\u0001user_agent=%s"
					+ "\u0001ad_modality=%s\u0001ad_zone_id=%s\u0001camp_id=%s\u0001"
					+ "idea_id=%s\u0001user_id=%s", pv_id,plat_user_id,ip,time,page_url,
					refer_url,user_agent,ad_modality,ad_zone_id,camp_id,idea_id,user_id);
		} else {
			return null;
		}
	}

	/**
	 * @param args
	 * @throws ParseException 
	 */
	public static void main(String[] args) {
		String s = "15c3545f2ccd82c257138b416d6f2013\u000113704624287628341189\u0001122.238.222.112\u000120130606040028\u0001http://www.zuile8.com/videoplay/9116-1-2.html\u0001undefined\u0001Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; QQDownload 718)\u00011\u0001\u00017100\u0001190534";
		PVParse pvp;
		try {
			pvp = new PVParse(s);
			System.out.println(pvp);
			pvp = new PVParse(null);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
