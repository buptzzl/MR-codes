package com.emar.recsys.user.log;

import java.text.ParseException;

import com.emar.recsys.user.util.IUserException;

/**
 * 日志解析基类
 * @author zhoulm
 *
 */
public class BaseLog {
	// 全部日志的全部字段的名字, 按字符顺序表示。 新增的字段须添加进来。
	public String ad_modality;
	public String ad_zone_id;
	public String camp_id;  // chuchuang中为camp_ids
	public String camp_ids;
	public String click_id;
	public String domain;
	public String idea_id;
	public String ip;
	public String mate_ids; 
//	public String oper_time;  // 与time合并
	public String order_id;
	public String order_no;
	public String order_price;
	public String orig_prod_type;
	public String page_url;
	public String plat_user_id;
	public String prod_cnt;
	public String prod_name;
	public String prod_no;
	public String prod_price;
	public String prod_type_name;
	public String pv_id;
	public String sw_id;
	public String time;
	public String refer_url;
	public String title;
	public String user_agent;  // chuchuang中为ua
	public String user_cookieid;
	public String user_id;
	
	public boolean status;
	public boolean isdug;
	
	protected String[] atom;
	protected static int N = 0; // 基本字段长度
	
	public BaseLog(String line) throws ParseException {
		this.fill(line);  // 与C++不同
	}
	
	// 重新解析一行， 实现对象重用
	public void fill(String line) throws ParseException {
		if(line == null) 
			throw new ParseException("input is null.", IUserException.ErrLogNull);
		
		this.reset();
		atom = line.concat(" ").split("\u0001");
		if(atom.length < BaseLog.N) {
			status = false;
		} else {
			status = true;
			atom[atom.length -1] = atom[atom.length - 1].trim();
		}
	}
	
	public void reset() {
		ad_modality = null;
		ad_zone_id = null;
		camp_id = null;
		camp_ids = null;
		click_id = null;
		domain = null;
		idea_id = null;
		ip = null;
		mate_ids = null;
//		oper_time = null;
		order_id = null;
		order_no = null;  //ad_modality,ad_zone_id,camp_id,camp_ids,click_id,domain,
		order_price = null;  //idea_id,ip,mate_ids,order_id,order_no,order_price,
		orig_prod_type = null;  //orig_prod_type,page_url,plat_user_id,prod_cnt,prod_name,
		page_url = null;  //prod_no,prod_price,prod_type_name,pv_id,refer_url,sw_id,time,
		plat_user_id = null;  //title,user_agent,user_cookieid,user_id
		prod_cnt = null;
		prod_name = null;
		prod_no =null;
		prod_price = null;
		prod_type_name = null;
		pv_id = null;
		refer_url = null;
		sw_id = null;
		time = null;
		title = null;
		user_agent = null;
		user_cookieid = null;
		user_id = null;
		
		status = false; 
		atom = null;
	}
	
	public String toString() {
		return String.format("status=%s\t"
				+ "ad_modality=%s,ad_zone_id=%s,camp_id=%s,camp_ids=%s,click_id=%s,domain=%s,"
				+ "idea_id=%s,ip=%s,mate_ids=%s,order_id=%s,order_no=%s,order_price=%s,"
				+ "orig_prod_type=%s,page_url=%s,plat_user_id=%s,prod_cnt=%s,prod_name=%s,"
				+ "prod_no=%s,prod_price=%s,prod_type_name=%s,pv_id=%s,refer_url=%s,sw_id=%s,time=%s,"
				+ "title=%s,user_agent=%s,user_cookieid=%s,user_id=%s",
				status, ad_modality,ad_zone_id,camp_id,camp_ids,click_id,domain,
				idea_id,ip,mate_ids,order_id,order_no,order_price,
				orig_prod_type,page_url,plat_user_id,prod_cnt,prod_name,
				prod_no,prod_price,prod_type_name,pv_id,refer_url,sw_id,time,
				title,user_agent,user_cookieid,user_id);
	}
	
	public void PVParse(String line) throws ParseException {
		this.fill(line);
		
		if(this.status && 10 < atom.length) {
			pv_id = atom[0];
			plat_user_id = atom[1];
			ip = atom[2];
//			oper_time = atom[3];
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
		
		if(this.isdug)
			System.out.println("BaseLog::PVParse\tres:\n" + this.toString() + "\ndata=" + line);
	}
	
	public void ClickParse(String line) throws ParseException {
		this.fill(line);
		
		if (this.status && 8 < atom.length) {
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
			if (10 < atom.length) {
				user_agent = atom[9];
				user_id = atom[10];
			} else {
				user_agent = "";
				user_id = "";
			}
		}
		
		if(this.isdug)
			System.out.println("BaseLog::ClickParse\tres:\n" + this.toString() + "\ndata=" + line);
	}

	public void OrderParse(String line) throws ParseException {
		this.fill(line);
		
		if(this.status && 11 < atom.length) {
			order_id = atom[0];
			plat_user_id = atom[1];
			order_no = atom[2];
			prod_no = atom[3];
			prod_name = atom[4];
			prod_price = atom[5];
			prod_cnt = atom[6];
			orig_prod_type = atom[7];
			prod_type_name = atom[8];
			order_price = atom[9];
			camp_id = atom[10];
//			oper_time = atom[11];
			time = atom[11];
			if(13 < atom.length) {
				domain = atom[12];
				user_id = atom[13];
			} else {
				domain = "";
				user_id = "";
			}
		}
		
		if(this.isdug)
			System.out.println("BaseLog::OrderParse\tres:\n" + this.toString() + "\ndata=" + line);	}
	
	public void ChuPVParse(String line) throws ParseException {
		this.fill(line);
		
		if(this.status && 9 < atom.length) {
			pv_id = atom[0];
			user_cookieid = atom[1];
			plat_user_id = user_cookieid;  // 统一用户ID的字段名称
			page_url = atom[2];
			refer_url = atom[3];
			user_agent = atom[4];  // name: ua
			ip = atom[5];
			time = atom[6];
			sw_id = atom[7];
			mate_ids = atom[8];
			camp_ids = atom[9];  // name: camp_ids
		}
		if(this.isdug)
			System.out.println("BaseLog::ChuPVParse\tres:\n" + this.toString() + "\ndata=" + line);
	}
	
	public void ChuClickParse(String line) throws ParseException {
		this.fill(line);
		
		if(this.status && 10 < atom.length) {
			click_id = atom[0];
			pv_id = atom[1];
			user_cookieid = atom[2];
			plat_user_id = user_cookieid;  
			page_url = atom[3];
			refer_url = atom[4];
			user_agent = atom[5];  // name: ua
			ip = atom[6];
			time = atom[7];
			sw_id = atom[8];
			mate_ids = atom[9];
			camp_ids = atom[10];  // name: camp_ids
		}
		if(this.isdug)
			System.out.println("BaseLog::ChuParse\tres:\n" + this.toString() + "\ndata=" + line);
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
	}

}
