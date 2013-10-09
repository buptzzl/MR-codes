package com.emar.recsys.user.log;

import java.text.ParseException;
import java.lang.reflect.Field;

import com.emar.recsys.user.util.IUserException;

/**
 * 日志解析基类
 * @author zhoulm
 * @Done UT.
 */
public class BaseLog {
	// 全部日志的全部字段的名字, 按字符顺序表示。 新增的字段须添加进来。
	public String ad_modality;
	public String ad_zone_id;
	public String age; // RTB 基本为空
	public String algo_id; // algorithm ID.
	public String camp_id;  // chuchuang中为camp_ids.
	public String camp_ids;
//	public String catid;
	public String click_id;
	public String coll_type; 
	public String domain;
	public String desc; // EmarBox-browse.
	public String idea_id;
	public String ip;
	public String keywords; 
	public String landing_url; // landing page url.
	public String mate_ids; 
//	public String oper_time;  // 与time合并
	public String order_id;
	public String order_no;
	public String order_price;
	public String orig_plat_type;
	public String orig_media; // RTB 媒体标注
	public String page_url;
	public String plat_user_id;
	public String prod_cnt;
	public String prod_name;
	public String prod_no;
	public String prod_price;
	public String prod_type_name;
	public String pv_id;
	public String refer_url;
	public String request_id; 
	public String rule_id;
//	public String seller_id; 
	public String sex;
	public String sw_id;
	public String time;
	public String time_stay; 
	public String title;
	public String user_agent;  // chuchuang中为ua
	public String user_cookieid; // 统一未CookMatch的用户ID的字段名称为plat_user_id
	public String user_id;
	
	public boolean status;
	public boolean isdebug;
	
	protected String[] atom;
	protected static int N = 0; // 基本字段长度
	
	public BaseLog(String line) throws ParseException {
		this.fill(line);  // 与C++不同
	}
	public BaseLog() {
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
		ad_modality = null; ad_zone_id = null; age = null; algo_id = null;
		camp_id = null; camp_ids = null; click_id = null; coll_type = null;
		desc = null; domain = null;
		idea_id = null; ip = null; 
		keywords = null; landing_url = null; mate_ids = null;
//		oper_time = null;
		order_id = null;  order_no = null; order_price = null; orig_media = null; 
		orig_plat_type = null; 
		page_url = null; plat_user_id = null; prod_cnt = null; prod_name = null;
		prod_no =null; prod_price = null; prod_type_name = null; pv_id = null;
		refer_url = null; request_id = null; rule_id = null;
		sex = null; sw_id = null;
		time = null; time_stay = null; title = null;
		user_agent = null; user_cookieid = null; user_id = null;
		
		status = false; //isdebug = isdebug; 
		atom = null;
	}
	
	public String toString() {
		return String.format("status=%s\t"
				+ "ad_modality=%s,ad_zone_id=%s,age=%s,algo_id=%s,"
				+ "camp_id=%s,camp_ids=%s,click_id=%s,coll_type=%s,domain=%s,desc=%s,"
				+ "idea_id=%s,ip=%s,keywords=%s,mate_ids=%s,landing_url=%s,order_id=%s,order_no=%s,order_price=%s,"
				+ "orig_prod_type=%s,orig_media=%s,page_url=%s,plat_user_id=%s,prod_cnt=%s,prod_name=%s,"
				+ "prod_no=%s,prod_price=%s,prod_type_name=%s,pv_id=%s,"
				+ "refer_url=%s,request_id=%s,rule_id=%s,sex=%s,sw_id=%s,"
				+ "time=%s,time_stay=%s,title=%s,user_agent=%s,user_cookieid=%s,user_id=%s",
				status, ad_modality,age,ad_zone_id,algo_id,
				camp_id,camp_ids,click_id,coll_type,domain,desc,
				idea_id,ip,keywords,mate_ids,landing_url,order_id,order_no,order_price,
				orig_plat_type,orig_media,
				page_url,plat_user_id,prod_cnt,prod_name,prod_no,prod_price,prod_type_name,pv_id,
				refer_url,request_id,rule_id,sex,sw_id,
				time,time_stay,title,user_agent,user_cookieid,user_id);
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
		
		if(this.isdebug)
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
		
		if(this.isdebug)
			System.out.println("BaseLog::ClickParse\tres:\n" + this.toString() + "\ndata=" + line);
	}

	public void OrderParse(String line) throws ParseException {
		this.fill(line);
		int i = 0;
		if(this.status && 11 < atom.length) {
			order_id = atom[0];
			plat_user_id = atom[1];
			order_no = atom[2];
			prod_no = atom[3];
			prod_name = atom[4];
			i = 5;
			prod_price = atom[i++];
			try {
				Float.parseFloat(atom[5]);
			} catch (Exception e) {
				if (atom[5].trim().length() != 0 && 12 < atom.length) {
					prod_name += atom[5];// 中文可能出现^A分隔符
					prod_price = atom[i++];// 取下1个字段
				}
			}
			prod_cnt = atom[i++];
			orig_plat_type = atom[i++];
			prod_type_name = atom[i++];
			order_price = atom[i++];
			try {
				Float.parseFloat(order_price);
			} catch (Exception e) {
				if (order_price.trim().length() != 0 && 13 < atom.length) {
					prod_type_name += order_price;
					order_price = atom[i++];
				}
			}
			camp_id = atom[i++];
//			oper_time = atom[11];
			time = atom[i++];
			if((i+1) < atom.length) {
				domain = atom[i++];
				user_id = atom[i++];
			} else {
				domain = "";
				user_id = "";
			}
		}
		
		if(this.isdebug)
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
		if(this.isdebug)
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
			if (11 < atom.length)
				landing_url = atom[11];
		}
		if(this.isdebug)
			System.out.println("BaseLog::ChuParse\tres:\n" + this.toString() + "\ndata=" + line);
	}
	
	public void EmarboxBrowse(String line) throws ParseException {
		this.fill(line);
		
		if (this.status && 11 < atom.length) {
			plat_user_id = atom[0];
			ip = atom[1];
			time = atom[2];
			page_url = atom[3];
			refer_url = atom[4];
			keywords = atom[5];
			title = atom[6];
			desc = atom[7];
			user_agent = atom[8];
			time_stay = atom[9];
			rule_id = atom[10];
			user_id = atom[11];
		}
		if (this.isdebug)
			System.out.println("BaseLog::EmarboxBrowse\tres:\n" + this.toString() + "\ndata=" + line);
	}
	
	public void EmarboxBProd(String line) throws ParseException {
		this.fill(line);
		
		if (this.status && 11 < atom.length) {
			plat_user_id = atom[0];
			ip = atom[1];
			time = atom[2];
			page_url = atom[3];
			refer_url = atom[4];
			prod_no = atom[5];
			prod_name = atom[6];
			prod_price = atom[7];
			orig_plat_type = atom[8];
			prod_type_name = atom[9];
			coll_type = atom[10];
			user_id = atom[11];
		}
		if (this.isdebug)
			System.out.println("BaseLog::EmarboxBProd\tres:\n" + this.toString() + "\ndata=" + line);
	
	}
	/** Rtb请求，无时间点。须根据日志的路径估计成整点时间 */
	public void BidRequest(String line) throws ParseException {
		this.fill(line);
		
		if (this.status && 9 < atom.length) {
			orig_plat_type = atom[0];
			request_id = atom[1];
			ip = atom[2];
			user_id = atom[3];
			plat_user_id = atom[4];
			page_url = atom[5];
			orig_media = atom[6];
			// website_id = atom[7];
			sex = atom[8];
			age = atom[9];
		}
		if (this.isdebug)
			System.out.println("BaseLog::RtbRequest\tres:\n" + this.toString() + "\ndata=" + line);

	}
	/** 解析搜索日志。 仅仅抽取搜索词，IP，time， 2个ID */
	public void Search(String line) throws ParseException {
		this.fill(line);
		
		if (this.status && 9 < atom.length) {
			plat_user_id = atom[0];
			ip = atom[1];
			time = atom[2];
			keywords = atom[3];
			// TODO 其他项一般为空。
			user_id = atom[9];
		}
		if (this.isdebug)
			System.out.println("BaseLog::RtbRequest\tres:\n" + this.toString() + "\ndata=" + line);
	}
	
	/**
	 * 基于反射动态获取对应的 字段的值
	 * @param fname
	 * @return
	 */
	public Object getField(String fname) {
		Object res = null;
		if(fname == null) {
			return res;
		}
		Field field;
		try {
			field = BaseLog.class.getField(fname);
			res = field.get(this);
		} catch (Exception e1) {}
		
		return res;
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
	}

}
