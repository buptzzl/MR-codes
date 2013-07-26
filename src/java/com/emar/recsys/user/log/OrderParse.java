package com.emar.recsys.user.log;

import java.text.ParseException;

public class OrderParse extends BaseLog {
	static {
		BaseLog.N = 12; 
	}
	/*
	public String order_id;
	public String plat_user_id;
	public String order_no;
	public String prod_no;
	public String prod_name;
	public String prod_price;
	public String prod_cnt;
	public String orig_prod_type;
	public String prod_type_name;
	public String order_price;
	public String camp_id;
	public String oper_time;
	public String domain;
	public String user_id;
	*/
	public OrderParse(String line) throws ParseException {
		super(line);
		if(this.status) {
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
			oper_time = atom[11];
			if(13 < atom.length) {
				domain = atom[12];
				user_id = atom[13];
			} else {
				domain = "";
				user_id = "";
			}
		}
	}
	
	public String toString() {
		if(this.status) {
			return String.format("order_id=%s\u0001plat_user_id=%s\u0001order_no=%s"
					+"\u0001prod_no=%s\u0001prod_name=%s\u0001prod_price=%s\u0001prod_cnt=%s"
					+"\u0001orig_prod_type=%s\u0001prod_type_name=%s\u0001order_price=%s"
					+"\u0001camp_id=%s\u0001oper_time=%s\u0001domain=%s\u0001user_id=%s",
					order_id,plat_user_id,order_no,prod_no,prod_name,prod_price,prod_cnt,
					orig_prod_type,prod_type_name,order_price,camp_id,oper_time,domain,user_id);
		} else {
			return null;
		}
	}
	
	/**
	 * @param args
	 * @throws ParseException 
	 */
	public static void main(String[] args)  {
		String s = "9aa76699-d8ca-445c-8a26-2ddccacd438c\u000113522223056255194795\u0001211279248-1\u0001588562\u0001\u000188.0\u00011\u0001O\u0001O\u000188.0\u00015402\u000120130630040157\u0001meituan.com\u000113522223056255194795";
		
		OrderParse ord;
		try {
			ord = new OrderParse(s);
			System.out.println(ord);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
