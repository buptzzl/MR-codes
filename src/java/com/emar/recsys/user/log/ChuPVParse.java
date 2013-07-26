package com.emar.recsys.user.log;

import java.text.ParseException;

/**
 * 橱窗点击日志解析
 * @author zhoulm
 *
 */
public class ChuPVParse extends BaseLog {
	static{
		BaseLog.N = 10;
	}
	
	public ChuPVParse(String line) throws ParseException {
		super(line);
		
		if(this.status) {
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
	};
	
	public String toString() {
		if(this.status) {
			return String.format("pv_id=%s\u0001user_cookieid=%s\u0001page_url=%s"
					+ "\u0001refer_url=%s\u0001user_agent=%s\u0001ip=%s\u0001time=%s"
					+ "\u0001sw_id=%s\u0001mate_ids=%s\u0001camp_id=%s", pv_id,user_cookieid,
					page_url,refer_url,user_agent,ip,time,sw_id,mate_ids,camp_id);
		} else {
			return null;
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String s = "frW3xMaQoCqTzo23KKQzXEDmFJvEenO2\u0001UF0haKYQoCqTzo2QRQCQLtzB1wSIyp2Q\u0001http://p.yigao.com/imprImg.jsp?tc=&cc=&bgc=&bc=D9D9D9&tb=0&cb=0&tu=0&cu=0&uid=79856&zid=144003&pid=6&w=300&h=250&t=1&a=1&c=1&sid=0c6ebc8a07531b79&ua=Mozilla/4.0%20%28compatible%3B%20MSIE%207.0%3B%20Windows%20NT%205.1%3B%20Trident/4.0%3B%20QQDownload%20718%3B%20SV1%3B%20SE%202.X%20MetaSr%201.0%29&n=Microsoft%20Internet%20Explorer&p=http:&v=4.0%20%28compatible%3B%20MSIE%207.0%3B%20Windows%20NT%205.1%3B%20Trident/4.0%3B%20QQDownload%20718%3B%20SV1%3B%20SE%202.X%20MetaSr%201.0%29&r=http%3A//www.baike.com/newtop/tupian_r2.html&ho=undefined&l=http%3A//www.baike.com/newtop/tupian_r2.html&ax=0&ay=0&rx=0&ry=0&os=WinXP&scr=1280_768&ck=true&s=1&ww=300&wh=250&ym=miaozhen&fs=-1&pvid=6774b56fcc8d52f38d16e53d53700635&yhc=86552&msid=6e6d933538c0c07d\u0001http://showad.gouwuke.com/windowControler.do?flag=sw&oid=451661&gsid=657697&sn=ccbaike4.gouwuke.com&winid=10800&modelName=300-250Y&euid=4\u0001Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; QQDownload 718; SV1; SE 2.X MetaSr 1.0)\u000127.188.205.64\u000120130705000000\u0001undefined\u0001,,,,,,,,,,,,,,,,,,,\u0001,,,,,,,,,,,,,,,,,,,";
		
		try {
			ChuPVParse chup = new ChuPVParse(s);
			System.out.println(chup);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
	}

}
