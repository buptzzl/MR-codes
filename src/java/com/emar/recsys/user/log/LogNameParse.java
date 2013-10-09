package com.emar.recsys.user.log;

/**
 * 解析日志文件的名称，抽取信息
 * 
 * @author zhoulm
 * @Done UT.
 */
public class LogNameParse {
	public String source;
//	public String plat;
//	public String type;
	/** 业务平台 */
	public LOG_PLAT plat;
	/** 日志的行为类型 */
	public LOG_TYPE type;
	public String date;
	public String hour;
	public String moreinfo;

	public boolean status;
	private String[] atom;
	
	public static enum LOG_TYPE {
		pv, click, order, 
		browse, event, goodsview, // emarbox
		search, // gouwuke 
		bidreq, bidsucc,
		BAD_LTYPE;
		
		public static LOG_TYPE setType(String type) {
			LOG_TYPE res = LOG_TYPE.BAD_LTYPE;
			
			for(LOG_TYPE ltype: LOG_TYPE.values()) {
				if(ltype.toString().equals(type)) {
					res = ltype;
					return res;
				}
			}
			
			return res;
		}
	};
	
	public static enum LOG_PLAT {
		yiqifa, gouwuke, yigao, emarbox, rtb,
		chuchuang, BAD_LPLAT;  // yiqifa-sub-plat
		
		public static LOG_PLAT setPlat(String plat) {
			LOG_PLAT res = LOG_PLAT.BAD_LPLAT;
			
			for(LOG_PLAT ltype: LOG_PLAT.values()) {
				if(ltype.toString().equals(plat)) {
					res = ltype;
					return res;
				}
			}
			
			return res;
		}
	};

	/**
	 * 按标准格式： i_yiqifa_order_20130522_00.dat 整理出属性
	 * 
	 * @param line
	 */
	public LogNameParse(String line) {
		this.fill(line);
	}

	public void fill(String line) {
		this.reset();
		if (line == null || line.trim().length() == 0) {
			status = false;
		} else {
			status = true;
			
			String tmp;
			int idxDot;
			if (line.indexOf('/') != -1) {
				line = line.substring(line.lastIndexOf('/') + 1);
			}
			atom = line.split("_");
			if (line.length() < 5) {
				status = false;
			} else {
				source = atom[0];
				atom[1] = atom[1].trim().toLowerCase();
				atom[2] = atom[2].trim().toLowerCase();
				if (line.contains("dsp_monitor") || line.contains("rtb_bidreq")) {
					// 单独解析RTB的特殊格式 i_rtb_bidreq_10_20130909.dat
					plat = LOG_PLAT.rtb;
					type = LOG_TYPE.bidreq;
					if (line.contains("dsp"))
						type = LOG_TYPE.bidsucc;
					tmp = atom[3];
					
					idxDot = atom[4].indexOf('.'); // 去掉.dat后缀
					atom[3] = atom[4];
					if (idxDot != -1)
						atom[3] = atom[4].substring(0, idxDot);
					
					atom[4] = tmp;
				} else {
					plat = LOG_PLAT.setPlat(atom[1]);
					type = LOG_TYPE.setType(atom[2]);
					idxDot = atom[4].indexOf('.'); // 去掉.dat后缀
					if (idxDot != -1)
						atom[4] = atom[4].substring(0, idxDot);
				}
				date = atom[3];
				hour = atom[4];
				if (hour.length() == 1) 
					hour = "0" + hour;
			}
		}
	}

	public void reset() {
		status = false;
		atom = null;
		source = null;
		plat = null;
		type = null;
		date = null;
		hour = null;
		moreinfo = null;
	}

	// 按收入格式输出
	public String toString() {
		if (this.status) {
			return String
					.format("%s_%s_%s_%s_%s.dat", source, plat, type, date, hour);
		} else {
			return null;
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String[] s = { "i_yigao_pv_20130504_22.dat",
				"/data/stg/s_ad_click_log/20130505/2/i_yigao_click_20130505_04.dat" };

		for (String si : s) {
			LogNameParse fpar = new LogNameParse(si);
			System.out.println(fpar);
		}
	}

}
