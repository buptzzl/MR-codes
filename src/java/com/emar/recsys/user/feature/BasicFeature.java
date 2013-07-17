package com.emar.recsys.user.feature;

import java.util.ArrayList;
import java.util.List;

import com.emar.recsys.user.util.DateParse;
import com.emar.recsys.user.util.UAParse;
import com.emar.util.Ip2AreaUDF;

/**
 * 日志的基础信息挖掘
 * 
 * @author zhoulm
 * 
 */
public class BasicFeature {

	public static String[] SystemFeature(String ua, List<String> flist) {
		if (ua == null || flist == null) {
			return null;
		}

		String[] uainfo = UAParse.agentInfo(ua);
		if (uainfo != null) {
			flist.add(FeatureType.concat(FeatureType.GEN, FeatureType.SEG,
					FeatureType.GBROWSE, FeatureType.SEG, uainfo[0].toUpperCase()));
			flist.add(FeatureType.concat(FeatureType.GEN, FeatureType.SEG,
					FeatureType.GOS, FeatureType.SEG, uainfo[2].toUpperCase()));
			flist.add(FeatureType.concat(FeatureType.GEN, FeatureType.SEG,
					FeatureType.GDEVICE, FeatureType.SEG, uainfo[3].toUpperCase()));
		}
		return uainfo;
	}

	public static int[] DateTimeFeature(String date, List<String> flist,
			String fmt) {
		if(date == null || flist == null) {
			return null;
		}
		int[] weekhour = DateParse.getWeekDayHour(date, fmt);
		if(weekhour != null) {
			flist.add(FeatureType.concat(FeatureType.GEN, FeatureType.SEG,
					FeatureType.GWEEK, FeatureType.SEG, weekhour[0]));
			flist.add(FeatureType.concat(FeatureType.GEN, FeatureType.SEG,
					FeatureType.GHOUR, FeatureType.SEG, weekhour[1]));
		}
		return weekhour;
	}

	public static int[] DateTimeFeature(String date, List<String> flist) {
		return DateTimeFeature(date, flist, "yyyyMMddHHmmss");
	}

	public static String[] IPFeature(String ip, List<String> flist) {
		if(ip == null || flist == null) {
			return null;
		}
		
		Ip2AreaUDF ip2area = Ip2AreaUDF.getInstance();
		if(ip2area == null) {
			System.out.println("[ERROR] ");
			System.exit(1);
		}
		String[] info = ip2area.evaluateMore(ip);
		for(int i = 0; i < info.length; ++i) {  // @Ref NE: i in [1, len-1]
			flist.add(FeatureType.concat(FeatureType.GEN, FeatureType.SEG,
					FeatureType.GIP, i+"", FeatureType.SEG, info[i]));
		}
		return info;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String[] test = new String[] {
				"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.56 Safari/537.17", 
				"20130616220000", "92.34.11.1"
		};
		List<String> list = new ArrayList<String>();
		BasicFeature.DateTimeFeature(test[1], list);
		System.out.println(list.toString());
		list.clear();
		BasicFeature.IPFeature(test[2], list);  // 不能本地加载
		System.out.println(list.toString());
		list.clear();
		BasicFeature.SystemFeature(test[0], list);
		System.out.println(list.toString());
	}

}
