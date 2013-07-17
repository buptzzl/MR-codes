package com.emar.util;

import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import com.emar.classify.GoodsMark;
import com.ibm.icu.util.Calendar;


public class Test {

	/**
	 * ���﷨����
	 * @param args
	 * @throws ParseException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws ParseException, IOException {
		
		HashMap<String, Integer> smap = new HashMap<String, Integer>();
		smap.put("aa", 1);
		smap.put("bb", 2);
		List<Entry<String, Integer>> sinfo = 
				new ArrayList<Entry<String, Integer>>(smap.entrySet());
		String s = "5\u00016\u00017";
		System.out.print(GoodsMark.getInstance().ClassifyGoods("葡萄酒100ml")
				+ "\narr:\n" + sinfo
				+ "\n" + smap.entrySet());
		
		String s1 = "add69c2f2fd282210bbbeaf076d2b8d9\u0001\u0001112.236.22.25\u00016bdd89e9-14c3-31ea-8e8f-658929b69911\u000120130522210018\u000182510\u0001115736\u00011\u0001141985\u0001";
		
		String urls = "http://fanxian.egou.com/oauthcallback.do?from=qq&bind=1&source=http%3A%2F%2Fwww.egou.com%2Fghs%2Fshop4529292.htm%3Fchn%3Dgdt%26tag%3Dghs%26utm_source%3Dgdt%26etc_n%3Ddirectad%26etc_m%3Dgdt%26etc_c%3Dkyp%26etc_g%3Dkyp%26etc_t%3Dqianxi%26tanceng%3D1%26qz_gdt%3Dc96WvF8aQioKViSxLas_gOI0vGv1Ns6T4cZWH6sw26kNu5p3jItqSKec_I0lhFxPkcPuUbG50aU&code=926191221712AC8B33DF7D8E53B7B75D";
		String durl = URLDecoder.decode(urls, "utf8");
		String tmptest = "shangchangdazhe/";
		
		Calendar c = Calendar.getInstance();
		System.out.println("\n[test]"
				+ "\n" + (4096>>12) + "\t" + (8192>>14)
				+ "\n" + durl.contains("fanxian")
				+ "\n" + null
				+ "\n" + smap.toString()
				+ "\n--\\a--"
				+ "5\t\t".split("\t").length
				+ "\t" + s.split("\u0001").length
				);
		
		URL aURL = new URL("http://www.doubn.com/group/echofans"); 

	}

}
