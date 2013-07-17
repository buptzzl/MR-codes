package com.emar.recsys.user.util;

import junit.framework.Assert;
import nl.bitwalker.useragentutils.Browser;
import nl.bitwalker.useragentutils.Manufacturer;
import nl.bitwalker.useragentutils.OperatingSystem;
import nl.bitwalker.useragentutils.UserAgent;

public class UAParse {
	
	/**
	 * @desc  low-string of some main browse's UserAgent.
	 * @author Administrator
	 *
	 */
	public static enum Browse {
				MAXTHON("maxthon",1), QQ("qqbrowser",2), SOGOU("metasr", 3),
				GREEN("greenbrowser",4), SE360("360se",5), 
				FIREFOX("firefox",6),
				OPERA("opera",7), CHROME("chrome",8), SAFARI("safari",9),
				IE9("msie 9.0",10), IE8("msie 8.0",11), IE7("msie 7.0",12),
				IE6("msie 6.0",13), 
				OTHER("unknow",14);
		
		private String name;
		private int index;
		
		private Browse(String name, int index) {
			this.name = name;
			this.index = index;
		}
		
		public static String getName(int index) {
			for(Browse b : Browse.values()) {
				if(b.getIndex() == index) {
					return b.name;
				}
			}
			return null;
		}
		public String getName() {
			return name;
		}
		public void setName() {
			this.name = name;
		}
		public int getIndex() {
			return index;
		}
		public void setIndex() {
			this.index = index;
		}
		public String toString() {
			return this.index + "_" + this.name; 
		}
	};
	
	/*
	 * @notice ��֧�ֹ����������ʶ��
	 */
	public static String[] agentInfo(String ustr) {
		// REF: https://user-agent-utils.java.net/javadoc/
		if (ustr == null) {
			return null;
		}
		
		UserAgent ua = new UserAgent(ustr);
		String browser = UAParse.getBrowser(ustr);
		if(browser.equals(Browse.OTHER.getName())) {
			Browser buser = ua.getBrowser();
			browser = buser.toString().toLowerCase();
//			Manufacturer muser = buser.getManufacturer();
		}
		

		// String browse = buser.getName();

		// Version vuser = ua.getBrowserVersion();
		// String mvbrowse = vuser.getMajorVersion();

		OperatingSystem ouser = ua.getOperatingSystem();
		// String osName = ouser.getName();

		Manufacturer duser = ouser.getManufacturer();
		// String mOS = duser.getName();

		return new String[] { 
				browser, "",  // muser.toString().toLowerCase(),
				ouser.toString().toLowerCase(), ouser.getDeviceType().toString().toLowerCase(),
				duser.getName().toString().toLowerCase() 
				};
	}
	
	public static String getBrowser(String ustr) {
		if (ustr == null) {
			return null;
		}
		ustr = ustr.toLowerCase();
		for(Browse b : Browse.values()) {
			if(ustr.indexOf(b.getName()) != -1) {
				return b.getName();
			}
		}
		return Browse.OTHER.getName();
	}
	

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String ua = "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.56 Safari/537.17";
		System.out.println("[test]" + UAParse.getBrowser(ua));
		UAParse.testAgentInfo();
	}
	
	public static void testAgentInfo() {
		String ua = "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.56 Safari/537.17";
		String ua2 = "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; SE 2.X MetaSr 1.0; SE 2.X MetaSr 1.0; .NET CLR 2.0.50727; SE 2.X MetaSr 1.0)";
		String ua3 = "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0; Trident/4.0)";  // IE
		UserAgent usera = new UserAgent(ua3);
		 String ustr = usera.getBrowser() + "|" +
		 usera.getBrowser().getManufacturer()
		 + "==" + usera.getOperatingSystem() + "|" +
		 usera.getOperatingSystem().getManufacturer();
		 System.out.print("[test] " 
//				 + "\n" + 
				 + "\n" + ustr);
		 UAParse uainfo = new UAParse();
		Assert.assertEquals("chrome", uainfo.getBrowser(ua)); 
//				new String[] { "CHROME", "", "WINDOWS_7", "COMPUTER", "Microsoft Corporation" });

	}

}
