package com.emar.recsys.user.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.junit.Assert;

/**
 * 对JAVA的基本对象（非str）进行相关操作
 * 
 * @author zhoulm
 * 
 */
public class UtilObj {
	
	// 将[b=-1, a=1, c=2], {a=1}等字符串转换为HashMap, value必须为Number
	public static Map<String, Float> Str2Map(String in, String unuse, String sepa) {
		if(in == null || in.length() < 3) {
			return null;
		}
		
		char c;
		Set<Character> cset = new HashSet<Character>();
		for(int i = 0; i < unuse.length(); ++i) {
			cset.add(unuse.charAt(i));
		}
		StringBuffer sbuf = new StringBuffer(in.length());
		for(int i = 0; i < in.length(); ++i) {
			if(!cset.contains(in.charAt(i))) 
				sbuf.append(in.charAt(i));
		}
		in = sbuf.toString();
		String[] atoms = in.split(sepa);
		
		Map<String, Float> res = new HashMap<String, Float>();
		int eidx;
		for(String s: atoms) {
			eidx = s.indexOf('=');
			res.put(s.substring(0, eidx), Float.valueOf(s.substring(eidx+1).trim()));
		}
		return res;
	}
	
	public static <T, CT extends Comparable> List<Entry<T, CT>> sortMap(Map<T, CT> min) {
		if(min == null) {
			return null;
		}
		List<Entry<T, CT>> res = new ArrayList<Entry<T, CT>>(min.entrySet());
		
		Collections.sort(res, new Comparator<Entry<T, CT>>() {
			public int compare(Entry<T, CT> e1, Entry<T, CT> e2) {
				return e1.getValue().compareTo(e2.getValue());
			}
		});
		
		return res;
	}
	
	// 对 Map 对象的
	public static List<Entry<String, Integer>> entrySort(
			Map<String, Integer> kvpair, final boolean desc) {
		List<Entry<String, Integer>> sinfo = new ArrayList<Entry<String, Integer>>(
				kvpair.entrySet());
		Collections.sort(sinfo, new Comparator<Entry<String, Integer>>() {
			public int compare(Entry<String, Integer> e1,
					Entry<String, Integer> e2) {
				int diff = e1.getValue() - e2.getValue();
				if (desc == true) {
					diff = diff * (-1);
				}
				return diff;
			}
		});
		return sinfo;
	}

	public static List<Entry<String, Float>> entrySortFloat(
			Map<String, Float> kvpair, final boolean desc) {
		List<Entry<String, Float>> sinfo = new ArrayList<Entry<String, Float>>(
				kvpair.entrySet());
		Collections.sort(sinfo, new Comparator<Entry<String, Float>>() {
			public int compare(Entry<String, Float> e1, Entry<String, Float> e2) {
				Float f = (e1.getValue() - e2.getValue());
				int diff = 0;
				if (0 < f) {
					diff = 1;
				} else if (f < 0) {
					diff = -1;
				} else {
					diff = 0;
				}
				if (desc == true) {
					diff = diff * (-1);
				}
				return diff;
			}
		});
		return sinfo;
	}

	/**
	 * @param args
	 */
	public static void testSortMap() {
		Map<String, Integer> tmap = new HashMap<String, Integer>();
		tmap.put("a", 1); tmap.put("b", -1); tmap.put("c", 2);
		List<Entry<String, Integer>> tres = UtilObj.sortMap(tmap);
		String res = tres.toString();
		
		Assert.assertEquals("[b=-1, a=1, c=2]", res);
	}
	
	public static void testStr2Map(){
		String s = "{aa=1.0, bb=2.0}";
		Map<String, Float> res = UtilObj.Str2Map(s, "[]{}", ", ");
		
		Assert.assertEquals(s, res.toString());
	}
	
	public static void main(String[] args) {
		UtilObj.testStr2Map();
		UtilObj.testSortMap();
		
	}

}
