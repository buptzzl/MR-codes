package com.emar.recsys.user.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Assert;


public class UtilStr {
	
	/**
	 *  统计字符串中 中文、英文、数字、其他字符 的个数
	 */
	public static int[] strCharCnt(String str) {
		if(str == null) {
			return null;
		}
		int[] res = new int[]{0, 0, 0, 0};
		char ctmp;
		for(int i = 0; i < str.length(); ++i) {
			ctmp = str.charAt(i);
			if(('A' <= ctmp && ctmp <= 'Z') || ('a' <= ctmp && ctmp <= 'z')) {
				res[1] += 1;
			} else if ('0' <= ctmp && ctmp <= '9') {
				res[2] += 1;
			} else if (isChinese(ctmp)) {
				res[0] += 1;
			} else {
				res[3] += 1;
			}
		}
		return res;
	}
	public static boolean isChinese(char ch) {
		Character.UnicodeBlock ub = Character.UnicodeBlock.of(ch);
        //  GENERAL_PUNCTUATION 判断中文的“号  
        //  CJK_SYMBOLS_AND_PUNCTUATION 判断中文的。号  
        //  HALFWIDTH_AND_FULLWIDTH_FORMS 判断中文的，号 
        if (ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS || ub == Character.UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS 
                || ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A || ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_B 
                || ub == Character.UnicodeBlock.CJK_SYMBOLS_AND_PUNCTUATION || ub == Character.UnicodeBlock.HALFWIDTH_AND_FULLWIDTH_FORMS 
                || ub == Character.UnicodeBlock.GENERAL_PUNCTUATION) {
            return true;
        }
        return false;
	}
	
	public static boolean isDigital(String s) {
		if(s == null) {
			return false;
		}
		char c;
		for(int i = 0; i < s.length(); ++i) {
			c = s.charAt(i);
			if(c < '0' || '9' < c) {
				return false;
			}
		}
		return true;
	}
	
	public static String objArr2str(Object[] arr) {
		String s = "";
		for (Object i : arr) {
			s = String.format("%s\u0001%s", s, i.toString());
		}
		return String.format("[%s]", s);
	}

	public static String iarr2str(int[] arr) {
		String s = "";
		for (int i : arr) {
			s = String.format("%s\u0001%d", s, i);
		}
		return String.format("[%s]", s);
	}

	// 统计字符串中子串的个数
	public static int SubStrCnt(String str, String sub) {
		if (str == null || sub == null) {
			return -1;
		}
		int cnt = 0;
		int lpos = -1;
		int sLen = sub.length();
		for (int i = 0; i < str.length();) {
			lpos = str.indexOf(sub, i);
			if (lpos != -1) {
				++cnt;
				i = lpos + sLen;
				lpos = -1;
			} else {
				break;
			}
		}
		return cnt;
	}
	public static void testSubStrCnt() {
		String s = "5\t\t", s1 = "5", s2 = "5\taa\ta\t";
		Assert.assertEquals(2, UtilStr.SubStrCnt(s, "\t"));
		Assert.assertEquals(0, UtilStr.SubStrCnt(s1, "\t"));
		Assert.assertEquals(3, UtilStr.SubStrCnt(s2, "\t"));
	}
	
	public static String[] str2arr(String str) {
		if(str == null || (!str.startsWith("[") || !str.endsWith("]"))) {
			return null;
		} 
		str = str.substring(1, str.length()-1);
		return str.split(", ");  // 使用默认的分割方式
	}

	public static void testStr2arr() {
		String[] in = new String[]{"", "[]", "[a, b, c]"};
		String[][] out = new String[in.length][];
		out[0] = null; 
		out[1] = new String[]{""};
		out[2] = new String[]{"a", "b", "c"};
		Assert.assertArrayEquals(out[0], UtilStr.str2arr(in[0]));
		Assert.assertArrayEquals(out[1], UtilStr.str2arr(in[1]));
		Assert.assertArrayEquals(out[2], UtilStr.str2arr(in[2]));
	}
	
	public static void main(String[] args) {
		UtilStr.testStr2arr();
//		ObjectOper.testSubStrCnt();
		
	}


}
