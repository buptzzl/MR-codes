package com.emar.recsys.user.util;

import java.util.*;

import org.junit.Assert;


public class UtilStr {
	
	/**
	 * 将字符串切成1gram的子串； 英文or数字串不分开
	 * @param srep 需要被新字符串替代的映射关系
	 * @return
	 */
	public static List WordAtom(String s, final Set<String> filter, 
			final Map<String, String> srep) {
		List words = new ArrayList<String>();
		if(s == null)
			return words;
		
		int begUnchi = 0;
		boolean unchi = false;
		char ctmp;
		String subtmp;
//		List<String> words = new ArrayList<String>(s.length());
		for(int i = 0; i < s.length(); ++i) {
			ctmp = s.charAt(i);
			if(('A' <= ctmp && ctmp <= 'Z') || ('a' <= ctmp && ctmp <= 'z')
					|| ('0' <= ctmp && ctmp <= '9')) {
				if(!unchi) {  // first char in SEPA-pattern.
					begUnchi = i;
				}
				unchi = true;
				continue;  /// 连续的英文 数字串当做一个字
			}
			if(unchi) {
				unchi = false;
				subtmp = s.substring(begUnchi, i);
				if(filter == null || !filter.contains(subtmp)) { // 不被过滤
					if(srep == null || !srep.containsKey(subtmp))
						words.add(subtmp);
					else 
						words.add(srep.get(subtmp));
				}
			}
			subtmp = String.format("%c", ctmp);
			if(filter == null || !filter.contains(subtmp)) { // 不被过滤
				if(srep == null || !srep.containsKey(subtmp))
					words.add(subtmp);
				else 
					words.add(srep.get(subtmp));
			}
		}
		
		return words;
	}
	
	/**
	 * 将字符串生成 从1到X的gram子串； 英文or数字串不分开
	 * @param Mxgram
	 * @param filter 去掉的字符
	 * @return List<List>
	 */
	public static List<List> Xgram(String s, int Mxgram, final Set<String> filter) {
		List<List> res = new ArrayList<List>(0);
		if (s == null || Mxgram < 1) 
			return res;
		
		for(int i = 0; i < Mxgram; ++i)
			res.add(new ArrayList<String>());
		int begUnchi = 0;
		// 将串 切分为字
		boolean unchi = false;
		char ctmp;
		String subtmp;
		List<String> words = new ArrayList<String>(s.length());
		for(int i = 0; i < s.length(); ++i) {
			ctmp = s.charAt(i);
			if(('A' <= ctmp && ctmp <= 'Z') || ('a' <= ctmp && ctmp <= 'z')
					|| ('0' <= ctmp && ctmp <= '9')) {
				if(!unchi) {  // first char in SEPA-pattern.
					begUnchi = i;
				}
				unchi = true;
				continue;  /// 连续的英文 数字串当做一个字
			}
			if(unchi) {
				unchi = false;
				subtmp = s.substring(begUnchi, i);
				if(filter == null || !filter.contains(subtmp))  // 不被过滤
					words.add(subtmp);
			}
			subtmp = String.format("%c", ctmp);
			if(filter == null || !filter.contains(subtmp))
				words.add(subtmp);
		}
		
		// 拼接字 为 X-garm
		int[] spos = new int[Mxgram];
		for (int i = 0; i < Mxgram; ++i)
			spos[i] = (i - Mxgram + 1);
		int pbeg = -1;
		StringBuffer sbuf = new StringBuffer();
		for(int i = 0; i < words.size(); ++i) {
			for(int j = 0; j < spos.length; ++j) {
				pbeg = spos[j] + i;
				if(0 <= pbeg) {
					for(int k = pbeg; k <= i; ++k)
						sbuf.append(words.get(k));
					res.get(j).add(sbuf.toString());
					sbuf.delete(0, sbuf.length());
				}
			}
		}
		
		return res;
	}
	
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
	}  // faster than str2arr(str, "[|]", ", ");
	/**
	 * 通用的字符串转换为数组方法
	 * @param unuse 无效的先后缀，中间字符
	 * @param sepa 数组元素之间的分割符
	 */
	public static String[] str2arr(String str, String unuse, String sepa) {
		if(str == null) {
			return null;
		}
		String[] res;
		
		String mstr = str.replace(unuse, "");
		if(mstr.length() == 0) {
			res = new String[]{""};
		} else {
			res = mstr.split(sepa);
		}
		return res;
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
		
		Assert.assertArrayEquals(out[0], UtilStr.str2arr(in[0], "\\[|\\]", ", "));
		Assert.assertArrayEquals(out[1], UtilStr.str2arr(in[1], "\\[|\\]", ", "));
		Assert.assertArrayEquals(out[2], UtilStr.str2arr(in[2], "\\[|\\]", ", "));
	}
	
	public static void testXgram() {
		String s = "49元	的海南妃子笑荔枝;盒装（1000克）！";
		int xgram = 3;
		Set<String> filter = new HashSet<String>(Arrays.asList(";"," ","\t",
				"（","）","！"));
		List<List> res = Xgram(s, xgram, null);
//		Assert.assertArrayEquals(, res.get(0))
		Assert.assertEquals("[49元\t, 元\t的, \t的海, 的海南, 海南妃, 南妃子, 妃子笑, 子笑荔, 笑荔枝, 荔枝;, 枝;盒, ;盒装, 盒装（, 装（1000, （1000克, 1000克）, 克）！]", res.get(0).toString());
		Assert.assertEquals("[49元, 元\t, \t的, 的海, 海南, 南妃, 妃子, 子笑, 笑荔, 荔枝, 枝;, ;盒, 盒装, 装（, （1000, 1000克, 克）, ）！]", res.get(1).toString());
		Assert.assertEquals("[49, 元, 	, 的, 海, 南, 妃, 子, 笑, 荔, 枝, ;, 盒, 装, （, 1000, 克, ）, ！]", res.get(2).toString());
//		System.out.println();
		res = Xgram(s, xgram, filter);
		Assert.assertEquals("[49元的, 元的海, 的海南, 海南妃, 南妃子, 妃子笑, 子笑荔, 笑荔枝, 荔枝盒, 枝盒装, 盒装1000, 装1000克]", res.get(0).toString());
		Assert.assertEquals("[49元, 元的, 的海, 海南, 南妃, 妃子, 子笑, 笑荔, 荔枝, 枝盒, 盒装, 装1000, 1000克]", res.get(1).toString());
		Assert.assertEquals("[49, 元, 的, 海, 南, 妃, 子, 笑, 荔, 枝, 盒, 装, 1000, 克]", res.get(2).toString());
	}
	
	public static void main(String[] args) {
		testXgram();
		
		UtilStr.testStr2arr();
//		ObjectOper.testSubStrCnt();
		
	}


}
