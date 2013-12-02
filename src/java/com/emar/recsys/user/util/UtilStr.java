package com.emar.recsys.user.util;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.*;

import org.junit.Assert;

public class UtilStr {
	
	public static void testdecodeURL() { 
		String[][] t_urls = new String[][] { 
				{"aaa.aspx?tag=.net%bc%bc%ca%f5", "aaa.aspx?tag=.net技术"},
				{"aaa.aspx?tag=.net%e6%8a%80%e6%9c%af", ""},
				{"aaa.aspx?tag=.net%bc%bc%ca%f5&.net%bc%bc%ca%f5", ""},
				{"%D6%D0%CE%C4%B9%FA%BC%CA", "中文国际"}, // gbk
				{"%E4%B8%AD%E6%96%87%E5%9B%BD%E9%99%85", ""}, // utf8
				{"http://taobao.egou.com/product/k.html?f=head&sh=http%253A%252F%252Fju.taobao.com%252Fthttp%253A%252F%252Fdetail.tmall.com%252Fitem.htm%25253Fid%253D18579235085%252526%252526tracelog%253Djubuyitnow%252526_u%253D9nr9r7b064%252526spm%253D608.1000566.12315.1024g%252Fhome.htm%25253Fspm%253D608.6654957.11.d5.wfqgiA%252526item_id%253D18579235085%252526id%253D10000000596587%252526act_sign_id%253D237502", ""},
		};
		for (int i = 0; i < t_urls.length; ++i) 
			System.out.println(decodeURL(t_urls[i][0]));
	}
	
	/** 解码URL 尝试[utf8, gbk]两者解码方式。  */
	public static String decodeURL(final String myurl) {
		if (myurl == null)
			return null;
		final String UTF8 = "UTF-8", GBK = "GBK";
		String tmp = null, res = myurl, url = myurl.toLowerCase();
		int i = 0;
		try {
			res = URLDecoder.decode(url, UTF8); // UTF8优先
			for (i = 0; i < res.length(); ++i) {
				if (res.charAt(i) != url.charAt(i))
					break;
			}
			tmp = URLEncoder.encode(res.charAt(i)+"", UTF8).toLowerCase();//编码1个字
			if (!url.substring(i).startsWith(tmp)) {
				throw new UnsupportedEncodingException();// 激发子代码
			}
		} catch (UnsupportedEncodingException e) {
			try {
				res = URLDecoder.decode(url, GBK);
				for (i = 0; i < res.length(); ++i) {
					if (res.charAt(i) != url.charAt(i))
						break;
				}
				tmp = URLEncoder.encode(res.charAt(i)+"", GBK).toLowerCase(); 
			} catch (UnsupportedEncodingException e1) {
			}
		}
		if (tmp != null && !url.substring(i).startsWith(tmp)) 
			return myurl;
		return res;
	}

	/**
	 * 将字符串切成1gram的子串； 英文or数字串不分开
	 * 
	 * @param srep
	 *            需要被新字符串替代的映射关系
	 * @return
	 */
	public static List WordAtom(String s, final Set<String> filter,
			final Map<String, String> srep) {
		List words = new ArrayList<String>();
		if (s == null)
			return words;

		int begUnchi = 0;
		boolean unchi = false;
		char ctmp;
		String subtmp;
		// List<String> words = new ArrayList<String>(s.length());
		for (int i = 0; i < s.length(); ++i) {
			ctmp = s.charAt(i);
			if (('A' <= ctmp && ctmp <= 'Z') || ('a' <= ctmp && ctmp <= 'z')
					|| ('0' <= ctmp && ctmp <= '9')) {
				if (!unchi) { // first char in SEPA-pattern.
					begUnchi = i;
				}
				unchi = true;
				continue; // / 连续的英文 数字串当做一个字
			}
			if (unchi) {
				unchi = false;
				subtmp = s.substring(begUnchi, i);
				if (filter == null || !filter.contains(subtmp)) { // 不被过滤
					if (srep == null || !srep.containsKey(subtmp))
						words.add(subtmp);
					else
						words.add(srep.get(subtmp));
				}
			}
			subtmp = String.format("%c", ctmp);
			if (filter == null || !filter.contains(subtmp)) { // 不被过滤
				if (srep == null || !srep.containsKey(subtmp))
					words.add(subtmp);
				else
					words.add(srep.get(subtmp));
			}
		}

		return words;
	}

	/**
	 * 将字符串生成 从1到X的gram子串； 英文or数字串不分开
	 * 
	 * @param Mxgram
	 * @param filter
	 *            去掉的字符
	 * @return List<List>
	 */
	public static List<List> Xgram(String s, int Mxgram,
			final Set<String> filter) {
		List<List> res = new ArrayList<List>(0);
		if (s == null || Mxgram < 1)
			return res;

		List<String> PlaceHolder = new ArrayList<String>();
		for (int i = 0; i < Mxgram; ++i) 
			PlaceHolder.add("");
		for (int i = 0; i < Mxgram; ++i)
			res.add(new ArrayList<String>());
		int begUnchi = 0;
		// 将串 切分为字
		boolean unchi = false;
		char ctmp;
		String subtmp;
		List<String> words = new ArrayList<String>(s.length());
		for (int i = 0; i < s.length(); ++i) {
			ctmp = s.charAt(i);
			if (('A' <= ctmp && ctmp <= 'Z') || ('a' <= ctmp && ctmp <= 'z')
					|| ('0' <= ctmp && ctmp <= '9')) {
				if (!unchi) { // first char in SEPA-pattern.
					begUnchi = i;
				}
				unchi = true;
				continue; // / 连续的英文 数字串当做一个字
			}
			if (unchi) {
				unchi = false;
				subtmp = s.substring(begUnchi, i);
				if (filter == null) // 不被过滤
					words.add(subtmp);
				else if (!filter.contains(subtmp)) 
					words.add(subtmp);
				else 
					words.addAll(PlaceHolder);
			}
			subtmp = String.format("%c", ctmp);
			if (filter == null) // 不被过滤
				words.add(subtmp);
			else if (!filter.contains(subtmp)) 
				words.add(subtmp);
			else 
				words.addAll(PlaceHolder);
			
		}

		return Xgram(words, Mxgram);
	}

	public static List<List> Xgram(List<String> words, int Mxgram) {
		// 拼接字 为 X-gram, 低阶gram 位于List的后端
		List<List> res = new ArrayList<List>(0);
		if (words == null || words.size() == 0 || Mxgram < 1)
			return res;

		for (int i = 0; i < Mxgram; ++i)
			res.add(new ArrayList<String>());

		int[] spos = new int[Mxgram];
		for (int i = 0; i < Mxgram; ++i)
			spos[i] = (i - Mxgram + 1);
		int pbeg = -1;
		StringBuffer sbuf = new StringBuffer();
		for (int i = 0; i < words.size(); ++i) {
			for (int j = 0; j < spos.length; ++j) {
				pbeg = spos[j] + i;
				if (0 <= pbeg) {
					for (int k = pbeg; k <= i; ++k)
						sbuf.append(words.get(k));
					res.get(j).add(sbuf.toString());
					sbuf.delete(0, sbuf.length());
				}
			}
		}

		return res;
	}

	/**
	 * 统计字符串中 中文、英文、数字、其他字符 的个数
	 */
	public static int[] strCharCnt(String str) {
		int[] res = new int[] { 0, 0, 0, 0 };
		if (str == null) {
			return res;
		}
		
		char ctmp;
		for (int i = 0; i < str.length(); ++i) {
			ctmp = str.charAt(i);
			if (('A' <= ctmp && ctmp <= 'Z') || ('a' <= ctmp && ctmp <= 'z')) {
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
		// GENERAL_PUNCTUATION 判断中文的“号
		// CJK_SYMBOLS_AND_PUNCTUATION 判断中文的。号
		// HALFWIDTH_AND_FULLWIDTH_FORMS 判断中文的，号
		if (ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS
				|| ub == Character.UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS
				|| ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A
				|| ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_B
				|| ub == Character.UnicodeBlock.CJK_SYMBOLS_AND_PUNCTUATION
				|| ub == Character.UnicodeBlock.HALFWIDTH_AND_FULLWIDTH_FORMS
				|| ub == Character.UnicodeBlock.GENERAL_PUNCTUATION) {
			return true;
		}
		return false;
	}
	/** 全为数字 */
	public static boolean isDigital(String s) {
		if (s == null) {
			return false;
		}
		char c;
		for (int i = 0; i < s.length(); ++i) {
			c = s.charAt(i);
			if (c < '0' || '9' < c) {
				return false;
			}
		}
		return true;
	}
	/** 全为字符 */
	public static boolean isChars(String s) {
		if(s == null)
			return false;
		char c;
		for (int i = 0; i < s.length(); ++i) {
			c = s.charAt(i);
			if(!('a' <= c && c <= 'z') || !('A' <= c && c <= 'Z'))
				return false;
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
		if (str == null || (!str.startsWith("[") || !str.endsWith("]"))) {
			return null;
		}
		str = str.substring(1, str.length() - 1);
		return str.split(", "); // 使用默认的分割方式
	} // faster than str2arr(str, "[|]", ", ");

	/**
	 * @deprecated 通用的字符串转换为数组方法
	 * @param unuse
	 *            无效的先后缀，中间字符
	 * @param sepa
	 *            数组元素之间的分割符
	 */
	public static String[] str2arr(String str, String unuse, String sepa) {
		if (str == null) {
			return null;
		}
		String[] res;
		String mstr = str.replace(unuse, "");
		if (mstr.length() == 0) {
			res = new String[] { "" };
		} else {
			res = mstr.split(sepa);
		}
		return res;
	}
	
	// 将N级广义数组 转换为2级数组
	public static boolean listDeepTrim(List arr) {
		if(arr == null || arr.size() == 0) {
			return false;
		}
		int mx = 0;
		
		for(int i = 0; i < arr.size(); ++i) {
			if (arr.get(i) instanceof List) {
				List arr_s1 = (List) arr.get(i);
				for(int j = 0; j < arr_s1.size(); ++j) {
					if(!(arr_s1.get(j) instanceof String)) 
						((ArrayList)arr_s1).set(j, arr_s1.get(j).toString());
				}
			}
		}
		return true;
	}

	/**
	 * 嵌套解析多层数组为多层list， 解析 [[20130113152937, 50011980, 乳液, 卸洗凝胶, 102.4, ]]
	 * 注意： 保证每次的 arr 中的内容已经清空。
	 */
	public static int str2list(String str, String left, String right,
			String sepa, List arr) {
		if (str == null || !str.startsWith(left) || !str.endsWith(right)
				|| arr == null)
			return -1;

		int deep = 0, pdeep = -1;
		String tstr = str.substring(left.length(),
				str.length() - right.length());
		String stmp = null;
		String[] atmp = null;

		int ibeg = 0, iend = 0, idx = tstr.length(), pbeg = -1, isep = -1;
//		ibeg = tstr.indexOf(left);
		ibeg = getLBound(tstr, left, sepa, 0);
		// 最后一层的 数组
		if (ibeg == -1) {
			atmp = tstr.split(sepa);
			if (atmp.length != 1) {
				for (String si : atmp)
					arr.add(si);
			} else {
				arr.add(tstr);
			}

		} else {
			isep = tstr.indexOf(sepa);
			if(isep < ibeg) {  // 前端的非子数组元素
				atmp = tstr.substring(0, ibeg).split(sepa);
				for(int i = 0; i < atmp.length; ++i) 
					arr.add(atmp[i]);
			}

			// 递归处理中间的层次
			for (iend = getRBound(tstr, left, right, sepa, ibeg + 1), ++pdeep; ibeg != -1
					&& iend != -1; iend = getRBound(tstr, left, right, sepa, ibeg + 1)) {
				stmp = tstr.substring(ibeg, iend + right.length());
				List arrAtom = new ArrayList();
				arr.add(arrAtom);
				int tdeep = str2list(stmp, left, right, sepa, arrAtom); // 递归
				((ArrayList) arrAtom).trimToSize();
				pdeep += tdeep;
				if (deep < pdeep) { // 第一次迭代，更新深度
				// deep += tdeep;
					deep = pdeep; // 取最大深度
				}
				pdeep -= tdeep;

				idx = iend + 1; // 更新后缀串的起点
//				ibeg = tstr.indexOf(left, idx);
				ibeg = getLBound(tstr, left, sepa, idx);
				if (ibeg == -1) 
					break;
				// 中间的非数组元素
				if (ibeg != -1 && (ibeg - idx) != sepa.length()) {
					String[] esub = tstr.substring(idx, ibeg).split(sepa);
					for (String s : esub) {
						arr.add(s);
					}
				} 
			}
		}
		// 末尾的非数组元素
		if (idx != tstr.length()) {
			String[] esub = tstr.substring(idx + sepa.length()).split(sepa);
			for (String s : esub) {
				arr.add(s);
			}
		}
		deep += 1;
		return deep;
	}
	private static int getLBound(String in, String left, String sepa, int beg) {
		 // 左边界位置: 1 起点， 2  分隔符后的出现. 
		int ibeg = in.indexOf(left, beg);  // <0 || ==0 时直接返回
		while(ibeg > 0 && in.lastIndexOf(sepa, ibeg) != (ibeg - sepa.length())) {
			ibeg = in.indexOf(left, ibeg + 1);  // 无效，继续向后找
		}
		
		return ibeg; 
	}
	// 查找最外层符号配对时的 右边界位置。 无法配对时返回-1.
	private static int getRBound(String in, String left, String right, String sepa, int beg) {
		int N = 1, Nleft = 0;
		int ibeg = 0, iend = 0, sbeg = 0;
		
		iend = in.indexOf(right, beg);
		while(N != 0) {
			while(iend != -1 && 
					(iend + right.length() != in.length() && in.indexOf(sepa, iend + 1) != (iend + right.length()))) {
				beg = iend + 1;
				iend = in.indexOf(right, beg);
			}
			if(iend == -1)  // 无匹配符号 
				break;
			Nleft = UtilStr.SubStrCnt(in.substring(sbeg, iend), sepa + left);  // 特殊的前缀条件
			sbeg = iend;
			N = N + Nleft - 1;
		}
		return iend;
		/*
		// 右边界位置： 1 终点， 2 分隔符前出现 且 中间的左右边界符已经匹配 
		int ibeg = 0, iend = 0, nbeg;
		ibeg = getLBound(in, left, sepa, beg);  // 下一个左边界
		iend = in.lastIndexOf(right);
		if (ibeg == -1 || iend == -1) {
			if (iend == (in.length() - right.length())
					|| in.indexOf(sepa, iend) == (iend + right.length())) 
				return iend;
			return -1;
		}
		ibeg = beg;
		int cnt_in = 1, cnt_mid = -1;
		while (cnt_in != 0) {
//			nbeg = in.indexOf(left, ibeg + 1); 
			nbeg = getLBound(in, left, sepa, ibeg + 1);
			// 在两个 前缀分割符之间，执行最长匹配
			iend = nbeg == -1 ? in.lastIndexOf(right, in.length()) : in.lastIndexOf(right, nbeg);
//			iend = in.indexOf(right, nbeg == -1 ? ibeg + 1);
			if (iend == -1 || iend == (in.length() - right.length()))
				return iend; // error or last-char.
			cnt_mid = Math.min(UtilStr.SubStrCnt(in.substring(ibeg + 1, iend), left), 
					UtilStr.SubStrCnt(in.substring(ibeg + 1, iend), right));
			if (cnt_mid == 0 && 
					(iend == (in.length() - right.length())
						|| in.indexOf(sepa, iend) == (iend + right.length()))) {
				--cnt_in;
			} else {
				cnt_in = cnt_mid; // 中间还有 mid 可左边界待匹配
				ibeg = iend;
			}
		}

		return iend;
		*/
	}

	public static void testStr2List() {
		String[] in = new String[] { 
//				"[[[a]], [[b]], [[c]]]", "[]", "[ab]",
//				"[[], [ab]]", "[[], [[ab], c], d, ]", "[[[a, b]], [[c]]]",
				"[[1625, 50010728, 26], [50018772, 50016729, 50012786], [50014597, 50016736], [], [], [], [], [], [], []]",
				"[[20130704173923, 50011397, 珠宝, 中粮工业苏格兰野生黄金蟹 [非真空] (袋装 400g), 42.0, womai.com ], [20130704173923, 50017087, 景点门票, 琨山水产 密云水库野生银鱼 (袋装 500g), 36.0, womai.com ]]",
				"[2013, 5001, zhubao, zhguo[ti-td, ]",
				"[2013, 5001, zhubao, [zhguoti-td] bcd], ]",
				"[20130628171747, 50011397, 珠宝/钻石/翡翠/黄金, 中粮工业苏格兰野生黄金蟹 [非真空] [袋装 [400g]], 42.0, womai.com ]"
				};
		for (int i = 0; i < in.length; ++i) {
			Integer deep = new Integer(0);
			List alist = new ArrayList();
			deep = str2list(in[i], "[", "]", ", ", alist);
			System.out.println("[info] deep=" + deep + "\tlist="
					+ alist.toString());
			listDeepTrim(alist);  // test Deep.
			System.out.println("[Info] listDeepTrim() " + alist);
		}
	}

	public static void testStr2arr() {
		String[] in = new String[] { "", "[]", "[a, b, c]" };
		String[][] out = new String[in.length][];
		out[0] = null;
		out[1] = new String[] { "" };
		out[2] = new String[] { "a", "b", "c" };
		Assert.assertArrayEquals(out[0], UtilStr.str2arr(in[0]));
		Assert.assertArrayEquals(out[1], UtilStr.str2arr(in[1]));
		Assert.assertArrayEquals(out[2], UtilStr.str2arr(in[2]));

		Assert.assertArrayEquals(out[0],
				UtilStr.str2arr(in[0], "\\[|\\]", ", "));
		Assert.assertArrayEquals(out[1],
				UtilStr.str2arr(in[1], "\\[|\\]", ", "));
		Assert.assertArrayEquals(out[2],
				UtilStr.str2arr(in[2], "\\[|\\]", ", "));
	}

	public static void testXgram() {
		String s = "49元	的海南妃子笑荔枝;盒装（1000克）！";
		int xgram = 3;
		Set<String> filter = new HashSet<String>(Arrays.asList(";", " ", "\t",
				"（", "）", "！"));
		List<List> res = Xgram(s, xgram, null);
		// Assert.assertArrayEquals(, res.get(0))
		Assert.assertEquals(
				"[49元\t, 元\t的, \t的海, 的海南, 海南妃, 南妃子, 妃子笑, 子笑荔, 笑荔枝, 荔枝;, 枝;盒, ;盒装, 盒装（, 装（1000, （1000克, 1000克）, 克）！]",
				res.get(0).toString());
		Assert.assertEquals(
				"[49元, 元\t, \t的, 的海, 海南, 南妃, 妃子, 子笑, 笑荔, 荔枝, 枝;, ;盒, 盒装, 装（, （1000, 1000克, 克）, ）！]",
				res.get(1).toString());
		Assert.assertEquals(
				"[49, 元, 	, 的, 海, 南, 妃, 子, 笑, 荔, 枝, ;, 盒, 装, （, 1000, 克, ）, ！]",
				res.get(2).toString());
		// System.out.println();
		res = Xgram(s, xgram, filter);
		Assert.assertEquals(
				"[49元的, 元的海, 的海南, 海南妃, 南妃子, 妃子笑, 子笑荔, 笑荔枝, 荔枝盒, 枝盒装, 盒装1000, 装1000克]",
				res.get(0).toString());
		Assert.assertEquals(
				"[49元, 元的, 的海, 海南, 南妃, 妃子, 子笑, 笑荔, 荔枝, 枝盒, 盒装, 装1000, 1000克]",
				res.get(1).toString());
		Assert.assertEquals("[49, 元, 的, 海, 南, 妃, 子, 笑, 荔, 枝, 盒, 装, 1000, 克]",
				res.get(2).toString());
	}

	public static void main(String[] args) {
		// testXgram();
		// UtilStr.testStr2arr();
		UtilStr.testdecodeURL();
	}

}

