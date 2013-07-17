package com.emar.recsys.user.media;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.emar.recsys.user.feature.FeatureType;
import com.emar.recsys.user.util.UtilStr;

/**
 * 解析 URL 信息：http://tech.xinmin.cn/3c/2010/11/08/7568391_8.html
 * 
 * @author zhoulm
 * 
 */
public class UrlExtract {

	private static final String urlsep = "/", dot = ".", comcn = ".com.cn";
	private static final String regstr = "[a-zA-Z]*://(.*?)/(.*)";
	private static final Pattern pattern;
	public static final Pattern pattQuery;
	
	static {
		pattQuery = Pattern.compile("\\?\\w+=");
		pattern = Pattern.compile(regstr, Pattern.DOTALL);
	}
	
	private static void hasQuery(String url, List<String> flist) {
		if(url == null) {
			return ;
		}
		
	}

	public static void UrlFeature(String url, List<String> flist) {
		// TODO 讲所有F存储到 flist
		if (url == null || flist == null) {
			return;
		}

		String[] atom = new String[4];
		Matcher m = pattern.matcher(url);
		if (m == null || !m.find()) {
			System.out.println("[ERROR] UrlFeature() reg-extract-failed."
					+ "RE=" + regstr + "\turl=" + url);
			return;
		}

		for (int i = 1; i <= m.groupCount(); ++i) {
			atom[i] = m.group(i); // REG匹配的下标0为原始字符串
		}
		String suburl = atom[2];
		int idxLeaf = suburl.lastIndexOf(UrlExtract.urlsep);
		if (idxLeaf == -1) { // 无中间节点
			atom[3] = atom[2];
			atom[2] = null;
		} else if ((idxLeaf+1) == suburl.length()) { // 无叶子
			atom[3] = urlsep;
			atom[2] = atom[2].substring(0, atom[2].length()-1); // 不用分隔符
		} else {
			atom[3] = suburl.substring(idxLeaf + 1);
			atom[2] = suburl.substring(0, idxLeaf);
		}
		UrlExtract.DomainFeature(atom[1], flist);
		UrlExtract.URLStructFeature(atom[2], flist);
		UrlExtract.URLLeafFeature(atom[3], flist);

//		System.out
//				.println("[Info] UrlFeature() atoms:\t" + Arrays.asList(atom)
//						+ "\nurl:\t" + url);
	}

	private static void DomainFeature(String url, List<String> flist) {
		if (url == null) { // flist 已经被public接口监测过
			return;
		}
		String tmp;
		int itmp;

		// feature
		String hcharchi = "NOCHI", hchardig = "NODIG", hcharoth = "NOOTH";
		int[] charcnt = UtilStr.strCharCnt(url);
		int cntDot = UtilStr.SubStrCnt(url, dot);
		if (charcnt != null) {
			if(charcnt[0] > 0)
				hcharchi = "HASCHI";
			if(charcnt[2] > 0)
				hchardig = "HASDIG";
			if(charcnt[3] > 0 && cntDot < charcnt[3])
				hcharoth =  "HASOTH";
		}
		// 3个关键字信息
		String htail = "UNKHTAIL";
		String host = "UNKHOST";
		String hhead = "UNKHHEAD";
		String fcom = "UNCOMCN";
		String hdot = "NODOT";

		if (cntDot != 0) {
			hdot = "DOT"
					+ (cntDot > FeatureType.HOSTDOTMX ? FeatureType.HOSTDOTMX
							: cntDot);
			itmp = url.lastIndexOf(dot);
			htail = url.substring(itmp + 1);
			if (url.indexOf(comcn) != -1) {
				fcom = "COMCN";
				url = url.replace(comcn, "");
			} else {
				url = url.substring(0, itmp).toUpperCase();
			}
			itmp = url.lastIndexOf(dot); // 对剪除URL末尾后的字符继续查找
			host = (itmp == -1 ? url: url.substring(itmp+1)).toUpperCase();
			itmp = url.indexOf(dot);
			if (itmp != -1) {
				itmp = itmp < FeatureType.HOSTLEN ? itmp : FeatureType.HOSTLEN;
				hhead = url.substring(0, itmp).toUpperCase();
			}
		}

		flist.add(FeatureType.concat(FeatureType.MEDHOST, FeatureType.SEG,
				hcharchi));
		flist.add(FeatureType.concat(FeatureType.MEDHOST, FeatureType.SEG,
				hchardig));
		flist.add(FeatureType.concat(FeatureType.MEDHOST, FeatureType.SEG,
				hcharoth));
		flist.add(FeatureType
				.concat(FeatureType.MEDHOST, FeatureType.SEG, fcom));
		flist.add(FeatureType.concat(FeatureType.MEDHOST, FeatureType.SEG,
				FeatureType.KEYWORD, FeatureType.SEG, htail));
		flist.add(FeatureType.concat(FeatureType.MEDHOST, FeatureType.SEG, 
				FeatureType.KEYWORD, FeatureType.SEG, host));
		flist.add(FeatureType.concat(FeatureType.MEDHOST, FeatureType.SEG,
				FeatureType.KEYWORD, FeatureType.SEG, hhead));
		flist.add(FeatureType
				.concat(FeatureType.MEDHOST, FeatureType.SEG, hdot));
		return;
	}

	private static void URLStructFeature(String url, List<String> flist) {
		// TODO url与ref相似时，比较两者大小
		if (url == null) {
			return;
		}

		String hasdate = "NODATE";
		int[] charcnt = UtilStr.strCharCnt(url);
		if (charcnt != null && charcnt[2] >= 4) {
			int idxdate = url.indexOf("20");
			// 无日期前缀标示 or 出现在中间不满足日期模式
			if ((idxdate > 0 && ('0' <= url.charAt(idxdate - 1) && url
					.charAt(idxdate - 1) <= '9')) || idxdate == -1) { 
				hasdate = "NODATE";
			} else {
				hasdate = "HASDATE";
			}
		}

		String[] atom = new String[FeatureType.DOMAINCHILDLEN];
		String[] atmp;
		int itmp = 0;
		int cntsepa = UtilStr.SubStrCnt(url, urlsep);
		if (cntsepa != 0) {
			atmp = url.split(urlsep);
			for (String s : atmp) { // 仅考虑字符
				if (2 < s.length() && s.length() < 7 && s.matches("[a-zA-Z]+")) {
					atom[itmp++] = s.toUpperCase();
				} else if (itmp != 0 || itmp > atom.length) { // 非连续的串不再考虑后续字符
					break;
				}
			}
		}
		flist.add(FeatureType.concat(FeatureType.MEDURLMID, FeatureType.SEG,
				hasdate));
		for (int i=0; i < atom.length && i < FeatureType.MEDULRMID_SZ; ++i) {  
			flist.add(FeatureType.concat(FeatureType.MEDURLMID, FeatureType.SEG, 
					FeatureType.KEYWORD, FeatureType.SEG, atom[i]));
		}
		return;
	}

	private static void URLLeafFeature(String url, List<String> flist) {
		// TODO 抽取URL最底层的结构特征， 如 同一字符的数量， 单词个数， 数字的长度， 后缀的特征html /等
		if(url == null) {
			return ;
		}
		
		int idxdot = url.lastIndexOf(dot);
		String suffix = "EMPTY";
		if(idxdot != -1) {
			suffix = url.substring(idxdot+1).toUpperCase();
			url = url.substring(0, idxdot);
		}
		
		String hasindex = "UNINDEX";
		String idxlen = "ZERO";
		String stype = "MULTITYPE";
		int[] charcnt = UtilStr.strCharCnt(url);  // 裁剪后的URL
		if(charcnt != null && charcnt[3] <= 1) {
			// 仅有 数字 与 一种字符
			if( charcnt[2] != 0 && (charcnt[0] == 0 || charcnt[1] == 0)) {
				hasindex = "INDEX";
				int i = charcnt[2]/FeatureType.HOSTLEAFSTEP;
				idxlen = "INDEX" + (i < FeatureType.HOSTLEAFSTEPMX? i: FeatureType.HOSTLEAFSTEPMX);
			}
			if(charcnt[0] == 0 && charcnt[1] != 0 && charcnt[2] == 0) {
				stype = "ONETYPE";
			}
		}
		
		String islist = "NOTLIST";
		if(url == urlsep || url.toLowerCase().indexOf("list") != -1) {
			islist = "LIST";
		}
		
		Matcher m = pattQuery.matcher(url);
		if(m.find()) {
			flist.add(FeatureType.concat(FeatureType.MEDHOST, FeatureType.SEG, "HASQUERY"));
		}
		flist.add(FeatureType.concat(FeatureType.MEDURLLEAF, FeatureType.SEG,
				suffix));
		flist.add(FeatureType.concat(FeatureType.MEDURLLEAF, FeatureType.SEG,
				hasindex));
		flist.add(FeatureType.concat(FeatureType.MEDURLLEAF, FeatureType.SEG,
				idxlen));
		flist.add(FeatureType.concat(FeatureType.MEDURLLEAF, FeatureType.SEG,
				stype));
		flist.add(FeatureType.concat(FeatureType.MEDURLLEAF, FeatureType.SEG,
				islist));
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String[] input = new String[] { 
				"http://p.yigao.com/imprImg.jsp",
				"https://s.x.cn.miaozhen.com/ad/banner_pid_106",
				"http://www.shanghaidz.com/dazhe/gouwu/shangchangdazhe/",
				"http://img.55bbs.com/55bbs/201305/iframe_auto_7378.html" };

		final String regstr = "([a-zA-Z]*)://(.*?)/(.*)";
		// Pattern.compile(regstr, Pattern.DOTALL);
		final Pattern pattern = Pattern.compile(UrlExtract.regstr, Pattern.DOTALL);

		ArrayList<String> list = new ArrayList<String>();
		for (int i = 0; i < input.length; ++i) {
//			UrlExtract.UrlFeature(input[i], list);
//			System.out.println("[Info]\t" + list);
//			list.clear();
			/*
			Matcher matcher = pattern.matcher(input[i]);
			Matcher m2 = UrlExtract.pattern.matcher(input[i]);
			while (matcher != null && matcher.find()) {
				int a = matcher.groupCount();
				while ((a) >= 0) {
					System.out.print(" a=" + a + "," + matcher.group(a--));
//					System.out.print(m2.group(a));
				}
//				System.out.println();
//				System.out.println("\n[Info] " + m2.groupCount() + m2.find() + m2.group(1));
			}
			*/
			
		}
	}

}
