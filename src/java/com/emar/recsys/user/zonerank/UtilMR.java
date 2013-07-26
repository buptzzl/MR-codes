package com.emar.recsys.user.zonerank;

import java.util.HashSet;
import java.util.Set;

import com.emar.recsys.user.log.BaseLog;
import com.emar.recsys.user.log.LogParse;

/**
 * 当前包下 MR 的解析方法集合
 * 
 * @author zhoulm
 * 
 */
public class UtilMR {
	public static String SEPA = LogParse.SEPA, SEPA_MR = LogParse.SEPA_MR,
			SEPA_Arr = ", ";

	public static boolean mergeIPRank(Set<String> ip, Set<String> rank,
			Set<String> mout) {
		if (ip == null || ip.size() == 0 || rank == null || rank.size() == 0
				|| mout == null) {
			return false;
		}
		String[] atom;
		String rawip;
		for (String s : ip) {
			s = s.replace('[', ' ');
			s = s.replace(']', ' ');
			atom = s.split("\u0001|, ");
			if (atom.length < 6) {
				continue;
			}
			for (String ri : rank) 
				for (String ai : atom)
					mout.add(String.format("%s%s%s", ai.trim(), SEPA, ri.trim()));// ipinfo->rank
		}
		return true;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Set<String> ip = new HashSet<String>(), rank = new HashSet<String>(),
				mo = new HashSet<String>();
		ip.add("222.243.232.60\u0001[1029 , 4119 , 16662 , 65540 , 263927]");
		rank.add("[50023866=1.0]");
		boolean isok = UtilMR.mergeIPRank(ip, rank, mo);
		
		System.out.println("[Test] Util" 
				+ "\nmergeIPRank:\n" + mo.toString()
				+ "\nmergeStatus: " + isok
				);
	}

}
