package com.emar.recsys.user.feature;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.Set;

import com.emar.recsys.user.ad.ADFeature;
import com.emar.recsys.user.media.MediaFeature;
import com.emar.recsys.user.util.DateParse;
import com.emar.recsys.user.util.IUserException;
import com.emar.recsys.user.util.UAParse;
import com.emar.recsys.user.util.UrlSlice;
import com.emar.util.Ip2AreaUDF;

/**
 * 一次展示请求的日志信息生成
 * @author zhoulm
 *
 */
public class FeatureDriver {
	private static ADFeature fad;
	private static final String SEPA = ",";
	private static final Random random = new Random();
	public static int idxUrl = -1, idxUA = -1,  idxDate = -1, idxIP = -1, idxAd = -1;
	
	private FeatureDriver() {};
	private static FeatureDriver instance;
	public static FeatureDriver getInstance() {
		if(instance == null) {
			instance = new FeatureDriver();
		}
		return instance;
	}
	
	public static String evaluateList(String[] line, int N, Set<String> clickMaters) throws ParseException {
		List<String> res = evaluate(line, N, clickMaters);
		if(res == null) {
			return null;
		}
		String fstr = res.toString();
		return fstr.substring(1, fstr.length()-1).trim();
	}
	
	/**
	 * ���һ����־����������
	 * @throws ParseException 
	 */
	public static List<String> evaluate(String[] atom, int N, Set<String> clickMaters) throws ParseException {
		if(atom == null || N < 0) {
			return null;
		}
//		String[] atom = line.split(sepa);
		if(atom.length < N) {
			return null;
		}
		if(idxUrl < 0 || N < idxUrl) {
			throw new ParseException("unset field index.", IUserException.ErrUserIndex);
		}
		List<String> flist = new ArrayList<String>(100);
		List<String> tlist = new ArrayList<String>();
		int fsize = flist.size();
		int[] dtime = BasicFeature.DateTimeFeature(atom[idxDate], flist);
		if(fsize == flist.size()) {
			System.out.println("[Info] FeatureDriver::evaluate emtpy DateTimeFeature.");
			return null;
		}
		
		fsize = flist.size();
		String[] ip = BasicFeature.IPFeature(atom[idxIP], flist);
		if(fsize == flist.size()) {
			System.out.println("[Info] FeatureDriver::evaluate emtpy IPFeature.");
			return null;
		}
		
		fsize = flist.size();
		String[] system = BasicFeature.SystemFeature(atom[idxUA], flist);
		if(fsize == flist.size()) {
			System.out.println("[Info] FeatureDriver::evaluate emtpy SystemFeature.");
			return null;
		}
		
		fsize = flist.size();
		String murl = MediaFeature.extractMediaFeature(atom[idxUrl], flist);
		if(fsize == flist.size()) {
			System.out.println("[Info] FeatureDriver::evaluate emtpy MediaFeature.");
			return null;
		}
		
		// AD
		fsize = flist.size();
		String[] adset = atom[idxAd].split(SEPA);  // maybe also idxCamp
		String ad = null;
		if(clickMaters != null) {  // 优先选择有点击的materid
			for(String ai: adset) {
				if(clickMaters.contains(ai)) {
					ad = ai;
					break;
				}
			}
		}
		if(ad == null) {
			ad = adset[random.nextInt(adset.length)];  // 按出现频率抽样
		}
		try {
			fad = new ADFeature(ad, flist);
			
		} catch (ParseException e) {
			e.printStackTrace();
			return null; // fail to extract AD feature.
		}
		tlist.add(system[2]);
		tlist.add(system[3]);
		fad.CombADDevice(tlist);
		tlist.clear();
//		fad.CombADPos();
//		fad.CombADSPONSORPos();
		for(int di: dtime) {
			tlist.add(di + "");
		}
		fad.CombADTime(tlist);
		tlist.clear();
		
		String host = "yigao", level = "DEEP1", type = "NOLIST";
		String[] turls;
		try {
			if(murl.endsWith("/") || murl.contains("list")) {
				type = "LIST";
			}
			UrlSlice urls;
			try{
				urls = new UrlSlice(murl);
			} catch (ParseException e) {
				int lpos = murl.indexOf("&l=");
				if(lpos != -1) {
					int epos = murl.indexOf('&', lpos);
					epos = epos == -1? murl.length(): epos;
					urls = new UrlSlice(murl.substring(lpos, epos));
				} else {
					System.out.println(e.getMessage());
					return null;
				}
			}
			host = urls.getHost();
			turls = murl.split(host);
			if(turls.length != 1) {
				turls = turls[1].split("/");
				level = "DEEP" + turls.length;
			}
			fad.CombADUrl(host, level, type);
		} catch (ParseException e) {
		}
		
//		tlist = (List<String>) Arrays.asList(ip);
		for(String s: ip) {
			tlist.add(s);
		}
		fad.CombADZone(tlist);
		tlist.clear();
		if(fsize == flist.size()) {
			System.out.println("[Info] FeatureDriver::evaluate emtpy ADFeature.");
			return null;
		}
		return flist;
	}
	

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
