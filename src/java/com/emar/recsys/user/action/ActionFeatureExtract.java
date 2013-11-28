package com.emar.recsys.user.action;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import com.emar.recsys.user.demo.IKeywords;
import com.emar.recsys.user.item.ItemAttribute;
import com.emar.recsys.user.util.UrlSlice;
import com.emar.recsys.user.util.WordSegment;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.webssky.jcseg.core.JcsegException;

/**
 * 定义过滤规则、 若干数据格式化规则的子类。方便主类进行调用。 抽取聚合用户的关键内容、目标行为（关键词） 作为候选训练集。
 * 
 * @author zhoulm
 * 
 * @FMT class,feature.
 */
public final class ActionFeatureExtract extends ActionExtract {
	private static Logger log = Logger.getLogger(ActionFeatureExtract.class);
	
	private static String DESC_SEPA = "@@@", SEPA_WORD = ", ", 
			KEY_PAGE_DESC = IKeywords.UserMergeAction[10], 
			KEY_PROD = IKeywords.UserMergeAction[6],
			KEY_PURL = IKeywords.UserMergeAction[7];
	/** URL的host与Query之间的空白字符数 */
	private static int URL_DIS = 3, N_ACT_MIN = 3;
	private ItemAttribute iattribute;

	private UrlSlice urlInfo;
	public Set<String> fastWhite, fastBlack;
	public Multimap<String, Integer> hitWhite, hitBlack;

	public ActionFeatureExtract() {
		super();
		this.initWords();
	}

	public ActionFeatureExtract(List<String> data) {
		super(data);
		this.initWords();
	}

	public ActionFeatureExtract(String[] args) throws FileNotFoundException {
		super(args);
		this.initWords();
	}
	/** 添加 关键词 到分词器  */
	public void initWords() {
		WordSegment ws = iattribute.ws;
		try {
			ws = WordSegment.getInstance();
		} catch (JcsegException e) {
		} catch (IOException e) {
		}
		for (int i = 0; i < WordsBlack.length; ++i)
			ws.add(WordsBlack[i]);
		for (int i = 0; i < WordsWhite.length; ++i)
			ws.add(WordsWhite[i]);
		fastWhite = new HashSet<String>();
		fastWhite.addAll(Arrays.asList(WordsWhite));
		fastBlack = new HashSet<String>();
		fastBlack.addAll(Arrays.asList(WordsBlack));
		hitWhite = ArrayListMultimap.create();
		hitBlack = ArrayListMultimap.create();
		log.info("build ActionFeatureExtract object. "
				+ "Add black words to dict: " + Arrays.asList(WordsBlack)
				+ ", white words to dict: " + Arrays.asList(WordsWhite));
	}

	@Override
	public boolean Filter(int index) {
		// 当前用户的行为是否完整 & 足够多
		if (this.userID == null) 
			this.parse(index);
		if (this.userID == null || this.userAction.length() < N_ACT_MIN) {
			return false;
		}
		return true;
	}
	
	@Override
	protected boolean parse(int index) {
		// 执行分词， 使用固定分隔符。 
		boolean succ = super.parse(index);
		if (!succ)
			return succ;
		JSONObject jObj;
		for (int i = 0; i < userAction.length(); ++i) {
			jObj = userAction.getJSONObject(i);
			this.prodSegment(KEY_PROD, jObj);
			this.prodSegment(KEY_PAGE_DESC, jObj);
		}
		return succ;
	}
	private boolean prodSegment(String key, JSONObject jObj) {
		List<String> words;
		List<String> pos;
		StringBuffer sbuf = new StringBuffer();
		if (jObj.has(key)) {
			try {
				iattribute = new ItemAttribute(jObj.getString(key));
			} catch (Exception e) {
				log.error("can't do WordSegment. [MSG]: " + e.getMessage());
				return false;
			}
			words = iattribute.getWord();
			pos = iattribute.getPos();
			sbuf.delete(0, sbuf.length());
			for (String s : words) 
				sbuf.append(s + SEPA_WORD);
			jObj.put(key, sbuf.toString());
		} 
		return true;
	}
	
	@Override
	public boolean format(int index) {
		boolean flag = super.format(index);
		if (flag && this.whiteFilter(index) && !this.blackFilter(index)) {
			this.data.set(index, formatUserActions(userID, userAction));
			this.flags.set(index);
		}

		return (true == flag);
	}
	
	@Override
	protected boolean whiteFilter(int index) {
		/// TODO 处理 白名单过滤后， 黑名单过滤时 一个命中与 全部命中等问题
		boolean find;
		JSONObject jObj;
		Multimap<String, Integer> fastFilter = ArrayListMultimap.create();
		Set<String> fastKeys;
		String[] atoms;
		this.hitBlack.clear();
		this.hitWhite.clear();
		for (int i = 0; i < userAction.length(); ++i) {
			jObj = userAction.getJSONObject(i);
			fastFilter.clear();
			// 行为有效性过滤
			if (jObj.has(KEY_PURL) && ActionFilterUtil.isBadUrl(jObj.getString(KEY_PURL)))
				continue;
			// 白名单关键词过滤 
			if (jObj.has(KEY_PROD)) {
				atoms = jObj.getString(KEY_PROD).split(SEPA_WORD);
				for (int j = 0; j < atoms.length; ++j) 
					fastFilter.put(atoms[i], i);
				fastKeys = fastFilter.keySet();
				fastKeys.retainAll(fastWhite);
				for (String s : fastKeys) 
					this.hitWhite.putAll(s, fastFilter.get(s));
			} 
			if (jObj.has(KEY_PAGE_DESC)) {
				atoms = jObj.getString(KEY_PAGE_DESC).split(DESC_SEPA);
				if (3 <= atoms.length) {
					fastFilter.clear();
					atoms = atoms[0].split(SEPA_WORD);
					for (int j = 0; j < atoms.length; ++j) 
						fastFilter.put(atoms[i], i);
					fastKeys = fastFilter.keySet();
					fastKeys.retainAll(fastWhite);
					for (String s : fastKeys) 
						this.hitWhite.putAll(s, fastFilter.get(s));
				}
			}
		}
		return true;
	}
	
	@Override
	protected boolean blackFilter(int index) {
		return false; // 全部通过
	}

	@Override
	public String formatUserActions(String uid, org.json.JSONArray action) {
		String fmt = super.formatUserActions(uid, action);
		fmt = fmt + "\t" + this.hitWhite;
		return fmt;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
