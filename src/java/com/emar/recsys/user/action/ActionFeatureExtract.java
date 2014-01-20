package com.emar.recsys.user.action;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
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
import com.emar.recsys.user.log.LogParse;
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
	
	private static String DESC_SEPA = LogParse.MAGIC, SEPA_WORD = ", ", 
			KEY_PAGE_DESC = IKeywords.UserMergeAction[10], 
			KEY_PROD = IKeywords.UserMergeAction[6],
			KEY_PURL = IKeywords.UserMergeAction[7];
	/** URL的host与Query之间的空白字符数 */
	private static int URL_DIS = 3, N_ACT_MIN = 3;
	/** 监测关键词是否在词典中可以为前缀，是则将所有该前缀的词添加到对应的 关键词列表*/
	public static boolean checkDictWords = true;
	private ItemAttribute iattribute;

	private UrlSlice urlInfo;
	public Set<String> fastWhite, fastBlack;
	public Multimap<String, Integer> hitWhite, hitBlack;

	public ActionFeatureExtract() throws JcsegException, IOException {
		super();
		this.initWords();
	}

	public ActionFeatureExtract(List<String> data) throws JcsegException, IOException {
		super(data);
		this.initWords();
	}
	
	public ActionFeatureExtract(ActionExtract obj) throws JcsegException, IOException {
		super(obj);
		this.initWords();
	}

	public ActionFeatureExtract(String[] args) throws JcsegException, IOException {
		super(args);
		this.initWords();
	}
//	public ActionFeatureExtract(boolean moreKeys) {
//		super();
//		checkDictWords = moreKeys;  // 临时 对当前对象可选[开启]词典的关键词拓展 @for test.
//		this.initWords();
//		checkDictWords = false;
//	}
	/** 添加 关键词 到分词器  */
	public void initWords() throws JcsegException, IOException {
		hitWhite = ArrayListMultimap.create();
		hitBlack = ArrayListMultimap.create();
		fastWhite = new HashSet<String>(Arrays.asList(WordsWhite));
		fastBlack = new HashSet<String>(Arrays.asList(WordsBlack));
		
		WordSegment ws = iattribute.ws;
		ws = WordSegment.getInstance();
		for (int i = 0; i < WordsBlack.length; ++i) 
			ws.add(WordsBlack[i]);
		for (int i = 0; i < WordsWhite.length; ++i)
			ws.add(WordsWhite[i]);
		// 拓展识别的 关键词
		if (checkDictWords) {
			List<String> moreWhite = new ArrayList<String>();
			List<String> moreBlack = new ArrayList<String>();
			moreWhite.addAll(checkDictKeywords(ws, WordsWhite));
			moreBlack.addAll(checkDictKeywords(ws, WordsBlack));
			fastBlack.addAll(moreBlack);
			fastWhite.addAll(moreWhite);
			fastWhite.removeAll(fastBlack); // 去重。
		}
		WordsWhite = fastWhite.toArray(new String[fastWhite.size()]);//更新内容
		WordsBlack = fastBlack.toArray(new String[fastBlack.size()]);
		
		log.warn("build ActionFeatureExtract object. "
				+ "\tAdd black words to dict: " + Arrays.asList(WordsBlack)
				+ "\twhite words to dict: " + Arrays.asList(WordsWhite));
	}
	
	private List<String> checkDictKeywords (WordSegment ws, String[] keywords) {
		List<String> res = new ArrayList<String>();
		if (keywords.length == 0) 
			return res;
		int cnt = 0;
		final Set<String> dictName = new HashSet<String>(Arrays.asList(
				new String[] {"lex-main.lex"}));
		String pdict = ws.getLexiPath();
		File fDict = new File(pdict);
		List<File> flist = new ArrayList<File>(); 
		if (fDict.isDirectory()) {
			for (File fi : fDict.listFiles()) {
				if (dictName.contains(fi.getName()))
					flist.add(fi);
			}
		} else {
			flist.add(fDict);
		}
		BufferedReader fbuf;
		String line, word;
		String []atom;
		for (File fi : flist) {
			try {
				fbuf = new BufferedReader(new FileReader(fi));
				for (; (line = fbuf.readLine()) != null;) {
					atom = line.split("/");
					if (1 <= atom.length) {
						for (int i = 0; i < keywords.length; ++i) {
							if (!atom[0].equals(keywords[i]) 
									&& atom[0].startsWith(keywords[i])) {
								res.add(atom[0]);
							}
						}
					}
				}
				fbuf.close();
				log.info("load dict [path]: " + fi + "\tsize=" + res.size());
			} catch (IOException e) {
				log.error("dict file load error. [path]: " + fi 
						+ "\t[MSG]: " + e.getMessage());
			}
		}
		return res;
	}

	@Override
	public boolean parse(int index) {
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
	/** 分词，结果直接写到对应的JSON, 格式： w1, w2, @@@,w3, ... */
	private boolean prodSegment(String key, JSONObject jObj) {
		List<String> words = new ArrayList<String>();
		List<String> pos = new ArrayList<String>();
		StringBuffer sbuf = new StringBuffer();
		String tmp;
		String[] atoms; 
		if (jObj.has(key)) {
			try {
				tmp = jObj.getString(key);
				atoms = tmp.split(LogParse.MAGIC);
				for (int i = 0; i <atoms.length; ++i) {
					iattribute = new ItemAttribute(atoms[i]);
					words.addAll(iattribute.getWord());
					pos.addAll(iattribute.getPos());
					if (atoms.length != (i + 1)) {
						words.add(LogParse.MAGIC);  // 保留特殊边界符
						pos.add(LogParse.MAGIC);
					}
				}
			} catch (Exception e) {
				log.error("can't do WordSegment. [MSG]: " + e.getMessage());
				return false;
			}
			sbuf.delete(0, sbuf.length());
			for (String s : words) 
				sbuf.append(s + SEPA_WORD);
			jObj.put(key, sbuf.toString());
			log.info("wordsegment. [input]:" + tmp
					+ "\t[output]:" + words + "\t[pos]:" + pos);
		} 
		return true;
	}
	
	public boolean BatchFormat() {
		for (int i = 0; i < this.data.size(); ++i)
			this.format(i);
		
		log.info("success finished.");
		return true;
	}
	
	@Override
	public boolean format(int index) {
		boolean flag = super.format(index);
		if (flag && this.whiteFilter(index) && !this.blackFilter(index)) {
			this.flags.set(index);
			this.data.set(index, this.data.get(index) + "\t" + this.hitWhite.keySet());
			this.hitWhite.clear();
		} else {
			this.flags.clear(index);
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
//		this.hitBlack.clear();
		this.hitWhite.clear();
		for (int i = 0; i < userAction.length(); ++i) {
			jObj = userAction.getJSONObject(i);
			fastFilter.clear();
			// 行为有效性过滤
			if (jObj.has(KEY_PURL) && ActionFilterUtil.isBadUrl(jObj.getString(KEY_PURL), configure_)) {
				log.info("bad url: " + jObj.getString(KEY_PURL));
				continue;
			}
			// 白名单关键词过滤 
			if (jObj.has(KEY_PROD)) {
				atoms = jObj.getString(KEY_PROD).split(SEPA_WORD);
				for (int j = 0; j < atoms.length; ++j) 
					fastFilter.put(atoms[j], j);
				fastKeys = fastFilter.keySet();
				fastKeys.retainAll(fastWhite);
				for (String s : fastKeys) 
					this.hitWhite.putAll(s, fastFilter.get(s));
				log.info("hit white words: " + fastKeys + "\tdata=" + jObj.getString(KEY_PROD));
			} 
			if (jObj.has(KEY_PAGE_DESC)) {
				atoms = jObj.getString(KEY_PAGE_DESC).split(DESC_SEPA);
				if (0 == atoms.length){
					log.warn("unnormal user page action format. [data]: "
							+ jObj.getString(KEY_PAGE_DESC));
				}
				if (1 <= atoms.length) {
					fastFilter.clear();
					atoms = atoms[0].split(SEPA_WORD);
					for (int j = 0; j < atoms.length; ++j) 
						fastFilter.put(atoms[j], j);
					fastKeys = fastFilter.keySet();
					fastKeys.retainAll(fastWhite);
					for (String s : fastKeys) 
						this.hitWhite.putAll(s, fastFilter.get(s));
					if (fastKeys.size() !=0) 
						log.info("hit white words: " + fastKeys + "\tdata=" + jObj.getString(KEY_PAGE_DESC));
				} 
			}
		}
		log.info("white keywords detect [res]:" + this.hitWhite);
		return (this.hitWhite.size() != 0);
	}
	
	@Override
	protected boolean blackFilter(int index) {
		
		log.info("black keywords detect [res]:" + this.hitBlack);
		return false; // 全部通过
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ActionFeatureExtract actionExtract = null;
		try {
			actionExtract = new ActionFeatureExtract(new String[]{"", ""});
			ActionFeatureExtract.batchExtract(actionExtract);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
