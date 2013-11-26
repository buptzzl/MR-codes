package com.emar.recsys.user.item;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.emar.recsys.user.demo.sex.SexWord;
import com.emar.recsys.user.util.UtilStr;
import com.emar.util.ConfigureTool;

/**
 * 字符串的关键词集合 识别工具。 识别对象基于配置文件实现， 对应字段集： 
 * 1 目标关键词集合. item.keyword.target;
 * 1.1 目标关键词对应的标志 item.keyword.label
 * 2 噪声关键词集合(对1的修正) item.keyword.noise
 * 
 * @author zhou
 * @Ref demo.sex.SexWord.java
 * TODO unit test.
 */
public class KeywordDetect {
	private Logger log = Logger.getLogger(KeywordDetect.class);
	private static Set<String> PUNCS;
	private static final String PCONF = "user.conf", 
			KEY_TARGET = "item.keyword.target", KEY_LABEL = "item.keyword.label",
			KEY_NOISE = "item.keyword.noise", 
			DEF_LABEL = "U", DEF_NOISE = null; 
	private static int DEF_NGRAM = 4;
	
	static {
		PUNCS = new HashSet<String>();
		PUNCS.addAll(Arrays.asList(" ", "\t", "#", "\\", "/", ",", ";", "+",
				"=", "|", "?", "-", "&", "$", "%", "*", "!", "@", "，", "；",
				"。", "？", "『", "』", "[", "]", "【", "】", "(", ")", "（", "）"));
		
	}

	public String label; // 外界调用。
	private boolean noNoise;
	private int MaxKeys, MaxNoise; // 关键词集、噪声词集中词的最大字数
	private Set<String> targetWords;
	private Set<String> noiseWords;
	private Map<String, Integer> kwordsInfo; // 记录命中的关键词信息 
	private List<String> words; 
	private ConfigureTool conf;
	
	public KeywordDetect() {
		init();
	}
	private boolean init() {
		conf = new ConfigureTool();
		conf.addResource("user.conf");
		String[] arr = conf.getStrings(KEY_TARGET);
		if (arr.length == 0) {
			log.error("unset keywords in conf.");
			System.exit(1);
		}
		targetWords = new HashSet<String>(Arrays.asList(arr));
		MaxKeys = 0;
		for (int i = 0; i < arr.length; ++i) {
			if (MaxKeys < arr[i].length() && (arr[i].toLowerCase().charAt(0) < 'a' 
					|| 'z' < arr[i].toLowerCase().charAt(0))) 
				MaxKeys = arr[i].length();
		}
		label = conf.get(KEY_LABEL, DEF_LABEL);
		arr = conf.getStrings(KEY_NOISE, DEF_NOISE);
		MaxNoise = 0;
		if (arr == null) 
			noiseWords = new HashSet<String>(0);
		else {
			noiseWords = new HashSet<String>(Arrays.asList(arr));
			for (int i = 0; i < arr.length; ++i) 
				if (MaxNoise < arr[i].length() && (arr[i].toLowerCase().charAt(0) < 'a' 
						|| 'z' < arr[i].toLowerCase().charAt(0))) 
					MaxNoise = arr[i].length();
		}
		kwordsInfo = new HashMap<String, Integer>();
		log.info("inital DONE. targetWords=" + targetWords + ", label=" + label
				+ ", noiseWords=" + noiseWords + ", max keywords length=" + MaxKeys
				+ ", max noise length=" + MaxNoise);
	}
	
	/** 识别是否含 有效的关键词对象. 子类可重写。 */
	public static boolean detect(KeywordDetect dector, String line) {
		// TODO
		dector.parse(line);
		if (dector.search() && !dector.isnoise())
			return true;
		return false;
	}
	// 将字符串拆分为 基本处理单元
	protected void parse(String line) {
		words = new ArrayList<String>();
		List<List> xgram;
		xgram = UtilStr.Xgram(line, MaxKeys, PUNCS); // 含汉字的串的xgram切割
		for (List xi : xgram) {
			words.addAll(xi);
		}
		
		return ;
	}
	// 搜索关键词 
	protected boolean search() {
		return this.ismatch(words, targetWords, kwordsInfo);
	}
	// 对搜索结果过滤噪声。
	protected boolean isnoise() {
		Map<String, Integer> mTmp = new HashMap<String, Integer>();
		return this.ismatch(words, noiseWords, mTmp);
	}
	private boolean ismatch(Collection coll, Collection filter, Map info) {
		info.clear();
		for (int i = 0; i < coll.size(); ++i) {
			if (filter.contains(words.get(i))) {
				info.put(, value)// TODO 使用Guava 的multimap 记录位置。
			}
		}
		return info.size() != 0;
	}
	
}
