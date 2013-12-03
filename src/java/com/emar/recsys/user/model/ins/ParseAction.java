package com.emar.recsys.user.model.ins;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.emar.recsys.user.demo.IKeywords;
import com.emar.recsys.user.log.BaseLog;
import com.emar.util.ConfigureTool;

/**
 * 解析一个已分词的用户行为数据文件中的行（uid\t[json]），整个文件为一个类，生成一个训练样本。
 * @author zhoulm
 * @see 基于 user.conf 配置文件， 固定类别、类别属性名；可选地抽取各字段；分割符部分不变；
 */
public class ParseAction implements IAttrParser {
	private static Logger log = Logger.getLogger(ParseAction.class);
	private static final String SEP_LOG = "\t", SEP_PAG = "@@@", SEP_WORD = ", ",
			DEF_LABEL = "0";
	private String[] DEF_KEY_WORDS = new String[] {
			"pagewords", 
		}, DEF_KEY_ATOM = {
			"prod_name", "prod_type_name", 
		};
	/** 统计每用户的：行为数，特征数分布  */
	private static int[] actionCounter, featureCounter;
	private static int userCounter;
	static {
		actionCounter = new int[0];
		featureCounter = new int[0];
		userCounter = 0;
	}
	
	private ConfigureTool configure;
	private String[] KEY_WORDS, KEY_ATOM;
	/** 用户行为数范围    */
	private int MinActPerUser, MaxActPerUser, MaxUser;
	private int cindex;  // 类别索引
	/**  类别属性标签的内容  */
	private String labelAttribute;
	private String labelValue; // 类别标签
	private Set<String> features;
	private boolean pageFilter; // 对页面描述的内容筛选第1字段
	private String user;
	private JSONArray action;
	
	public ParseAction() {
		configure = new ConfigureTool();
		configure.addResource("user.conf");
		this.init();
	}
	public ParseAction(String confPath) {
		configure = new ConfigureTool();
		configure.addResource(confPath);
		this.init();
	}

	@Override
	public boolean init(String... args) {
		MinActPerUser = configure.getInt("extract.instance.minactperuser", 50);
		MaxActPerUser = configure.getInt("extract.instance.maxactperuser", 1000);
		MaxUser = configure.getInt("extract.instance.maxuser", 10000);
		labelAttribute = configure.get("extract.instance.attribute");
		labelValue = configure.get("extract.instance.labelvalue", DEF_LABEL);
		cindex = configure.getInt("extract.instance.classindex", 1);
		pageFilter = configure.getBoolean("extract.instance.pagefilter", true);
		KEY_WORDS = configure.getStrings("extract.instance.keyword", DEF_KEY_WORDS);
		KEY_ATOM = configure.getStrings("extract.instance.keyatom", DEF_KEY_ATOM);
		
		features = new HashSet<String>();
		user = null;
		action = new JSONArray();
		
		if (actionCounter.length < configure.getInt("extract.instance.actioncounter", 101)) {
			int[] tmpCnt = new int[configure.getInt("extract.instance.actioncounter", 101)];
			System.arraycopy(actionCounter, 0, tmpCnt, 0, actionCounter.length);
			actionCounter = tmpCnt;
			int[] tmpFea = new int[actionCounter.length * 10]; //特征数的范围十倍于行为数
			System.arraycopy(featureCounter, 0, tmpFea, 0, featureCounter.length);
			featureCounter = tmpFea;
		}
		return false;
	}

	@Override
	public String getAttribute() {
		return labelAttribute;
	}

	@Override
	public void setAttribute(String s) {
		labelAttribute = s;
	}

	@Override
	public Boolean parseLine(String line) {
		if (!this.parse(line))
			return false;
		if (!this.Filter())
			return false;
		
		String[] atoms = null;
		JSONObject jobj = null;
		for (int i = 0; i < action.length(); ++i) {
			jobj = action.getJSONObject(i);
			for (int j = 0; j < KEY_ATOM.length; ++j) {
				if (jobj.has(KEY_ATOM[j])) 
					features.add(jobj.getString(KEY_ATOM[j]));
			}
			for (int j = 0; j <KEY_WORDS.length; ++j) {  // 分词后的特征
				if (jobj.has(KEY_WORDS[j])) {
					atoms = jobj.getString(KEY_WORDS[j]).split(SEP_WORD);
					features.addAll(Arrays.asList(atoms));
				}
			}
		}
		// 统计信息
		++userCounter;
		if (features.size() < featureCounter.length)
			featureCounter[features.size()]++;
		else 
			featureCounter[0]++;
		log.info("parse a user's action.");
		return true;
	}

	@Override
	public Object getClassify() {
		return labelValue;
	}

	@Override
	public Object[] getFeatures() {
		return features.toArray(new String[features.size()]);
	}

	@Override
	public Object getWeight() {
		// 0 无权重； 
		// TODO: 可能的拓展： 1  如果末尾有关键词数组，则已其为权重； 2 按用户的行为数做权重；
		return null;
	}

	@Override
	public int getClassIndex() {
		return -1;  // 写死的类别，无索引
	}

	@Override
	public void countClass(int idx) { // 在parse 中直接记录
	}

	@Override
	public String getStaticInfo() {
		return String.format("total user=%d, action-distribution=%s," +
				"feature-distribution=", userCounter, 
				Arrays.asList(actionCounter)+"", Arrays.asList(featureCounter)+"");
	}
	/** 从原始日志抽取元数据： user,action  */
	protected boolean parse(String line) {
		JSONArray jArrs;
		action = new JSONArray();
		String[] atom = line.split(SEP_LOG);
		if (atom.length != 2) {
			log.warn("data format error. [data]=" + line);
			return false;
		}

		user = atom[0];
		try {
			jArrs = new JSONArray(atom[1]);
			for (int i = 0; i < jArrs.length(); ++i) 
				action.put(new JSONObject(jArrs.getString(i)));
		} catch (JSONException e) {
			log.error("bad user JSON action str. [STR]: " + atom[1]
					+ ", [MSG]: " + e.getMessage());
		}
		
		log.debug("success finished.");
		return true;
	}
	/** 清洗元数据  */
	private boolean Filter() {
		// 1 判断行为数阈值； 2 数据清洗：保留关键KEY；关键字段； @附带统计行为数的分布
		if (user == null || action.length() == 0) {
			log.error("unobserved user info");
			return false;
		}
		if (action.length() < actionCounter.length) { 
			actionCounter[action.length()]++;
		} else {
			actionCounter[0]++;
		}
		if (MinActPerUser > action.length() && action.length() >= MaxActPerUser) {
			log.info("action size unnormal." + action.length());
			return false;
		}
		
		JSONObject jobj = null;
		Set<String> kfilter = null;
		String[] atoms;
		for (int i = 0; i < action.length(); ++i) {
			jobj = action.getJSONObject(i);
			kfilter = jobj.keySet();
			kfilter.removeAll(Arrays.asList(KEY_WORDS));
			kfilter.removeAll(Arrays.asList(KEY_ATOM));
			for (String ki : kfilter) {
				jobj.remove(ki);
			}
			if (pageFilter) {
				// 对PageWord 进行清洗
				atoms = jobj.getString(KEY_WORDS[0]).split(SEP_PAG);
				if (1 <= atoms.length)
				jobj.put(KEY_WORDS[0], atoms[0]);
			}
		}
		log.debug("action after filter: " + action);
		return true;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
