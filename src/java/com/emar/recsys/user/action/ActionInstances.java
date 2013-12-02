package com.emar.recsys.user.action;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import weka.core.Instances;

import com.emar.recsys.user.model.ins.IAttrParser;
import com.emar.recsys.user.util.UtilObj;

/**
 * 基于频率抽取样本。使用IAttrParser 的实例抽取形成Arff 文件.
 * 支持指定已有的一个arff 文件的头。
 * @author zhoulm
 * 
 */
public class ActionInstances extends ActionExtract {
	private static Logger log = Logger.getLogger(ActionInstances.class);
	private static final String INFO_HEAD = "@relation user action", 
			INFO_ATTR = "@attribute", INFO_VAL = "{0, 1}", INFO_DATA = "@data",
			INFO_EMP = "{}", SEPA_NL = "\n", SEPA = ", ";
	private static final String[] KEY_WORDS = new String[] {
		"prod_name", "prod_type_name", "pagewords",
	};
	private int MinActPerUser, MaxActPerUser, MaxUser;
	private int counter;
	private Map<String, Integer> features;
	private IAttrParser classifyParser;
	private String arffHead;
	
	public int getFeatureSize() {
		return features.size();
	}
	public int getFeatureFrequence(String key) {
		return features.containsKey(key) ? features.get(key) : 0;
	}
	public int getMinActPerUser() {
		return MinActPerUser;
	}
	public String getArffHead()	{
		return arffHead;
	}
	
	private void initInstances() throws IOException {
		MinActPerUser = configure_.getInt("extract.instance.minactperuser", 50);
		MaxActPerUser = configure_.getInt("extract.instance.maxactperuser", 1000);
		MaxUser = configure_.getInt("extract.negative.maxuser", 10000);
		
		features = new HashMap<String, Integer>(1024, 0.95f);
		String arffHead = configure_.get("extract.instance.arffpath");
		BufferedReader rbuf = new BufferedReader(new FileReader(arffHead));
		String[] atom;
		int cnt = 0, nline = 0;
		for (String line; (line = rbuf.readLine()) != null; ) {
			atom = line.split(" ");
			if (atom[0].equals("@attribute") && atom.length == 3) 
				features.put(atom[1], cnt++);
			++ nline;
		}
		rbuf.close();
		log.info("set size, MinActPerUser=" + MinActPerUser 
				+ ", MaxActPerUser" + MaxActPerUser + ", MaxUser=" + MaxUser
				+ ", feature-size=" + cnt + ", arff-lines=" + nline);
	}
	
	private static String buildArffHead(Map<String, Integer> Finfo) {
		StringBuffer sbuf = new StringBuffer();
		sbuf.append(INFO_HEAD + "\t" + System.currentTimeMillis()/1000 + SEPA_NL);
		List<Entry<String, Integer>> sMap = UtilObj.entrySort(Finfo, false);
		for (Entry<String, Integer> ei : sMap) 
			sbuf.append(INFO_ATTR + " " + ei.getKey() + " " + INFO_DATA + SEPA_NL);
		sbuf.append(INFO_DATA + SEPA_NL);
		return sbuf.toString();
	}
	
	@Override
	public boolean BatchFormat() {
		// 先写 Arff 文件头(写到第1 个样本的内容前)，再写各个样本。
		if (this.data.size() == 0) {
			log.info("data is empty.");
			return false;
		}
		arffHead = buildArffHead(features);
		super.format(0);
		this.data.set(0, arffHead + this.data.get(0) + SEPA_NL);
		for (int i = 1; i < this.data.size(); ++i) {
			super.format(i);
		}
		
		log.debug("finish.");
		return true;
	}
	
	@Override
	public String formatUserActions(String uid, JSONArray action) {
		// 抽样特征词， 基于IAttrParser 解析输出全局的arff 向量索引。不对特征做任何过滤，新的特征添加到容器中。
		StringBuffer sbuf = new StringBuffer();
		sbuf.append(INFO_EMP + SEPA_NL);
		if (action == null) 
			return sbuf.toString();
		JSONObject jobj = null;
		String[] atoms = null;
		for (int i = 0; i < action.length(); ++i) {
			jobj = action.getJSONObject(i);
			for (int j = 0; j < KEY_WORDS.length; ++j) {
				if (jobj.has(KEY_WORDS[j])) {
					atoms = jobj.getString(KEY_WORDS[j]).split(SEPA);
					for (int k = 0; k < atoms.length; ++k) {
						if (!features.containsKey(atoms[k])) {
							features.put(atoms[k], features.size());
						}
						sbuf.append();
						Instances
					}
				}
			}
		}
	}
	
	
	@Override
	public boolean Filter(int index) {
		// 自定义基于用户行为频率的过滤。
		if (this.userAction != null && this.userAction.length() < MaxActPerUser
				&& MinActPerUser < this.userAction.length() && counter < MaxUser) {
			counter++;
			return true;
		}
		return false;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
