package com.emar.recsys.user.model;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.annotation.Target;

import org.json.JSONArray;
import org.json.JSONObject;

import weka.filters.supervised.instance.StratifiedRemoveFolds;

import com.emar.recsys.user.demo.IKeywords;

/**
 * 解析 预测结果， 合并到JSON，解决第三方库 不能通过 反射加载 序列化的对象的问题。 
 * 基于 PredictParser 归并到预测结果的问题。
 * @author zhoulm
 *
 */
public class PredictMerge {
	
	private String p_Predict;
	private String p_Srcjson;
	/** 合并后的结果存放位置 */
	private String p_Merge; 
	public int ErrParser;
	public boolean debug;
	/** JSON 中新插入字段的 KEY */
	public final String KEY = IKeywords.PSex;
	public final String ValDefClass = "?", 
			Male = "male", Female = "female", Unk = "unknown",
			PMIdx = "0", PFIdx = "1", PUIdx = "2";
	
	/**  采用标准的WEKA 预测结果。 可能有分数。 */
	public final String Sscore=",", Sclass=":", Starget="*", SRegpred = "\t| "
			;
	public final int IdxClass = 1, MinLenPred = 4; 
	
	public PredictMerge(String pathPred, String pathJson, String pathSave) {
		p_Predict = pathPred;
		p_Srcjson = pathJson;
		p_Merge = pathSave;
	}
	
	/** 处理两个文件，合并得到最终结果 
	 * @throws IOException */
	public boolean ScoreMerge() throws IOException {
		String line, jline;
		String nline;
		String[] sparser;
		BufferedWriter wjson = new BufferedWriter(new FileWriter(p_Merge));
		BufferedReader rpred = new BufferedReader(new FileReader(p_Predict));
		BufferedReader rjson = new BufferedReader(new FileReader(p_Srcjson));
		while((line = rpred.readLine()) != null 
				&& (jline = rjson.readLine()) != null) {
			sparser = this.ScorePredict(line);
			if (sparser != null) {
				nline = this.JsonAdd(jline, sparser, KEY);
			} else {
				nline = line;
				++ ErrParser;
			}
			wjson.write(nline);
			wjson.newLine();
		}
		wjson.close();
		rjson.close();
		rpred.close();
		if (debug)
			System.out.println("PredictMerge::parser err-size=" + ErrParser);
		return true;
	}
	
	/** 抽取Weka一行预测结果。主要有：类别、分数。 子类须重写。 */
	protected String[] ScorePredict(String line) {
		if (line.trim().length() == 0)
			return null;
		String[] atom = line.split(SRegpred);
		if(atom.length < MinLenPred) {
			if (debug)
				System.out.print("PredictMerg::sepa str too few. data=" + line);
			return null;
		}
		String classes = atom[IdxClass].trim();
		String scoreDist;
		if (atom.length == MinLenPred) {
			scoreDist = atom[3].trim();
		} else {
			scoreDist = atom[4].trim();
		}
		
		classes = getPredClass(classes);
		scoreDist = getPredClass(scoreDist);
		return new String[]{classes, scoreDist};
	}
	
	
	/** 抽取预测结果的类别，映射为具体的名称。 子类根据业务重写 */
	protected String getPredClass(String pidx) {
		String classname = ValDefClass;
		int pos = pidx.indexOf(Sclass);
		if (pos == -1 || pos == (Sclass.length() - 1)) {
			if (debug) 
				System.out.print("PredictMerge:: pred-class-empty="+pidx);
			return classname;
		}
		
		String c = pidx.substring(pidx.indexOf(Sclass) + 1);
		if (c.equals(PMIdx))  // 按性别分类打标注
			classname = Male;
		else if (c.equals(PFIdx))
			classname = Female;
		else if (c.equals(PUIdx))
			classname = Unk;
		
		return classname;
	}
	
	/** 抽取预测结果的分值。 */
	protected String getPredScore(String score) {
		int pbeg = score.indexOf(Starget) + 1;
		if (pbeg == 0) {
			if (debug) 
				System.out.println("PredictMerge:: pred-score no target.");
			return null;
		}
		
		int pend = score.indexOf(Sscore, pbeg);
		pend = pend == -1 ? score.length() : pend;
		
		return score.substring(pbeg, pend);
	}
	
	/** 将原始结果插入到 JSON 中： 直接在大括号末尾添加对应的字段。 */
	protected String JsonAdd(String json, String[] atom, String akey) {
		JSONArray rawdata = new JSONArray(atom);
		JSONObject obj = new JSONObject(json);
		obj.put(akey, rawdata);
		return obj.toString();
	}

}
