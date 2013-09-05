package com.emar.recsys.user.model;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

import com.emar.recsys.user.demo.IKeywords;
import com.emar.recsys.user.log.LogParse;

import weka.classifiers.Classifier;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.Utils;
import weka.core.converters.ConverterUtils.DataSource;

/**
 * 解析 模型的预测结果， 加入按Json 存储的源数据中。
 * 
 * @author zhoulm
 * 
 */
public class PredictParser {
	public final String SEPA_MR = LogParse.SEPA_MR, KPar = IKeywords.KPar,
			PSex = IKeywords.PSex, KUid = IKeywords.KUid, SSexDist = IKeywords.SSexDist;

	private List<JSONObject> atom;
	private Map<String, Integer> uidIdx; // uid: index-in-atom.
	private String psrc, pdst; 
	/** 增加 概率分布 到预测结果 */
	private boolean debug; 

	public PredictParser() {
		atom = new ArrayList<JSONObject>();
		debug = true; 
	}
	public PredictParser(String pathJson, String pathSave) throws Exception {
		this();
		psrc = pathJson;
		pdst = pathSave;
		this.init(psrc);
	}
	
	public void setdebug(boolean deb) {
		debug = deb;
	}

	public void evaluationWapper(String[] options) throws Exception {
		// TODO 预测数据. 采用与Evaluation 相同的参数。
		Classifier classifier; // = (Classifier)
		// Class.forName(classifier).newInstance();
		ObjectInputStream ois = new ObjectInputStream(new FileInputStream(
				Utils.getOption('d', options)));
		classifier = (Classifier) ois.readObject();
		ois.close();

		String testFileName = Utils.getOption('T', options);
		DataSource testSource = new DataSource(testFileName);
		Instances ins = testSource.getDataSet();
		if (ins.classIndex() < 0)
			ins.setClassIndex(ins.numAttributes() - 1);
		testSource.reset();
		ins = testSource.getStructure(ins.classIndex());
		int i = 0;
		while (testSource.hasMoreElements(ins)) {
			Instance inst = testSource.nextElement(ins);

			Instance withMissing = (Instance) inst.copy();
			withMissing.setDataset(inst.dataset());
			withMissing.setMissing(withMissing.classIndex());
			double predValue = classifier.classifyInstance(withMissing);
			double[] dist = classifier.distributionForInstance(withMissing);

			double actValue;
			double error;
			if (inst.dataset().classAttribute().isNumeric()) {
				actValue = inst.classValue();
				error = Instance.isMissingValue(predValue)
						|| inst.classIsMissing() ? 0.0 : predValue
						- inst.classValue();
			} else {
				actValue = inst.classIndex();
				error = Instance.isMissingValue(predValue)
						|| inst.classIsMissing() ? 1 : (int) predValue
						- (int) inst.classIndex();
			}
			this.mergeSource(predValue, dist, error, i);
			i++;
		}
		// Evaluation testingEvaluation = new Evaluation(new Instances(ins, 0));
		// String StrEval = Evaluation.evaluateModel(classifier, options);

	}

	private boolean mergeSource(double predValue, double[] dist, double error,
			int idx) {
		// 将预测结果更新到第K个原始数据的结果中。
		if(atom.size() <= idx)
			return false;
		
		JSONObject jobj = atom.get(idx);
		JSONObject jnew;
		if(jobj.has(KPar)) {
			jnew = jobj.getJSONObject(KPar);
		} else {
			jnew = new JSONObject();
			jobj.put(KPar, jnew);
		}
		jnew.put(PSex, predValue);
		if(debug)
			jnew.put(SSexDist, new JSONArray(dist));
		
		return true;
	}

	public boolean init(String psrc) throws Exception {
		// TODO 加载原始JOSON数据到list
		if (psrc == null)
			return false;
		File fp = new File(psrc);
		if (!fp.exists())
			return false;

		String line;
		BufferedReader rbuf = new BufferedReader(new FileReader(psrc));
		while ((line = rbuf.readLine()) != null) {
			String[] atom = line.split(SEPA_MR);
			if (atom.length < 2)
				continue;
			if (2 < atom.length) {
				for (int i = 1; i < atom.length; ++i)
					atom[1] = atom[1] + atom[i];
			}
			JSONObject jobj;
			JSONArray jRawLog;
			try {
				jobj = new JSONObject(atom[1]);
				jobj.put(KUid, atom[0].trim());
				this.atom.add(jobj);
			} catch (Exception e) {
			}
		}
		rbuf.close();

		return true;
	}

	public boolean save(String psave) throws IOException {
		if (psave == null || this.atom == null || this.atom.size() == 0)
			return false;

		BufferedWriter wbuf = new BufferedWriter(new FileWriter(psave));
		for (JSONObject jobj : this.atom) {
			wbuf.write(jobj.toString());
			wbuf.newLine();
		}

		return true;
	}

	public static void main(String[] args) {
		if (args.length < 3) {
			System.out
					.println("Usage-parameter: <json-source-path> <json-dst-path>"
							+ "[evaluation-para-set]");
			System.exit(1);
		}

		PredictParser rps = new PredictParser();
		String psource = args[0], psave = args[1];
		try {
			rps.init(psource);
			args[0] = "";
			args[1] = "";
			rps.evaluationWapper(args);
			rps.save(psave);
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

	}

}
