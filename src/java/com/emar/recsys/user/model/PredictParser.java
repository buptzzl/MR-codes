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

import org.apache.hadoop.security.KerberosName.NoMatchingRule;
import org.json.JSONArray;
import org.json.JSONObject;

import com.emar.recsys.user.demo.IKeywords;
import com.emar.recsys.user.log.LogParse;
import com.emar.recsys.user.util.UtilMath;

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
			PSex = IKeywords.PSex, KUid = IKeywords.KUid,
			SSexDist = IKeywords.SSexDist;

	private List<JSONObject> atom;
	private List<List> scores; //
	private Map<String, Integer> uidIdx; // uid: index-in-atom.
	/** 原始数据、输出整合结果的 路径 */
	private String psrc, pdst;
	/** 按Evaluation 格式赋值的模型测试参数 */
	private String[] m_evalParam;
	private String KEY; // json 的key
	/** 原始文件的行数（规模） */
	private int Nsrc, Incorrect;
	/** 增加 概率分布 到预测结果 */
	private boolean debug;

	public PredictParser(String pathJson, String pathSave, String[] opts)
			throws Exception {
		atom = new ArrayList<JSONObject>();
		scores = new ArrayList<List>();
		debug = false;
		Nsrc = 0;
		Incorrect = 0;
		psrc = pathJson;
		pdst = pathSave;
		m_evalParam = opts;
		KEY = IKeywords.PSex;
		this.init();
	}

	public PredictParser(String pathJson, String pathSave, String key,
			String[] opts) throws Exception {
		this(pathJson, pathSave, opts);
		KEY = key;
	}

	public void setdebug(boolean deb) {
		debug = deb;
	}

	/** 预测数据. 采用与Evaluation 相同的参数。 */
	public void evaluationWapper() throws Exception {
		String[] options = this.m_evalParam;
		Classifier classifier; // = (Classifier)
		// Class.forName("weka.classifiers.functions.LibLINEAR").newInstance();
		ObjectInputStream ois = new ObjectInputStream(new FileInputStream(
				Utils.getOption('l', options)));
		classifier = (Classifier) ois.readObject();
		ois.close();

		String testFileName = Utils.getOption('T', options);
		DataSource testSource = new DataSource(testFileName);
		Instances ins = testSource.getDataSet();
		if (ins.classIndex() < 0)
			ins.setClassIndex(ins.numAttributes() - 1);
		testSource.reset();
		ins = testSource.getStructure(ins.classIndex());
		// int i = 0;
		while (testSource.hasMoreElements(ins)) {
			Instance inst = testSource.nextElement(ins);

			this.instancePredict(inst, classifier);
			// i++;
		}

	}

	/** 处理1个实例 */
	private void instancePredict(Instance inst, Classifier classifier)
			throws Exception {
		Instance withMissing = (Instance) inst.copy();
		withMissing.setDataset(inst.dataset());
		withMissing.setMissing(withMissing.classIndex());
		double predValue = classifier.classifyInstance(withMissing);
		double[] dist = classifier.distributionForInstance(withMissing);

		double actValue;
		double error;
		if (inst.dataset().classAttribute().isNumeric()) {
			// actValue = inst.classValue();
			error = Instance.isMissingValue(predValue) || inst.classIsMissing() ? 0.0
					: predValue - inst.classValue();
		} else {
			error = Instance.isMissingValue(predValue) || inst.classIsMissing() ? 1
					: (int) predValue - (int) inst.classValue();
		}
		if (Math.abs(error) > UtilMath.m_Zero)
			Incorrect++;

		dist = UtilModel.adjustSexPredict(dist);

		List<Double> predVals = new ArrayList<Double>(dist.length);
		for (int j = 0; j < dist.length; ++j)
			predVals.add(dist[j]);
		this.scores.add(predVals);

		if (debug)
			System.out.println("[Info] predVal=" + predValue + "\tprob-dist="
					+ Utils.arrayToString(dist) + "\tdata=" + inst);
		return;
	}

	/** 基于两个模型预测数据（先male, 后female. 需要保证两个测试文件的实例的顺序一致）. 采用与Evaluation 相同的参数。 */
	public void evaluationWapper(String[] args) throws Exception {
		// 原始模型一: male
		String[] options = this.m_evalParam;
		String tmpPath = Utils.getOption('l', options);
		System.out.println("[info] evaluationWapper() " + tmpPath);
		ObjectInputStream ois = new ObjectInputStream(new FileInputStream(
				tmpPath));
		// Utils.getOption('l', options)));
		Classifier classifier = (Classifier) ois.readObject();
		ois.close();

		String testFileName = Utils.getOption('T', options);
		DataSource testSource = new DataSource(testFileName);
		Instances ins = testSource.getDataSet();
		if (ins.classIndex() < 0)
			ins.setClassIndex(ins.numAttributes() - 1);
		testSource.reset();
		ins = testSource.getStructure(ins.classIndex());
		// 参数指定的模型二: female.
		options = args;
		ois = new ObjectInputStream(new FileInputStream(Utils.getOption('l',
				options)));
		Classifier classifier2 = (Classifier) ois.readObject();
		ois.close();
		testFileName = Utils.getOption('T', options);
		DataSource testSource2 = new DataSource(testFileName);
		Instances ins2 = testSource2.getDataSet();
		if (ins2.classIndex() < 0)
			ins2.setClassIndex(ins2.numAttributes() - 1);
		testSource2.reset();
		ins2 = testSource2.getStructure(ins2.classIndex());
		// List male = new ArrayList<>(), female = new ArrayList<double[]>();
		double[] male = new double[ins.numInstances()], female = new double[ins2
				.numInstances()];
		final int IdxPositive = 1; // 预测结果中 性别为真时的概率下标
		int i = 0;

		while (testSource.hasMoreElements(ins)) {
			Instance inst = testSource.nextElement(ins);
			Instance withMissing = (Instance) inst.copy();
			withMissing.setDataset(inst.dataset());
			withMissing.setMissing(withMissing.classIndex());
			double predValue = classifier.classifyInstance(withMissing);
			double[] dmale = classifier.distributionForInstance(withMissing);
			male[i] = dmale[IdxPositive];

			Instance inst2 = testSource2.nextElement(ins2);
			Instance withMissing2 = (Instance) inst2.copy();
			withMissing2.setDataset(inst2.dataset());
			withMissing2.setMissing(withMissing2.classIndex());
			double predValue2 = classifier2.classifyInstance(withMissing);
			double[] dfemale = classifier2
					.distributionForInstance(withMissing2);
			female[i] = dfemale[IdxPositive];

			++i;
			if (debug)
				System.out.println("[Info] predVal=" + predValue
						+ "\tprob-dist=" + "\tdata=" + inst);
		}
		this.scoreMerge(male, female);
	}

	/** 归一化预测分值， 更新男女性别的预测分值。 */
	private void scoreMerge(double[] male, double[] female) {
		double m_mean = Utils.mean(male), f_mean = Utils.mean(female), m_var = Utils
				.variance(male), f_var = Utils.variance(female);
		double m_score, f_score, normal = 1;
		for (int j = 0; j < Nsrc; ++j) {
			m_score = male[j];
			f_score = female[j];
			if ((male[j] < 0.5 && female[j] < 0.5)
					|| (male[j] > 0.5 && female[j] > 0.5)) { // 进行分值校正前先归一化
				m_score = (male[j] - m_mean) / m_var;
				f_score = (female[j] - f_mean) / f_var;
				if (male[j] < 0.5) {
					normal = 0.5;
				}
				m_score = normal * 1 / (1 + Math.log(m_score));
				f_score = normal * 1 / (1 + Math.log(f_score));
				normal = 1; // 还原
			}
			double[] d_score = UtilModel.adjustSexPredict(m_score, f_score);
			List<Double> predVals = new ArrayList<Double>(d_score.length);
			for (int k = 0; k < d_score.length; ++k)
				predVals.add(d_score[j]);
			this.scores.add(predVals);
		}

	}

	/**
	 * 将预测结果更新到第K个原始数据的结果中。
	 * 
	 * @deprecated 支持使用动态key.
	 */
	private boolean mergeSource(double predValue, double[] dist, double error,
			int idx) {
		if (atom.size() <= idx)
			return false;

		JSONObject jobj = atom.get(idx);
		JSONObject jnew;
		if (jobj.has(KPar)) {
			jnew = jobj.getJSONObject(KPar);
		} else {
			jnew = new JSONObject();
			jobj.put(KPar, jnew);
		}
		jnew.put(PSex, predValue);
		if (debug)
			jnew.put(SSexDist, new JSONArray(dist));

		return true;
	}

	/** 统计原始数据 的行数 */
	private boolean init() throws Exception {
		if (psrc == null)
			return false;
		File fp = new File(psrc);
		if (!fp.exists())
			return false;

		String line;
		BufferedReader rbuf = new BufferedReader(new FileReader(psrc));
		while ((line = rbuf.readLine()) != null) {
			this.Nsrc++;
		}
		rbuf.close();

		return true;
	}

	/** 保存json 串到文件. 原始文件非JSON时 先将其以Root为key插入JSON */
	public boolean save() throws IOException {
		if (pdst == null || // this.atom == null || this.atom.size() == 0)
				this.scores.size() != this.Nsrc) { // can't match per-line.
			if (debug)
				System.out
						.println("[Error] PredictParser::save source-file-size != predict-size");
			return false;
		}

		String line;
		JSONObject jobj;
		JSONArray jRawLog;
		int cnt = 0;

		BufferedWriter wbuf = new BufferedWriter(new FileWriter(pdst));
		BufferedReader rbuf = new BufferedReader(new FileReader(psrc));
		while ((line = rbuf.readLine()) != null) {
			String[] atom = line.split(SEPA_MR);
			if (atom.length < 2) {
				jobj = new JSONObject(line.trim()); // 全部为JSON格式
			} else {
				try {
					for (int i = 2; i < atom.length; ++i)
						atom[1] += " " + atom[i];
					atom[1] = atom[1].trim();
					jobj = new JSONObject(atom[1]);
				} catch (Exception e) {
					jobj = new JSONObject();
					if (atom[1].length() != 0)
						jobj.put(IKeywords.Root, atom[1]);
				}
				jobj.put(KUid, atom[0].trim()); // 转换为全 JSON 格式
			}
			jobj.put(this.KEY, new JSONArray(this.scores.get(cnt).toArray()));
			++cnt;
			// wbuf.write(atom[0] + SEPA_MR + jobj.toString());
			wbuf.write(jobj.toString());
			wbuf.newLine();
		}

		return true;
	}

	public static void main(String[] args) {
		if (args.length < 3) {
			System.out
					.println("Usage-parameter: <json-source-path> <json-dst-path> "
							+ "[\"evaluation-man-set\"] [\"eval-female-set\"] "
							+ "[key's name]");
			System.exit(1);
		}

		String psource = args[0], psave = args[1];
		args[0] = "";
		args[1] = "";
		System.out.println("[Info] PredictParse\nsource-file=" + psource
				+ "\tsave-file=" + psave);

		String key = null;
		String[] s_default, female = null;
		s_default = args[2].split(" ");
		if (args.length >= 4) {
			female = args[3].split(" ");
			args[3] = "";
		}
		if (args.length >= 5) {
			key = args[4].trim();
			args[4] = "";
		}
		try {
			PredictParser rps;
			// unit test. 
//			psource = "C:/Program Files/Weka-3-6/data/credit-g.txt";
//			psave = psource + ".res"; //D:/Downloads/telnet/good_sex.731.test"; 
//			s_default = new String[] { 
//					"-l", "C:/Program Files/Weka-3-6/model/test.credit-g.Liblinear.model",
//			  "-T", "C:/Program Files/Weka-3-6/data/credit-g.arff" 
//			 "D:/Downloads/telnet/good_sex.731_fs.IG1E-8.arff" 
//			 }; 
//			female = null;
			
			if (key == null)
				rps = new PredictParser(psource, psave, s_default);
			else
				rps = new PredictParser(psource, psave, key, s_default);
			// rps.init();
			args[0] = "";
			args[1] = "";
			// rps.setdebug(true);
			if (female == null)
				rps.evaluationWapper();
			else
				rps.evaluationWapper(female);
			rps.save();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
