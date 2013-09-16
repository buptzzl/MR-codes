package com.emar.recsys.user.model;

import weka.attributeSelection.*;
import weka.core.*;
import weka.core.converters.ArffSaver;
import weka.core.converters.Saver;
import weka.core.converters.ConverterUtils.*;
import weka.classifiers.*;
import weka.classifiers.meta.*;
import weka.classifiers.trees.*;
import weka.filters.*;

import java.io.*;
import java.util.*;

/**
 * performs attribute selection using CfsSubsetEval and GreedyStepwise
 * (backwards) and trains J48 with that. Needs 3.5.5 or higher to compile.
 * 
 * @author FracPete (fracpete at waikato dot ac dot nz)
 */
public class WekaFilterFeature {

	/**
	 * uses the meta-classifier
	 */
	protected static void useClassifier(Instances data) throws Exception {
		System.out.println("\n1. Meta-classfier");
		AttributeSelectedClassifier classifier = new AttributeSelectedClassifier();
		CfsSubsetEval eval = new CfsSubsetEval();
		GreedyStepwise search = new GreedyStepwise();
		search.setSearchBackwards(true);
		J48 base = new J48();
		classifier.setClassifier(base);
		classifier.setEvaluator(eval);
		classifier.setSearch(search);
		Evaluation evaluation = new Evaluation(data);
		evaluation.crossValidateModel(classifier, data, 5, new Random(1));
		System.out.println(evaluation.toSummaryString());
	}

	/**
	 * uses the filter
	 */
	protected static void useFilter(Instances data, String path, String[] opts)
			throws Exception {
		System.out.println("\n2. Filter");

		weka.filters.supervised.attribute.AttributeSelection filter = new weka.filters.supervised.attribute.AttributeSelection();
		filter.setOptions(opts);
		// CfsSubsetEval eval = new CfsSubsetEval();
		// GreedyStepwise search = new GreedyStepwise();
		// search.setSearchBackwards(true);
		// filter.setEvaluator(eval);
		// filter.setSearch(search);
		filter.setInputFormat(data);

		Instances newData = Filter.useFilter(data, filter);
		// System.out.println(newData);
		ArffSaver asaver = new ArffSaver();
		asaver.setInstances(newData);
		asaver.setFile(new File(path));
		// asaver.setDestination(new File(path));
		asaver.writeBatch();
	}

	/** 
	 * 采用增量写方式： 基于dtrain 的头文件属性，过滤dtest 的属性与ins.
	 * 注意： 相同的属性的取值 也必须完全相同（含顺序），使用最后一列为分类标签所在列
	 */
	public static boolean dataFilter(String ptrain, String ptest,
			String pout) throws Exception {
		if (ptrain == null || ptest == null || pout == null)
			return false;
		DataSource stemp = new DataSource(ptrain);
		Instances header = stemp.getStructure();
		header.setClassIndex(header.numAttributes() -1);

		DataSource source = new DataSource(ptest);
		Instances ins = source.getStructure();
		ins.setClassIndex(ins.numAttributes() - 1);
		Attribute attr;
		int Ntemp = header.numAttributes();
		int N = 0;
		int[] temp2filter = new int[Ntemp];
		for (int i = 0; i < Ntemp; ++i) {
			attr = ins.attribute(header.attribute(i).name());
			if (attr != null) {
				temp2filter[i] = attr.index();
				N += 1;
			} else {
				temp2filter[i] = -1; // unuse.
			}
		}
		System.out.println("[info] dataFilter() total-size=" + ins.numAttributes()
				+ "\ttarget-size=" + header.numAttributes() + "\teffect-size=" + N
				+ "\nmap=" + Utils.arrayToString(temp2filter));
		
		ArffSaver parff = new ArffSaver();
		File fout = new File(pout);
//		FileOutputStream fout = new FileOutputStream(pout);
		parff.setFile(fout);
//		parff.setDestination(fout);
		parff.setRetrieval(Saver.INCREMENTAL);
//		parff.setStructure(header);  // the same as latter one.
		parff.setInstances(header);
		SparseInstance temp;
		int[] idxIns = new int[Ntemp];
		int Nfill = 0, Nr = 0, Nw = 0, Ne = 0;
		while (source.hasMoreElements(ins)) {
			for (int i = 0; i < idxIns.length; ++i) {
				idxIns[i] = -1;
			}
			Nfill = 0;
			Instance inst = source.nextElement(ins);
			Nr ++;
			for (int i = 0; i < Ntemp; ++i) {
				int idx = temp2filter[i];
				if(idx != -1 && !inst.isMissing(idx) && inst.value(idx) > 1e-6) {
					idxIns[i] = idx;
					Nfill ++;
				}
			}
			if (Nfill == 0) {
				System.out.println("[ERROR] instance is empty after filter.");
				++Ne;
			}
//			temp = new SparseInstance(Nfill);
			temp = new SparseInstance(1);
			temp.setDataset(header);
			temp.setValue(header.classIndex(), inst.value(inst.classIndex()));
			for (int i = 0; i < Ntemp && Nfill != 0; ++i)
				if (idxIns[i] != -1) {
					temp.setValue(i, inst.value(idxIns[i]));
					-- Nfill;
//					System.out.print(", idx=" + i + " unk-fidx=" + idxIns[i] + "\trval="
//							+ inst.value(idxIns[i]) + " wval=" + temp.value(i));
				}
//			System.out.println("[Test] Nfill=" + Nfill
//					+ "\nval=" + temp + "\nsrc-val=" + inst);
			parff.writeIncremental(temp);
			Nw ++;
		}
		parff.getWriter().flush();
		System.out.println("[Info] read-size=" + Nr + "\twrite-size=" + Nw
				+ "\tempty-size=" + Ne);
		
		return true;
	}

	/**
	 * uses the low level approach
	 */
	protected static void useLowLevel(Instances data, String peval,
			String psearch, String path) throws Exception {
		System.out.println("\n3. Low-level");
		AttributeSelection attsel = new AttributeSelection();
		// CfsSubsetEval eval = new CfsSubsetEval();
		ASEvaluation eval = (ASEvaluation) Class.forName(peval).newInstance();
		// GreedyStepwise search = new GreedyStepwise();
		ASSearch search = (ASSearch) Class.forName(psearch).newInstance();
		// search.setSearchBackwards(true);
		attsel.setEvaluator(eval);
		attsel.setSearch(search);
		data.setClassIndex(data.numAttributes() - 1);
		attsel.SelectAttributes(data);
		int[] indices = attsel.selectedAttributes();
		System.out.println("selected attribute indices (starting with 0):\n"
				+ Utils.arrayToString(indices));

		Instances fdata = attsel.reduceDimensionality(data);
		ArffSaver asaver = new ArffSaver();
		asaver.setInstances(fdata);
		asaver.setFile(new File(path));
		// asaver.setDestination(new File(path));
		asaver.writeBatch();

	}

	/**
	 */
	public static void main(String[] args) throws Exception {
		// load data
		String inpath = System.getProperty("Ein"), outpath = System
				.getProperty("Eout");
		System.out.println("[info] in=" + inpath + "\nout="
				+ outpath);
//		DataSource source = new DataSource(inpath);
		// DataSource source = new
		// DataSource("C:/Program Files/Weka-3-6/data/weather.nominal.arff");
		// /**
		
		// unit test.
//		String p = "D:/Data/"; 
//		dataFilter(p + "tN450.arff", p + "tunk.arff",
//				p+ "tmerge_.arff");
		dataFilter(inpath, outpath, outpath+".arff");
		// */
		// 1. meta-classifier
		// useClassifier(data);

		// 2. filter
		/**
		  System.out.println(
		  "Usage: -DEout=<out-path> -DEin=<in-path> <filter-options>" + inpath
		  + "\n" + outpath); useFilter(data, outpath, args); // 2. filter. both
		  train data & test data. String pinTest = System.getProperty("Eein"),
		  poutTest = System.getProperty("Eeout"); System.out.println(
		  "Usage: -DEout=<out-train-path> -DEin=<in-train-path> " +
		  "-DEeout=<out-test-path> -DEein=<in-test-path> <filter-options>"
		  +"\ntrain:\t" + outpath + "\t" + inpath + "\ntest:\t" + poutTest +
		  "\t" + pinTest); Instances dtest = new
		  DataSource(pinTest).getDataSet(); useFilter(data, dtest, outpath,
		  poutTest, args);
		 */

		// 3. low-level
		// System.out.println("Usage: -DEout=<out-path> -DEin=<in-path> classEval classSearch");
		// useLowLevel(data, args[1], args[2], outpath);
	}
}
