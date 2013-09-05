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

	/** 采用增量写方式： 基于dtrain 的头文件属性，过滤dtest 的属性与ins */
	public static boolean dataFilter(String ptemplate, String pfilter,
			String pout) throws Exception {
		if (ptemplate == null || pfilter == null || pout == null)
			return false;
		DataSource stemp = new DataSource(ptemplate);
//		System.out.println("[test] loader=" + stemp.getLoader());
		Instances header = stemp.getStructure();

		DataSource source = new DataSource(pfilter);
		Instances ins = source.getStructure();
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
				+ "\ttarget-size=" + header.numAttributes() + "\teffect-size=" + N);
		// Enumeration enu = ins.enumerateAttributes();
		// while (enu.hasMoreElements()) {
		// Attribute attribute = (Attribute) enu.nextElement();
		// }
		
		ArffSaver parff = new ArffSaver();
		File fout = new File(pout);
//		FileOutputStream fout = new FileOutputStream(pout);
		parff.setFile(fout);
//		parff.setDestination(fout);
		parff.setRetrieval(Saver.INCREMENTAL);
//		parff.setStructure(header);  // the same as latter one.
		parff.setInstances(header);
		Instance temp;
		while (source.hasMoreElements(ins)) {
			Instance inst = source.nextElement(ins);
			temp = new Instance(Ntemp);
			temp.setDataset(header);
			for (int i = 0; i < Ntemp; ++i) {
				int idx = temp2filter[i];
				if(idx != -1) {
					temp.setValue(i, inst.stringValue(idx));  //header.attribute(idx))
				} else {
					temp.setMissing(i);
				}
			}
			parff.writeIncremental(temp);
		}
		parff.getWriter().flush();
		
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
		System.out.println("\n0. Loading data");
		String inpath = System.getProperty("Ein"), outpath = System
				.getProperty("Eout");
//		DataSource source = new DataSource(inpath);
		// DataSource source = new
		// DataSource("C:/Program Files/Weka-3-6/data/weather.nominal.arff");
		// /**
		System.out.println("[info] in=" + inpath + "\nout="
				+ outpath);
		// unit test.
//		String p = "D:/Data/MR-codes/data/test/";
//		dataFilter(p + "merge_filter.arff", p + "merge_template.arff", 
//				p+ "merge_.arff");
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
