package com.emar.recsys.user.model;

import weka.attributeSelection.*;
import weka.core.*;
import weka.core.converters.ArffSaver;
import weka.core.converters.ConverterUtils.*;
import weka.classifiers.*;
import weka.classifiers.functions.LibLINEAR;
import weka.classifiers.functions.Logistic;
import weka.classifiers.meta.*;
import weka.classifiers.trees.*;
import weka.filters.*;
 
import java.io.File;
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
  protected static void useFilter(Instances data, String path, String[] opts) throws Exception {
    System.out.println("\n2. Filter");
    
    weka.filters.supervised.attribute.AttributeSelection filter = new weka.filters.supervised.attribute.AttributeSelection();
    filter.setOptions(opts);
//    CfsSubsetEval eval = new CfsSubsetEval();
//    GreedyStepwise search = new GreedyStepwise();
//    search.setSearchBackwards(true);
//    filter.setEvaluator(eval);
//    filter.setSearch(search);
    filter.setInputFormat(data);
    
    Instances newData = Filter.useFilter(data, filter);
//    System.out.println(newData);
    ArffSaver asaver = new ArffSaver();
    asaver.setInstances(newData);
    asaver.setFile(new File(path));
//    asaver.setDestination(new File(path));
    asaver.writeBatch();
  }
  
  protected static void useFilter(Instances dtrain, Instances dtest, 
		  String ptrain, String ptest, String[] opts) throws Exception { 
	weka.filters.supervised.attribute.AttributeSelection filter = new weka.filters.supervised.attribute.AttributeSelection();
	filter.setOptions(opts);
	filter.setInputFormat(dtrain);
	
	ArffSaver asa = new ArffSaver();
	Instances fTrain = Filter.useFilter(dtrain, filter);
	asa.setInstances(fTrain);
	asa.setFile(new File(ptrain));
	asa.writeBatch();
	
	Instances fTest = Filter.useFilter(dtest, filter);
	asa.setInstances(fTest);
	asa.setFile(new File(ptest));
	asa.writeBatch();
  }
 
  /**
   * uses the low level approach
   */
  protected static void useLowLevel(Instances data, String peval, String psearch,
		  String path) throws Exception {
    System.out.println("\n3. Low-level");
    AttributeSelection attsel = new AttributeSelection();
//    CfsSubsetEval eval = new CfsSubsetEval();
    ASEvaluation eval = (ASEvaluation) Class.forName(peval).newInstance();
//    GreedyStepwise search = new GreedyStepwise();
    ASSearch search = (ASSearch) Class.forName(psearch).newInstance();
//    search.setSearchBackwards(true);
    attsel.setEvaluator(eval);
    attsel.setSearch(search);
    data.setClassIndex(data.numAttributes() - 1);
    attsel.SelectAttributes(data);
    int[] indices = attsel.selectedAttributes();
    System.out.println("selected attribute indices (starting with 0):\n" + Utils.arrayToString(indices));
    
    Instances fdata = attsel.reduceDimensionality(data);
    ArffSaver asaver = new ArffSaver();
    asaver.setInstances(fdata);
    asaver.setFile(new File(path));
//    asaver.setDestination(new File(path));
    asaver.writeBatch();
    
  }
 
  /**
   * takes a dataset as first argument
   *
   * @param args        the commandline arguments
   * @throws Exception  if something goes wrong
   */
  public static void main(String[] args) throws Exception {
    // load data
    System.out.println("\n0. Loading data");
    String inpath = System.getProperty("Ein"), 
    		outpath = System.getProperty("Eout");
    DataSource source = new DataSource(inpath);
//    DataSource source = new DataSource("C:/Program Files/Weka-3-6/data/weather.nominal.arff");
    Instances data = source.getDataSet();
    if (data.classIndex() == -1)
      data.setClassIndex(data.numAttributes() - 1);
 
    // 1. meta-classifier
//    useClassifier(data);
 
    // 2. filter
    System.out.println("Usage: -DEout=<out-path> -DEin=<in-path> <filter-options>"
    		+ inpath + "\n" + outpath);
    useFilter(data, outpath, args);
    // 2. filter. both train data & test data.
    String pinTest = System.getProperty("Eein"), poutTest = System.getProperty("Eeout");
    System.out.println("Usage: -DEout=<out-train-path> -DEin=<in-train-path> "
    		+ "-DEeout=<out-test-path> -DEein=<in-test-path> <filter-options>" 
    		+"\ntrain:\t" + outpath + "\t" + inpath 
    		+ "\ntest:\t" + poutTest + "\t" + pinTest);
    Instances dtest = new DataSource(pinTest).getDataSet();
    useFilter(data, dtest, outpath, poutTest, args);
    
 
    // 3. low-level
//    System.out.println("Usage: -DEout=<out-path> -DEin=<in-path> classEval classSearch");
//    useLowLevel(data, args[1], args[2], outpath);
  }
}
