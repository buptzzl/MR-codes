package com.emar.recsys.user.model;

import java.io.File;

import weka.classifiers.*;
import weka.classifiers.meta.Vote;
import weka.core.*;
import weka.core.converters.ArffLoader;

public class WekaEnsemble {
	private String trainFile, testFile;
	private String cls1, cls2, cls3;
	
	public WekaEnsemble() {
		trainFile = "C://Program Files//Weka-3-6//data//segment-challenge.arff";
	}

	public static void main(String[] args) {
//		String trainFile = "C://Program Files//Weka-3-6//data//segment-challenge.arff";
//		String testFile = "C://Program Files//Weka-3-6//data//segment-test.arff";
		String trainFile = args[0];
		String testFile = args[1];
		
	       Instances trainIns = null;
	       Instances testIns = null;
	       Classifier cfs1 = null;
	       Classifier cfs2 = null;
	       Classifier cfs3 = null;
	       Classifier[] cfsArray = new Classifier[3];
	      
	       try{
	          
	           /*
	            * 1.读入训练、测试样本
	            * 在此我们将训练样本和测试样本是由weka提供的segment数据集构成的
	            */
	           File file= new File(trainFile);
	           ArffLoader loader = new ArffLoader();
	           loader.setFile(file);
	           trainIns = loader.getDataSet();
	          
	           file = new File(testFile);
	           loader.setFile(file);
	           testIns = loader.getDataSet();
	          
	           //在使用样本之前一定要首先设置instances的classIndex，否则在使用instances对象是会抛出异常
	           trainIns.setClassIndex(trainIns.numAttributes()-1);
	           testIns.setClassIndex(testIns.numAttributes()-1);
	          
	          
	           /*
	            * 2.初始化基分类器
	            * 具体使用哪一种特定的分类器可以选择，请将特定分类器的class名称放入forName函数
	            * 这样就构建了一个简单的分类器
	            */
	           //贝叶斯算法
	           cfs1 = (Classifier)Class.forName("weka.classifiers.bayes.NaiveBayes").newInstance();
	           //决策树算法，是我们常听说的C45的weka版本，不过在我看代码的过程中发现有一些与原始算法有点区别的地方。
	           //即在原始的C45算法中，我们规定没有一个属性节点在被使用（即被作为一个分裂节点以后，他将被从属性集合中去除掉）。
	           //但是在J48中没有这样做，它依然在下次分裂点前，使用全部的属性集合来探测一个合适的分裂点。这样做好不好？
	           cfs2 = (Classifier)Class.forName("weka.classifiers.trees.J48").newInstance();
	           cfs3 = (Classifier)Class.forName("weka.classifiers.functions.Logistic").newInstance();
	                    
	           /*
	            * 3.构建ensemble分类器
	            */
	          
	           cfsArray[0] = cfs1;
	           cfsArray[1] = cfs2;
	           cfsArray[2] = cfs3;
	          
	           Vote ensemble = new Vote();
	           /*
	            * 订制ensemble分类器的决策方式主要有：
	            * AVERAGE_RULE
	            * PRODUCT_RULE
	            * MAJORITY_VOTING_RULE
	            * MIN_RULE
	            * MAX_RULE
	            * MEDIAN_RULE
	            * 它们具体的工作方式，大家可以参考weka的说明文档。
	            * 在这里我们选择的是多数投票的决策规则
	            */
	           SelectedTag tag1 = new SelectedTag(
	                  Vote.MAJORITY_VOTING_RULE, Vote.TAGS_RULES);
	 
	           ensemble.setCombinationRule(tag1);
	           ensemble.setClassifiers(cfsArray);
	           //设置随机数种子
	           ensemble.setSeed(2);
	           //训练ensemble分类器
	           ensemble.buildClassifier(trainIns);
	          
	           /*
	            * 4.使用测试样本测试分类器的学习效果
	            * 在这里我们使用的训练样本和测试样本是同一个，在实际的工作中需要读入一个特定的测试样本
	            */
	           Instance testInst;
	           /*
	            * Evaluation: Class for evaluating machine learning models
	            * 即它是用于检测分类模型的类
	            */
	           Evaluation testingEvaluation = new Evaluation(testIns);
//	           System.out.println(Evaluation.evaluateModel(
//						ensemble, new String[]{"-t", trainFile, "-i"}));
	           int length = testIns.numInstances();
	           for (int i =0; i < length; i++) {
	              testInst = testIns.instance(i);
	              //通过这个方法来用每个测试样本测试分类器的效果
	              testingEvaluation.evaluateModelOnceAndRecordPrediction(
	                     ensemble, testInst);
	           }
	          
	           /*
	            * 5.打印分类结果
	            * 在这里我们打印了分类器的正确率
	            * 其它的一些信息我们可以通过Evaluation对象的其它方法得到
	            */
	           System.out.println( "分类器的正确率：" + (1- testingEvaluation.errorRate()
	        		   + "\n" + testingEvaluation.toSummaryString(true)
	        		   + "\n" + testingEvaluation.toMatrixString()
	        		   + "\nAUC=" + testingEvaluation.weightedAreaUnderROC()));
	       }catch(Exception e){
	           e.printStackTrace();
	       }
	    }
}
