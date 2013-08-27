package com.emar.recsys.user.model;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;

import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.functions.Logistic;
import weka.classifiers.trees.J48;
import weka.core.Instance;
import weka.core.Instances;
import weka.classifiers.functions.LibLINEAR;
import weka.classifiers.functions.LibSVM;


/**
 * 
 * Weka模型训练|预测|持久化存储和加载。
 * 
 */
public class LRPredict implements Serializable {

	private static final long serialVersionUID = 1L;

	private String TrainingDateFile;  //

	private Logistic lrModel;
	private Classifier model;
	private Instances inst;
	private Evaluation eval;

	public LRPredict() {
		lrModel = new Logistic();
		TrainingDateFile = "C:/Program Files/Weka-3-6/data/weather.nominal.arff";
	}
	public LRPredict(String trainInpath) {
		lrModel = new Logistic();
		TrainingDateFile = trainInpath;
	}
	public LRPredict(String[] ops) {
		lrModel = new Logistic();
		Setoption(ops);
	}
	
	public void EvaluationClassifier(String[] opts, String classifier) {
		try {
			model = (Classifier) Class.forName(classifier).newInstance();
			
			System.out.println( "classifier=" + classifier + 
					"\noptions:\n" + Arrays.asList(opts) + 
					"[Info] EvaluationClassifier()\n" +
					weka.classifiers.Evaluation.evaluateModel(
					this.model, opts));
			/**
			eval = new Evaluation(this.getInstances());
			eval.evaluateModel(classifier, this.getInstances());
			System.out.println("[Info] EvaluateWeka::crossValidationDetail \n"
					+ "AUC=" + eval.weightedAreaUnderROC() 
					+ "\tTotal Time Cost=" + eval.totalCost()
					+ eval.toClassDetailsString()
					+ "\n" + eval.toSummaryString()
					+ "\n" + eval.toMatrixString());
					*/
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public void EvaluationClassifier(String[] option) {
		try {
			System.out.println("[Info] EvaluationClassifier()\n" +
					weka.classifiers.Evaluation.evaluateModel(
					this.lrModel, option));
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public Instances getInstances() {
		return this.inst;
	}

	public boolean Setoption(String[] option) {

		if (option.length == 0) {
			return false;
		}
		try {
			lrModel.setOptions(option);
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

	}

	/**
	 * 
	 * @param AttributeIndex
	 *            -classIndex number
	 * @return true if sucesess,otherwise false
	 */
	public boolean Training(int AttributeIndex) {

		System.out.println("[Info] Training() Start training.....");
		try {
			inst = new Instances(new FileReader(this.TrainingDateFile));
			if (AttributeIndex < 0 || AttributeIndex > inst.numAttributes()) {
				return false;
			}
			inst.setClassIndex(AttributeIndex);
			lrModel.buildClassifier(inst);
			System.out.println("[Info] Training() build complete");
			return true;

		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

	}

	/**
	 * 
	 * @param ins
	 *            -the instance to be classified
	 * @return- -1 if take error,otherwise the predicted value
	 */
	public double predicted(String[] predictedInstance) {
		double predicted = 0;
		int attributeNum = inst.numAttributes();
		if (predictedInstance.length > attributeNum) {
			return -1;
		}
		Instance instance = new Instance(attributeNum);
		for (int i = 0; i < predictedInstance.length; i++) {
			instance.setValue(inst.attribute(i), predictedInstance[i]);
		}
		instance.setDataset(inst);
		System.out.println("[Info] predicted()测试样本为:" + instance);
		try {
			predicted = lrModel.classifyInstance(instance);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return predicted;

	}

	/**
	 * save the trained classifier to Disk
	 * 
	 * @param classifier
	 *            -the classifier to be saved
	 * @param modelname
	 *            -file name
	 */
	public void SaveModel(Object classifier, String modelname) {
		try {
			ObjectOutputStream oos = new ObjectOutputStream(
					new FileOutputStream(modelname));
			oos.writeObject(classifier);
			oos.flush();
			oos.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/**
	 * load the model from disk
	 * 
	 * @param file
	 *            -the model filename
	 * @return-the trained classifier
	 */
	public Object LoadModel(String file) {
		try {
			ObjectInputStream ois = new ObjectInputStream(new FileInputStream(
					file));
			Object classifier = ois.readObject();
			ois.close();
			return classifier;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		String[] ops1 = {
				"-t", "C:/Program Files/Weka-3-6/data/credit-g.arff",
				"-i", // "-x", "5", 
//				"-R", "1.0E-6", "-M", "-1",  // LR
//				"-I", "10", "-K", "0", "-S", "1",  // RandomForest
				"-K", "1", "-S", "0", "-N", "0", "-M", "1000", "-W", "1.0 1.0"
				};
		// 预测的输入
		String[] predictedData = { "sunny", "mild", "normal", "TRUE" };
		int pidx = 54; // 分类标签的指示
		String pmode = "E:/Data/pinyou/data/lr_test.model";
		
		/*
		 * //smo option String[] ops2 = {
		 * "-t","trainingdata/weather.nominal.arff",
		 * "-L", "0.0010", "-N", "1", "-V","10", //k-fold cross-validation "-K",
		 * "weka.classifiers.functions.supportVector.PolyKernel", "-W", "1" };
		 */

		LRPredict lrPredict = new LRPredict();

//		lrPredict.EvaluationClassifier(ops1);
//		lrPredict.EvaluationClassifier(args);
		System.out.println("[args] " + Arrays.asList(args));
//		ops1 = new String[args.length - 1];
//		System.arraycopy(args, 1, ops1, 0, args.length - 1);
//		lrPredict.EvaluationClassifier(ops1, args[0]);
		lrPredict.EvaluationClassifier(ops1, "weka.classifiers.functions.LibSVM");
		/*
		if (lrPredict.Setoption(ops1)) {
			if (lrPredict.Training(pidx)) {
				lrPredict.SaveModel(lrPredict, pmode);
			}
		}
		
		lrPredict = (LRPredict) lrPredict.LoadModel(pmode);
		double result = lrPredict.predicted(predictedData);
		System.out.println("是否适合运动 : "
						+ lrPredict.getInstances().classAttribute()
								.value((int) result));
		*/
	}
}