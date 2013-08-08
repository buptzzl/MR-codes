package com.emar.recsys.user.model;

/**
 * 基于weka 工具包的模型evaluation.
 * @author zhoulm
 *
 */

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.Random;

import sun.security.pkcs11.Secmod.ModuleType;

import weka.classifiers.*;
import weka.classifiers.trees.J48;
import weka.core.Instances;

public class EvaluteWeka {
	private Instances ins;
	private Classifier classifier;
	public Evaluation eval;
	private String modelClass;
	private String in, outModel, outEva;
	private int NCross, ClassIdx;
	
	public EvaluteWeka(String in) throws Exception {
		this.in = in;
		this.outEva = "./J48.evaluate";
		this.outModel = "./J48.model";
		this.modelClass = "weka.classifiers.trees.J48";
		this.ClassIdx = 0;
		this.NCross = 5;
		this.classifier = (Classifier) Class.forName(modelClass).newInstance();
		assert this.getData();
		this.eval = new Evaluation(ins);
	}

	public EvaluteWeka(String outeva, String outmodel, String in, String model, int cidx, int Ncs) throws Exception {
		this.in = in;
		this.outEva = outeva;
		this.outModel = outmodel;
		this.modelClass = model;
		this.ClassIdx = cidx;
		this.NCross = Ncs;
		this.classifier = (Classifier) Class.forName(modelClass).newInstance();
		assert this.getData();
		this.eval = new Evaluation(ins);
	}

	private boolean getData() {
		try {
			FileReader fr = new FileReader(in);
			ins = new Instances(fr);
			ins.setClassIndex(this.ClassIdx);
			return true;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}
	
	public void setOpinion(String... opi) throws Exception {
		classifier.setOptions(opi);
	}
	
	public void crossValidation() throws Exception {
		eval.evaluateModel(classifier, ins);
		System.out.println("[Info] EvaluateWeka::crossValidationDetail \n"
				+ "AUC=" + eval.weightedAreaUnderROC() 
				+ "\tTotal Time Cost=" + eval.totalCost()
				+ eval.toClassDetailsString()
				+ "\n" + eval.toSummaryString()
				+ "\n" + eval.toMatrixString());
		/// write out.
//		FileOutputStream fos = new FileOutputStream(new File(this.outEva));
		
	}

	public static void main(String[] args) {

		System.out
				.println("Usage: <out-evaluate> <out-model> <in-data> <class-idx> <model> [num-cross-folder] ");

	}

}
