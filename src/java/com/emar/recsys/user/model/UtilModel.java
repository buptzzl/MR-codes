package com.emar.recsys.user.model;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;

import weka.classifiers.Classifier;
import weka.core.Utils;

/**
 * 当前路径下的公用方法集合。
 * 
 * @author zhoulm
 * 
 */
public class UtilModel {

	/** 处理binary 分类结果的分值修正 */
	public static double[] adjustPredict(double[] scores) {
		// TODO
		if (scores == null)
			return null;

		return null;
	}

	/** 对adjustSexPredict 的包装 */
	public static double[] adjustSexPredict(double smale, double sfemale) {
		final double[] male = new double[2], female = new double[2];
		male[1] = smale;
		female[1] = sfemale;
		return adjustSexPredict(male, female);
	}

	/**
	 * 对比两个 binary 分类结果的分值[(male, unk), (female, unk)]， 得到修正后的分值(0:male,
	 * 1female, 0.5+unk)。
	 */
	public static double[] adjustSexPredict(double[] male, double[] female) {
		if (male == null || male.length != 2)
			return female != null && female.length != 2 ? female : null;
		if (female == null || female.length != 2)
			return male;

		double[] res = new double[2];
		if (male[1] > 0.5 || female[1] > 0.5) {
			if (male[1] >= female[1]) {
				res[0] = 0.5 + female[1];
				res[1] = 1 - res[0];
			} else {
				res[1] = 0.5 + male[1];
				res[0] = 1 - res[1];
			}
		}
		// 男性概率较强，或较弱时分值为女性的两倍及以上，更新 distribution.
		else if (2 * male[1] >= female[1]) {
			res[0] = 0.5 + (male[1] > 0.125 ? 0.25 : male[1] * 2); //
			res[1] = 1 - res[0];
		} else if (2 * female[1] >= male[1]) {
			res[1] = 0.5 + (female[1] > 0.125 ? 0.25 : female[1] * 2);
			res[1] = 1 - res[0];
		}

		return res;
	}

	public static void testAdjustSexPredict() {
		double[] m1 = new double[] { 0.1, 0.9 }, m2 = new double[] { 0.9, 0.1 }, f1 = new double[] {
				0.8, 0.2 }, f2 = new double[] { 0.06, 0.94 }, res;
		res = adjustSexPredict(m1, f1);
		System.out.println(Utils.arrayToString(res));
		res = adjustSexPredict(m1, f2);
		System.out.println(Utils.arrayToString(res));
		res = adjustSexPredict(m2, f2);
		System.out.println(Utils.arrayToString(res));
		res = adjustSexPredict(m2, f1);
		System.out.println(Utils.arrayToString(res));

	}

	public static void main(String[] args) {
		if (args.length < 1) {
			System.out.println("Usage: <model-path>");
			System.exit(1);
		}
		
		try {
			ObjectInputStream ois = new ObjectInputStream(new FileInputStream(args[0]));
			Classifier classifier2 = (Classifier) ois.readObject();
			ois.close();
			System.out.println("[classifier info] input=" + args[0]
					+ "\n"+ classifier2.toString());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
