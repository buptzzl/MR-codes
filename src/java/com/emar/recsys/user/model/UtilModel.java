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

	

	/** Un-UT. 对adjustSexPredict 的包装 */
	public static double[] adjustSexPredict(double smale, double sfemale) {
		final double[] male = new double[2], female = new double[2];
		male[1] = smale;
		female[1] = sfemale;
		return adjustSexPredict(male, female);
	}
	
	/** UT done. 对三类别的性别预测分值修正:交换未知类别分值与 M|F中较大者的值   */
	public static double[] adjustSexPredict(double[] scores) {
		if (scores == null || scores.length < 3)
			return null;
		if (scores[2] <= scores[0] || scores[2] <= scores[1])
			return scores;
		
		final double sDiff = 0.2;
		final int sAmply = 10;  // M|F 分值差异比率
		int idxUpdate = 0;  // 默认调整为男性
		double stmp;
		if (scores[0] < scores[1])
			idxUpdate = 1;
		
		if (scores[2] < 0.5
				|| scores[1^idxUpdate] * sAmply < scores[idxUpdate]) {
			stmp = scores[2];
			scores[2] = scores[idxUpdate];
			scores[idxUpdate] = stmp;
		}

		return scores;
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
