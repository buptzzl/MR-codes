package com.emar.recsys.user.model.ins;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import org.apache.log4j.Logger;

import com.emar.util.FileOperator;

import weka.core.Attribute;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.SparseInstance;
import weka.core.converters.ArffLoader;
import weka.core.converters.ArffSaver;

/**
 * 一些通用的实例操作程序。
 * @author zhoulm
 *
 */
public class UtilInstances {
	private static Logger log = Logger.getLogger(UtilInstances.class);
	private static String DEF_BAK = ".bak~";

	Instances train;
	
	/** 加载本地的Arff 文件。  */
	public static Instances loadArff(String trainFile) throws IOException {
		if (trainFile == null)
			return null;
		Instances trainIns;
		File file= new File(trainFile);
        ArffLoader loader = new ArffLoader();
        loader.setFile(file);
        trainIns = loader.getDataSet();
        
        log.debug("load total arff file.");
        return trainIns;
	}
	
	public static Instances loadArffStruct(String trainFile) throws IOException {
		if (trainFile == null)
			return null;
		Instances ins;
		File file= new File(trainFile);
        ArffLoader loader = new ArffLoader();
        loader.setFile(file);
        ins = loader.getStructure();
        
        log.debug("load arff head.");
        return ins;
	}
	
	public static boolean saveArff(String file, Instances ins) throws IOException {
		if (file == null || ins == null) {
			return false;
		}
		
		ArffSaver sarff = new ArffSaver();
		sarff.setInstances(ins);
		sarff.setFile(new File(file));
		sarff.writeBatch();
		log.debug("save arff size: " + ins.numInstances());
		return true;
	}
	
	/** 合并多个同任务不同类别标签的 Arff 文件； 基于DataMerge.java 的配置；新增的属性替换样本中未值为0；     */
	public static boolean mergeMultiArffIns(String... files) throws IOException {
		if (files == null || files.length < 2)
			return false;
//		File fi;
		FileOperator.copyFile(files[0], files[0] + DEF_BAK);
		Instances[] insArr = new Instances[files.length];
		insArr[0] = loadArff(files[0]);
		for (int i = 1; i < files.length; ++i)
			insArr[i] = loadArffStruct(files[i]);
		Attribute ai = null;
		Enumeration enu = null, enuIns = null;
		Instance ins = null, itmp = null;
		
		List<Attribute> newAtts = new ArrayList<Attribute>();
		// 添加全部 属性
		for (int i = 1; i < insArr.length; ++i) {
			enu = insArr[i].enumerateAttributes();
			while (enu.hasMoreElements()) {
				ai = (Attribute) enu.nextElement();
				try {
					insArr[0].insertAttributeAt(ai, insArr[0].numAttributes());
					newAtts.add(ai);
				} catch (IllegalArgumentException e) {
					log.warn(e.getMessage());
					continue;
				}
			}
		}
		// repalce missing with 0.
		
		enuIns = insArr[0].enumerateInstances();
		while(enuIns.hasMoreElements()) {
			ins = (Instance) enuIns.nextElement();
			for (Attribute ati : newAtts)
				ins.setValue(ati, 0);
		}
//		Instances itmp = null;
		// 添加实例
		for (int i = 1; i < insArr.length; ++i) {
			ins = new SparseInstance(0);
			ins.setDataset(insArr[0]);
			insArr[i] = loadArff(files[i]);
			enuIns = insArr[i].enumerateInstances();
			while (enuIns.hasMoreElements()) { // 遍历实例
				itmp = (Instance)enuIns.nextElement();
				enu = itmp.enumerateAttributes();
				while(enu.hasMoreElements()) { // 遍历属性
					ai = (Attribute) enu.nextElement();
					ins.setValue(insArr[0].attribute(ai.name()), itmp.value(ai));
				}
				insArr[0].add(itmp);
			}
		}
		
//		 ReplaceMissingValues: 
		saveArff(files[0], insArr[0]);
		return true;
	}
	
	/** 按比例抽取样本。 numFolder 指抽取其中的第from-to 份数据，剩余的数据的存储文件名增加后缀_r.arff.*/
	public static boolean sampleArff(String arff, String save,
			int foldBeg, int foldEnd, int Folders) throws IOException {
		Instances data;
		data = loadArff(arff);
		Instances dchoice = data.testCV(Folders, foldBeg), dtmp = null;
		for (int i = (foldBeg + 1); i < foldEnd; ++i) {
			dtmp = data.testCV(Folders, i);
			for (int j = 0; j < dtmp.numInstances(); ++j) {
				dchoice.add(dtmp.instance(j));
			}
		}
		saveArff(save, dchoice);
		// 保留剩余的数据
		Instances drest = new Instances(data, data.numInstances() - dchoice.numInstances());
		for (int i = 0; i < foldBeg; ++i) {
			dtmp = data.testCV(Folders, i);
			for (int j = 0; j < dtmp.numInstances(); ++j) {
				drest.add(dtmp.instance(j));
			}
		}
		for (int i = foldEnd; i < Folders; ++i) {
			dtmp = data.testCV(Folders, i);
			for (int j = 0; j < dtmp.numInstances(); ++j) {
				drest.add(dtmp.instance(j));
			}
		}
		saveArff(save+"_r.arff", drest);
		log.info("input: " + arff + ", output:" + save 
				+ "total data: " + data.numInstances() + ", choice data: " 
				+ dchoice.numInstances() + ", rest data: " + drest.numInstances());
		return true;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String[] arffs = {
				"D:/Data/MR-codes/data/test/merge_.arff", 
				"D:/Data/MR-codes/data/test/merge_template.arff"
		};
		try {
//			boolean bmerge = mergeMultiArffIns(arffs);
//			boolean fchoice = sampleArff(arffs[0], arffs[0]+".sample", 0, 3, 5);
			sampleArff(args[0], args[1], Integer.parseInt(args[2]), 
					Integer.parseInt(args[3]), Integer.parseInt(args[4]));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
