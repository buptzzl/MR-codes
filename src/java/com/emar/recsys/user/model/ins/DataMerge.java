package com.emar.recsys.user.model.ins;

import org.apache.log4j.Logger;

import com.emar.util.ConfigureTool;

/**
 * 合并多个 唯一类别的样本文件为一个多类别arff文件：基于DataNormalize
 * @author zhoulm
 *
 * @TODO
 * 基于配置文件learn.conf， 每个 DataNormalize都有参数： in, IAttrParse; 并有唯一的输出路径out.
 */
public class DataMerge {
	private static Logger log = Logger.getLogger(DataMerge.class);
	/** 配置文件中的字段名 */
	private static final String PATH_CONF = "learn.conf", PATH_DEF_OUT = "merge.arff", 
			KEY_IN = "ins.merge.inputs",	KEY_PARSE = "ins.merge.parsers",
			KEY_OUT = "ins.merge.output";
	
	private ConfigureTool configure;
	private String[] inputs;
	/** 各样本类别文件对应的解析类，如果不够默认使用第1个解析  */
	private String[] inparsers;
	private String output;
	private DataNormalize[] insset;  // 多个文件解析类
//	private DataNormalize insmerge;  // 唯一的文件合并类 => 默认用第1个非null对象执行合并
	private int firstUnnull;
	
	public DataMerge() {
		configure = new ConfigureTool();
		configure.addResource(PATH_CONF);
		inputs = configure.getStrings(KEY_IN);
		inparsers = configure.getStrings(KEY_PARSE);
		output = configure.get(KEY_OUT, PATH_DEF_OUT);
		insset = new DataNormalize[inputs.length];
		firstUnnull = inputs.length;
//		insmerge = null;
		
		assert inputs.length >= inparsers.length;
	}
	
	private int loadSet() {
		// 加载各个类别的待解析文件
		int cnt = 0;
		int j = 0;
		for (int i = 0; i < inputs.length; ++i) {
			j = i;
			if (inparsers.length <= i) 
				j = 0;
			try {
				insset[i] = new DataNormalize("", inputs[i], inparsers[j]);
				insset[i].init(false);
				if (i < firstUnnull)
					firstUnnull = i;
			} catch (Exception e) {
				log.error("fail to initial object, param=[output:\"\", input:" 
						+ inputs[i] + ", parser:" + inparsers[j]
						+ ".\t[MSG]: " + e.getMessage());
				continue;
			}
		}
		// 合并所有特征
		for (int i = firstUnnull + 1; i < insset.length; ++i) {
			if (insset[i] != null) {
				for (String fi : insset[i].getFeatures()) {
					insset[firstUnnull].updateFeature(fi);
				}
			}
		}
		return 0;
	}
	
	private boolean mergeOne(DataNormalize di) {
		// 合并一个实例到最终的结果
	}
	
	public void process() {
		// 执行最终的解析结果
		
	}
	

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
