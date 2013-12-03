package com.emar.recsys.user.model.ins;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.emar.util.ConfigureTool;

/**
 * 合并多个 唯一类别raw-vector样本文件为一个多类别的arff文件：基于DataNormalize
 * 要求类别属性的名称相同、值不同。
 * 配置字段支持设置各个IAttrParser 的配置文件路径。
 * @author zhoulm
 * 
 * @TODO UT
 * @deprecated 建议使用两步法： S.1 DataNormalize 生成若干arff； S.2 ArffMerge 合并为1个arff.
 */
public class DataMerge {
	private static Logger log = Logger.getLogger(DataMerge.class);
	/** 配置文件中的字段名 */
	private static final String PATH_CONF = "learn.conf", PATH_DEF_OUT = "merge.arff", 
			KEY_IN = "ins.merge.inputs",	KEY_PARSE = "ins.merge.parsers",
			KEY_OUT = "ins.merge.output", KEY_CLA_ATTR = "ins.merge.class",
			KEY_CLA_VAL = "ins.merge.classlabel", KEY_PAR_PARAM = "ins.merge.param",
			SEP_ARFF = ", ";
	
	private ConfigureTool configure;
	private String[] inputs;
	/** 各样本类别文件对应的解析类，如果不够默认使用第1个解析  */
	private String[] inparsers;
	private IAttrParser[] parsers;
	/** 各文件对应的类别标签；解决公用相同的解析类的标签问题 */
	private String[] classLabel;
	private String output;
//	private DataNormalize[] insset;  // 多个文件解析类=> 
//	private DataNormalize insmerge;  // 唯一的文件合并类 => 默认用第1个非null对象执行合并
	private DataNormalize insmerge;
	/** 第1个未被处理的路径下标 */
	private int unuseIndex;
	/** 统一的类别属性名称 */
	private String classAttribute;
	
	public String toString() {
		return String.format("DataMerge:[configure: %s, input paths: %s, parsers: %s, "
				+ "class labels: %s, output path: %s]", configure, 
				Arrays.asList(inputs), Arrays.asList(inparsers), 
				Arrays.asList(classLabel),output);
	}
	
	public DataMerge() throws Exception {
		configure = new ConfigureTool();
		configure.addResource(PATH_CONF);
		this.init();
	}
	/** 指定配置文件  */
	public DataMerge(String arg) throws Exception {
		configure = new ConfigureTool();
		configure.addResource(arg);
		this.init();
	}
	
	private void init() throws Exception {
		inputs = configure.getStrings(KEY_IN);
		inparsers = configure.getStrings(KEY_PARSE);
		String[] parserParas = configure.getStrings(KEY_PAR_PARAM);
		parsers = new IAttrParser[inputs.length];
		Class<? extends IAttrParser> iclass = null;
		Constructor<? extends IAttrParser> iconstructor = null;
		for (int i = 0; i < inputs.length; ++i) {
			iclass = Class.forName(inparsers[i]).asSubclass(IAttrParser.class);
			if (parserParas != null) {
				iconstructor = iclass.getConstructor(String.class);
				parsers[i] = iconstructor.newInstance(parserParas[i]);
			} else {
				iconstructor = iclass.getConstructor();
				parsers[i] = iconstructor.newInstance();
			}
		}
		classLabel = configure.getStrings(KEY_CLA_VAL);
		
		output = configure.get(KEY_OUT, PATH_DEF_OUT);
		classAttribute = configure.get(KEY_CLA_ATTR);
		insmerge = new DataNormalize(output, inputs[0], inparsers[0]);
		insmerge.init(false);
		
		unuseIndex = 1; 
		log.info("build object, prepare to parse file.");
	}
	
	/**  解析全部文件的集合， 返回解析失败的文件数。   */
	public int process() throws IOException {
		int ErrFea = 0, ErrVect = 0;
		ErrFea = this.loadFeatures();
		boolean succ = false;
		
		BufferedWriter wBuf = new BufferedWriter(new FileWriter(output));
		for (int i = 0; i < inputs.length; ++i) {
			succ = parseFile(i, wBuf);
			if (!succ) {
				++ErrVect;
				log.error("unparsed file: " + this.inputs[i]);
			}
		}
		wBuf.close();
		
		log.info("merge " + inputs.length + " file.");
		return ErrFea < ErrVect ? ErrVect : ErrFea;
	}
	
	/** 解析第index个文件为训练数据    */
	private boolean parseFile(int index, BufferedWriter wBuf) throws IOException {
		String vector = null;
		int Nempty = 0;
		BufferedReader rBuf = new BufferedReader(new FileReader(inputs[index]));
		for (String line; (line = rBuf.readLine()) != null; ) {
			vector = this.parseLine(line, index);
			if (vector.length() != 0) {
				wBuf.write(vector);
				wBuf.newLine();
			} else {
				++Nempty;
			}
		}
		rBuf.close();
		log.warn("parse file: " + inputs[index] + ", emtpy line: " + Nempty);
		return true;
	}
	
	/**  基于IAttrParser 解析第index个文件中的一个样本为稀疏向量。 解析失败时返回“”  */
	public String parseLine(String line, int index) {
		if (line == null) {
			return "";
		}
		IAttrParser Parser = parsers[index];
		StringBuffer dataBuf = new StringBuffer();
		Integer tmp;
		Set<Integer> arrSet = new HashSet<Integer>();
		
		dataBuf.delete(0, dataBuf.length());
		dataBuf.append("{");

		// 基于新的解析方式。
		if(Parser.parseLine(line) == null)
			return "";
		Object[] fs = Parser.getFeatures();
		if (fs == null || fs.length == 0)
			return "";
		for(Object s: fs) {
			tmp = insmerge.getFeature((String)s);
			if(tmp != null) {
				arrSet.add(tmp);
			} else {
				insmerge.updateFeature((String)s);
				log.warn("add feature: " + s);
			}
		}
			
		List<Integer> arr = Arrays.asList(arrSet.toArray(new Integer[0]));
		Collections.sort(arr);  // asc
		for(Integer t: arr) {  // 各特征值
			dataBuf.append(String.format("%d 1%s", t, SEP_ARFF));
		}
		// 类别标签
		dataBuf.append(String.format("%d %s", insmerge.getFeature(classAttribute), 
				classLabel[index]));
		dataBuf.append("}");
		// 添加实例的权重信息
		Float weight = (Float)Parser.getWeight();
		if(weight != null) 
			dataBuf.append(String.format(",  { %.4f }", weight));
		log.info("change to raw line to vector: " + dataBuf);
		return dataBuf.toString();
	}
	
	private int loadFeatures() {
		// 加载各个类别的待解析文件, 返回加载失败的文件数
		int cnt = 0;
		int j = 0;
		DataNormalize tParser = null;
		for (int i = unuseIndex; i < inputs.length; ++i) {
			j = i;
			if (inparsers.length <= i) {
				j = 0;  // 可能解析类的数量不够
			}
			try {
				tParser = new DataNormalize("", inputs[i], inparsers[j]);
				tParser.init(false);
				for (String fi : tParser.getFeatures()) { // 合并每个文件的raw特征
					insmerge.updateFeature(fi);
				}
			} catch (Exception e) {
				++cnt;
				log.error("fail to initial object, param=[output:\"\", input:" 
						+ inputs[i] + ", parser:" + inparsers[j]
						+ ".\t[MSG]: " + e.getMessage());
				continue;
			}
		}
		log.info("add all feature words, size: " + insmerge.getFeatureSize());
		return cnt;
	}
	

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
