package com.emar.recsys.user.action;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import weka.core.Utils;

import com.emar.recsys.user.demo.IKeywords;

/**
 * 抽取用户聚合日志后key 对应的内容， 结果输出为每个用户的字符串。
 * @author zhoulm
 * 
 * @TODO
 */
abstract public class ActionExtract {
	static private Logger log = Logger.getLogger(ActionExtract.class);

	private final static Set<String>  UKeys = new HashSet<String>(
			Arrays.asList(IKeywords.UserMergeAction));
	private String input, output;
	
	/** 解析 输出参数  */
	private void parseArg(String[] args) {
		final String[] HMSG = {"-h", "--help" };
		if (args == null || args[0].toLowerCase().equals(HMSG[0]) 
				|| args[0].toLowerCase().equals(HMSG[1])) {
			System.out.println("Usage: \tfor batchExtract\n-i input -o output\n");
			return;
		}
		
		try {
			input = Utils.getOption("-i", args);
			output = Utils.getOption("-o", args);
		} catch (Exception e) {
			log.error("message parse error. " + e.getMessage());
		}
		
	}
	/** 批处理过滤。  */
	public static void batchExtract(ActionExtract extractor, String[] args) {
		
	}
	/** 单独处理一个用户行为序列  */
	public static String singleExtract(ActionExtract extractor, String line) {
		
	}
	
	/** 自定义一个用户的输出格式. */
	public abstract String userFormat();
	
	/** 自定义过滤格式 */
	public abstract void userFilter();
	
//	/** 单个处理结果的格式化输出 */
//	public abstract String toString();
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
