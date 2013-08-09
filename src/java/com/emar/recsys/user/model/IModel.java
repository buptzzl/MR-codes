package com.emar.recsys.user.model;

/**
 * model 包中的通用接口
 * @author Administrator
 *
 */
public interface IModel {

	/**
	 * 解析一行数据， 抽取对应的字段用于特征规范化
	 * @param line
	 * @return 任意的类型
	 */
	public abstract Object parseLine(String line);
	
}
