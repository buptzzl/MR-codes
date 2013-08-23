package com.emar.recsys.user.model;

/**
 * model 包中的属性解析通用接口，注意： 所有实现必须定义无参的默认构造函数。
 * @author zhoulm
 *
 */
public interface IAttrParser {
	
	/** 自定义 初始化操作。 在使用前必须先调用该方法。 */
	public abstract boolean init(String... args) ;
	
	/**  分类|回归字段  等关键属性 的 attribute */
	public abstract String getAttribute() ;
	/**  设置分类|回归字段 等关键属性 的 attribute */
	public abstract void setAttribute(String s) ;

	/**
	 * 解析一行数据， 抽取对应的字段用于特征规范化
	 * @param line
	 * @return 任意的类型
	 */
	public abstract Object parseLine(String line);
	
	/**
	 * 将解析的结果中 分类|回归 的值的字符串
	 * @return
	 */
	public abstract Object getClassify();
	
	/**
	 * 解析结果中 的特征值向量
	 * @return
	 */
	public abstract Object[] getFeatures();
	
	/** 方便测试 */
	public abstract String toString();
	
}
