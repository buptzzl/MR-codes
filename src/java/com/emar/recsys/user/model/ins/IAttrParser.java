package com.emar.recsys.user.model.ins;

/**
 * model训练前的实例生成接口，注意： 所有实现必须定义无参的默认构造函数。
 * @author zhoulm
 * @tips 非线程安全
 */
public interface IAttrParser {
	
	/** 自定义 初始化操作。 在使用前必须先调用该方法。 */
	public abstract boolean init(String... args) ;
	
	/**  分类|回归字段  等关键属性 的 attribute */
	public abstract String getAttribute() ;
	/**  设置分类|回归字段 等关键属性 的 attribute */
	public abstract void setAttribute(String s) ;

	/**
	 * 解析一行数据， 抽取对应的字段用于特征规范化。将NaN 转换为0. 
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
	
	/**
	 * 解析结果中 实例的权重
	 */
	public abstract Object getWeight() ;
	
	public static int[] CntClass = new int[100];
	/**
	 * 对分类数据等， 解析不同的类别到index.
	 */
	public abstract int getClassIndex() ;
	/**
	 * 统计分类等的数据，
	 * @param idx 计数器CntClass的索引
	 */
	public abstract void countClass(int idx) ;
	/**
	 * 打印统计数据。  建议：数据处理完后调用该方法。
	 */
	public abstract String getStaticInfo();
	
	/** 方便测试 */
	public abstract String toString();
	
}
