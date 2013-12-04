package com.emar.recsys.user.action;

import java.util.HashMap;
import java.util.Map;

/**
 * 通用用户行为 统计分析类。
 * @author zhoulm
 *
 */
public class ActionAnalysis extends ActionCombiner {
	/** 基于K-V 存储的统计数据  */
	private Map<String, Integer>[] containers;
	private int dimension; // 统计的维度
	
	private void initAna () {
		
		 = new HashMap<String, Integer>(1024, 0.95f);
	}
	
	/** 基于单个用户的统计分析  */
	private int singleAnalysis(int index) {
		// TODO
	}
	
	private boolean updateUrl(int index) {
		// 统计个Host 的分布
		if (userAction == null || userAction.length() == 0)
			return false;
		
		
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
