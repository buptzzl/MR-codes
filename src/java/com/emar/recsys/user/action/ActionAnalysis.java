package com.emar.recsys.user.action;

import java.util.HashMap;
import java.util.Map;

/**
 * 通用用户行为 统计分析类。
 * @author zhoulm
 * TODO 
 */
public class ActionAnalysis extends ActionCombiner {
	/** 基于K-V 存储的统计数据  */
	private Map<String, Integer>[] containers;
	private int dimension; // 统计的维度
	
	private void initAna () {
		// TODO
	}
	
	/** 基于单个用户的统计分析  */
	private int singleAnalysis(int index) {
		// TODO
		return 0;
	}
	
	private boolean updateUrl(int index) {
		// 统计个Host 的分布
		if (userAction == null || userAction.length() == 0)
			return false;
		
		return false;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
