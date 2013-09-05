package com.emar.recsys.user.demo;

import java.util.Set;

/**
 * 属性通用接口:
 * 1. 定义统一的 Json keyword。
 * 2. TODO
 * @author zhoulm
 *
 */

public interface IKeywords {

//	public static final Set<String> mset = null;
//	public static final Set<String> fset = null;
	
	public static final String KUid="Uid";
	
	/** 订单日志中的 key */
	public static final String NGood = "NGood";  // goods总数
	public static final String RawLog = "RawLog"; 
	public static final String SSum = "SSum";
	public static final String SPos = "SPos";  // female's score
	public static final String SReduce = "SRed";  // fscore-mscore.
	public static final String IScore = "IScore";
	
	/** 模型学习结果 */
	public static final String KPar = "Predict";  // prediction res.
	public static final String PSex = "Sex";  // sex's predict res.
	public static final String SSexDist = "SexDist";  // sex distribution. 
	
}
