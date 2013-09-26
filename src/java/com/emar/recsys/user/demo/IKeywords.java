package com.emar.recsys.user.demo;

import java.util.Set;

/**
 * 属性通用接口:
 * 1. 定义统一的 Json keyword。要求全部用大写，对已经使用大小写的历史KEY保留原样
 * 2. TODO
 * @author zhoulm
 *
 */

public interface IKeywords {

//	public static final Set<String> mset = null;
//	public static final Set<String> fset = null;
	
	public static final String Root="ROOT";  // 所有日志的根KEY
	
	public static final String KUid="Uid";
	
	/** user profile result. */
	public static final String UID_CONSUMER = "CONSUMER"; // 消费
	public static final String UID_CONS_W = "CONSUMER_WEIGHT"; 
	
	/** 订单日志中的 key */
	public static final String NGood = "Ngood";  // goods总数
	public static final String RawLog = "RawLog"; 
	public static final String SSum = "SSum"; // N_female+N_male
	public static final String SPos = "SPos";  // N_female
	public static final String SReduce = "SRed";  // fscore-mscore.
	public static final String IScore = "IScore";
	
	/** 模型学习结果 */
	public static final String KPar = "PREDICT";  // prediction res.
	public static final String PSex = "SEX";  // sex's predict res.
	public static final String SSexDist = "SEX_DIST";  // sex distribution. 
	
}
