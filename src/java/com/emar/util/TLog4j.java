package com.emar.util;

import org.apache.log4j.Logger;
import org.apache.log4j.*;

/**
 * log4j的测试
 * @author zhoulm 
 *
 */
public class TLog4j {
	/**
	 * 日志分等级：在该等级及以上的才记录。
	 * 默认继承最近的祖先的级别。包com.foo.bar中创建的Logger 会继承com.foo 中的存在日志；
	 * 没有则继承root 的级别（默认为 DEBUG）。
	 */
//	Logger.getRootLogger();
	static Logger logger = Logger.getLogger(TLog4j.class);  // 静态的全局日志记录器:
//	logger.setLevel((Level)Level.WARN);
	
	public static void main(String[] args) {
//		SimpleLayout layout = new SimpleLayout();
		
	}
	
}
