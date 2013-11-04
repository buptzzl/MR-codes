package com.emar.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * log4j的测试
 * @author zhoulm 
 * TODO 基于日期分割文件的功能。
 *  
 */
public class TLog4j {
	
	static {
		String filePath = "libs/log4j.properties";
		Properties props = new Properties(); 
        System.out.println("[Test]\t" + filePath);
//        System.err.println("[err]" + filePath);
        Logger logger = Logger.getLogger(TLog4j.class);  
        
        try { 
            FileInputStream log4jStream = new FileInputStream(filePath); 
            props.load(log4jStream); 
            log4jStream.close(); 
            String logFile = props.getProperty("log4j.appender.A1.File"); //设置路径 

            System.out.println(logFile); 
//            props.setProperty("log4j.appender.A1.File", logFile); 
//            PropertyConfigurator.configure(props); //装入log4j配置信息
//            PropertyConfigurator.configure(filePath);
        } catch (IOException e) { 
            e.printStackTrace(); 
        } 
        logger.info("Initializing, end My Init"); 
	}
	
	public static void main(String[] args) {
        Logger logger = Logger.getLogger(TLog4j.class);  

		logger.debug("[deb]");
		logger.info("info leave!");  
        logger.warn("warn leave!");  
		logger.error("[err]");
	}
	
}
