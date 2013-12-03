package com.emar.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.log4j.Logger;

import com.ecyrd.speed4j.log.Log;

/** 
 * 快速复制文件和目录<br>
 * 功能:<br>
 * 利用nio来快速复制文件<br>
 * 利用nio快速复制目录<br>
 * 所有的方法都包装了RuntimeException异常<br>
 * 如果要对异常进行处理可以捕获该异常<br>
 * 2006-6-9 <br>
 * @author csj <br>
 * @url http://bbs.csdn.net/topics/210070041
 */
public class FileOperator {
	private static Logger log = Logger.getLogger(FileOperator.class);

	public FileOperator(){
	}
	
	/**
	 * 返回 (Java 虚拟机中的空闲内存量)*(2/3) <br>
	 */
	private static long freeMemory(){
		return (long) ((Runtime.getRuntime().freeMemory()) * (2f/3f));
	}
	
	public static void copyFile(String srcFile, String destFile){
		copyFile(new File(srcFile),new File(destFile));
	}
	
	/**
	 * 功能:利用nio来快速复制文件
	 */
	public static void copyFile(File srcFile, File destFile){
		if(srcFile.length() < FileOperator.freeMemory()){
			FileOperator.copyBigFile(srcFile,destFile);
		}else{
			FileChannel srcFcin = null;
			FileChannel destFcout = null;
			
			try {			
				srcFcin = new FileInputStream(srcFile).getChannel();
				destFcout = new FileOutputStream(destFile).getChannel();
				
				srcFcin.transferTo(0, srcFcin.size(), destFcout);
			} catch (FileNotFoundException e) {
				throw new RuntimeException(e);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}finally{
				//关闭所有IO对象
				try {
					if(srcFcin!=null){
						srcFcin.close();
						srcFcin=null;
					}
					if(destFcout!=null){
						destFcout.close();
						destFcout=null;
					}
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
				srcFile=null;
				destFile=null;
			}
		}
	}
	
	/**
	 * 处理大型文件,可使用缓冲流来解决上一个方法出现的问题<br>
	 */
	private static void copyBigFile(String srcFile, String destFile){
		FileOperator.copyBigFile(new File(srcFile),new File(destFile));
	}
	/**
	 * 处理大型文件,可使用缓冲流来解决上一个方法出现的问题<br>
	 * 功能:<br>
	 * 利用nio来快速复制文件<br>
	 * @param srcFile 要移动的文件<br>
	 * @param destFile 移动后的文件<br>
	 * 2006-6-9 <br>
	 * 18:20:20 <br>
	 * @author csj <br>
	 */
	private static void copyBigFile(File srcFile, File destFile){
		
		FileChannel srcFcin = null;
		FileChannel destFcout = null;
		ByteBuffer buffer=null;
		
		try {
			srcFcin=new FileInputStream(srcFile).getChannel();
			destFcout=new FileOutputStream(destFile).getChannel();
			
			/*每次读取数据的缓存大小*/
			buffer=ByteBuffer.allocate((int)(FileOperator.freeMemory() * (2f/3f)));
			
			while(srcFcin.read(buffer) != -1){
				buffer.flip();
				destFcout.write(buffer);
				buffer.clear();
			}
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}finally{
			//关闭所有IO对象
			try {
				if(srcFcin!=null){
					srcFcin.close();
					srcFcin=null;
				}
				if(destFcout!=null){
					destFcout.close();
					destFcout=null;
				}
				if(buffer!=null){
					buffer.clear();
					buffer=null;
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}	
			srcFile=null;
			destFile=null;
		}
	}
	
	/**
	 * 功能: 利用nio来快速剪切文件<br>
	 */
	public static void cutFile(String srcFile, String destFile){
		FileOperator.cutFile(new File(srcFile),new File(destFile));
	}
	
	/**
	 * 功能: 利用nio来快速剪切文件<br>
	 */
	public static void cutFile(File srcFile, File destFile){
		FileOperator.copyFile(srcFile,destFile);
		srcFile.delete();
	}
	
	public static void copyDirectory(String srcDirectory, String destDirectory){	
		FileOperator.copyDirectory(new File(srcDirectory),new File(destDirectory));
	}
	/**
	 * 功能:  利用nio快速复制目录<br>
	 */
	public static void copyDirectory(File srcDirectory, File destDirectory){		
		
		/**
		 * 得到目录下的文件和目录数组
		 */
		//File srcDir = new File(srcDirectory);
		//File[] fileList = srcDir.listFiles();
		File[] fileList = srcDirectory.listFiles();
		/**
		 * 循环处理数组
		 */
		for (int i = 0; i < fileList.length; i++) {
			if (fileList[i].isFile()) {
				/**
				 * 数组中的对象为文件
				 * 如果目标目录不存在，
				 * 创建目标目录
				 */
				/*
				 * File descDir = new File(destDirectory);
				 * if (!descDir.exists()) {
				 *   descDir.mkdir();
				 * } 
				 */
				if (!destDirectory.exists()) {
					destDirectory.mkdir();
				} 
				
				/**
				 * 复制文件到目标目录
				 */
				//FileOperator.copyBigFile(srcDirectory + File.separatorChar + fileList[i].getName(),destDirectory + File.separatorChar + fileList[i].getName());
				FileOperator.copyBigFile(srcDirectory.getAbsolutePath() + File.separatorChar + fileList[i].getName(),destDirectory.getAbsolutePath() + File.separatorChar + fileList[i].getName());
				
			} else {
				/**
				 * 数组中的对象为目录
				 * 如果该子目录不存在就创建
				 * （其中也包含了对多级目录的处理）
				 */
				//File subDir = new File(destDirectory + File.separatorChar + fileList[i].getName());
				File subDir = new File(destDirectory.getAbsolutePath() + File.separatorChar + fileList[i].getName());
				if (!subDir.exists()) {
					subDir.mkdir();
				}
				/**
				 * 递归处理子目录
				 * 循环调用自己
				 */
				//FileOperator.copyDirectory(srcDirectory + File.separatorChar + fileList[i].getName(),destDirectory + File.separatorChar + fileList[i].getName());
				FileOperator.copyDirectory(new File(srcDirectory.getAbsolutePath() + File.separatorChar + fileList[i].getName()),new File(destDirectory.getAbsolutePath() + File.separatorChar + fileList[i].getName()));
			}
		}
		//srcDir = null;
		fileList = null;
	}
	
	/**
	 * 功能: 利用nio快速剪切目录<br>
	 */
	public static void cutDirectory(String srcDirectory, String destDirectory){
		FileOperator.cutDirectory(new File(srcDirectory),new File(destDirectory));
	}
	
	public static void cutDirectory(File srcDirectory, File destDirectory){
		FileOperator.copyDirectory(srcDirectory,destDirectory);
		srcDirectory.delete();
	}
	
	
}

