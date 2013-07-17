package com.emar.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import com.emar.util.Ip2AreaUDF.IpArea;

/**
 * 
 * @author Administrator
 * 重用常见 IO 操作
 */
public class HdfsIO {
	
	/**
	 * @param path
	 * @param c input as this.getClass()
	 * @return 读取 本地 or Global 文件的内容返回
	 */
	public static List<String> readFile(Class c, String path) {
		if(path == null) {
			return null;
		}
		ArrayList<String> lines = new ArrayList<String>();
		BufferedReader buf;
		String line;
		
		try {
			InputStream ins = c.getResourceAsStream(path);

			buf = new BufferedReader(new InputStreamReader(ins));
			while((line = buf.readLine()) != null) {
				lines.add(line);
			}
			buf.close();
			
		} catch (Exception e) {
			e.printStackTrace();
			lines.clear();
			// load Global file
			FSDataInputStream in = null;
			Configuration conf = new Configuration();
			FileSystem fs = null;
			
			try {
				fs = FileSystem.get(URI.create(path), conf);
				in = fs.open(new Path(path));
				while ((line = in.readLine()) != null) {
					lines.add(line);
				}
			} catch (IOException e1) {
				e1.printStackTrace();
			} finally {
				IOUtils.closeStream(in);
			}
		} 
		
		return lines;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		List<String> lines = HdfsIO.readFile(HdfsIO.class, "resourse/TB_level1.txt");
		System.out.println("[test] \n" 
				+ "size: " + lines.size()
				+ "\nclass-load: " + HdfsIO.class.getResource("resourse/TB_level1.txt")
				+ "\ncload2： " + HdfsIO.class.getResource("/com/emar/util/resourse/TB_level1.txt")
				+ "\ncload3: " + Ip2AreaUDF.class.getResource("resourse/ip_dstc_ne.dat")
				);
	}

}
