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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.emar.recsys.user.util.DateParse;
import com.emar.util.Ip2AreaUDF.IpArea;

/**
 * 
 * @author zhou 重用常见 IO 操作
 */
public class HdfsIO {

	/**
	 * @param path
	 * @param c
	 *            input as this.getClass()
	 * @return 读取 本地 or Global 文件的内容返回
	 */
	public static List<String> readFile(Class c, String path) {
		if (path == null) {
			return null;
		}
		ArrayList<String> lines = new ArrayList<String>();
		BufferedReader buf;
		String line;

		try {
			InputStream ins = c.getResourceAsStream(path);

			buf = new BufferedReader(new InputStreamReader(ins));
			while ((line = buf.readLine()) != null) {
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

	public static void printInput(Job job) {
		int fcnt = 0;
		Path[] input_tot = FileInputFormat.getInputPaths(job);
		for (Path p : input_tot) {
			FileSystem fsystem;
			try {
				fsystem = p.getFileSystem(job.getConfiguration());
				if (fsystem.isFile(p)) {
					System.out.println("[Info] inPath:" + p.toString());
				} else {
					FileStatus[] input_fs = fsystem.globStatus(p);
					for (FileStatus ps : input_fs) {
						fcnt += 1;
						System.out.println("[Info] inPath:" + ps.getPath());
					}
				}
			} catch (IOException e) {
			}
		}
		System.out.println("[Info] total-input-file=" + fcnt);
		return;
	}

	public static boolean setInput(String range, String timefmt, String fmt,
			Job job) {
		String[] datapath = DateParse.getRange(range, timefmt);
		String fpath;
		int fcnt = 0;
		try {
			for (String s : datapath) {
				fpath = String.format(fmt, s);
				FileInputFormat.addInputPath(job, new Path(fpath));
			}
			printInput(job);
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String s = "resource/classify/firstcate";
		List<String> lines = HdfsIO.readFile(HdfsIO.class, s);
		System.out.println("[test] \n" + "size: " + lines.size()
				+ "\nclass-load: " + HdfsIO.class.getResource(s) + "\ncload2： "
				+ HdfsIO.class.getResource("/com/emar/util/" + s)
				+ "\ncload3: "
				+ Ip2AreaUDF.class.getResource("resource/ip_dstc_ne.dat"));

		try {
			String range = "2013061800_2013061900", tfmt = "yyyyMMdd", fmt = "/data/stg/s_order_log/%s/1/*.dat";
			HdfsIO.setInput(range, tfmt, fmt, new Job());
			System.out.println("from CMD");
			HdfsIO.setInput(args[0], args[1], args[2], new Job());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
