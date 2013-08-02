package com.emar.recsys.user.zonerank;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.PiEstimator.PiReducer;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.emar.recsys.user.util.UtilStr;

/**
 * 过滤HDFS上的多个数据，产出最终结果
 * @author Administrator
 *
 */
public class GFilter {
	
	private Set<String> pathMid; // 中间文件集合
	private Set<String> keyset;  // 基于KEY去重
	
	public String outpath;
	public String inpath;
	
	public GFilter(String ipath, String opath) throws IOException {
		inpath = ipath;
		outpath = opath;
		this.init();
		this.listFile(this.inpath);
//		this.reload();
	}
	/**
	 * @func UnitTest
	 */
	protected GFilter() {
		this.init();
	}

	private void init() {
		pathMid = new HashSet<String>();
		keyset = new HashSet<String>(1<<10, 0.9f);
	}
	
	
	
	public boolean listFile(String inpath) throws IOException {
		if(inpath == null) 
			return false;
		int fsz = this.pathMid.size();
		Configuration conf = new Configuration();
//		conf.set("mapred.child.java.opts", "-Xmx10240m");
		Path cpath = new Path(inpath);
		FileSystem cfs = FileSystem.get(conf);
		FileStatus[] cfstat = cfs.globStatus(cpath);
		for(FileStatus cfsi: cfstat) {
			this.pathMid.add(cfsi.getPath().toString());
//			in = cfs.open(cfsi.getPath());
//			while ((line = in.readLine()) != null) {
				// 
//			}
//			in.close();
		}
		System.out.println("[Info] GFilter::listFile() add-infile=" 
				+ (this.pathMid.size() - fsz));
		return (this.pathMid.size() != 0);
	}
	
	public boolean reload() throws IOException	{
		FSDataInputStream in = null;
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		String line;
		String[] atom;
		int cnt = 0, cntr = 0;
		
		FileWriter out = new FileWriter(new File(outpath));
		for(String s: this.pathMid) {
			in = fs.open(new Path(s));
			while((line = in.readLine()) != null) {
				cntr += 1;
				if(this.filter(line)) {
					out.write(line.trim() + "\n");
					cnt += 1;
				}
			}
			// @test
			System.out.println("[Info] GFilter::reload() cntr=" + cntr);
			out.flush();
			in.close();
		}
		out.close();
		System.out.println("[Info] GFilter::reload() read-size=" 
				+ cntr + "\twrite-size=" + cnt);
		return true;
	}
	
	private boolean filter(String line) {
		if(line == null || line.length() == 0) 
			return false;
		// 检测每条数据
		if(!this.checkFmt(line)) 
			return false;
		
		return true;
	}
	
	/**
	 * 按格式
	 * @param line
	 * @return
	 */
	private boolean checkFmt(String line) {
		// 检测数据格式
		boolean res = true;
		String[] atom = line.split("\t");
		if(atom.length < 2) 
			res = false;
		if(res)
			res = this.checkKey(atom[0]) && this.checkFmtValue(atom[1]);
		if(res) {
			// 检测全部通过, 更新全局记录
			this.keyset.add(atom[0]);
		}
		
		return res;
	}
	private boolean checkKey(String key) {
		boolean res = true;
		// 首先检测格式
		res = this.checkFmtKey(key);  
		if(res) 
			res = !this.keyset.contains(key);  // 去重
//		if(res) 
//			this.keyset.add(key);
		
		return res;
	}
	private boolean checkFmtKey(String key) {
		// 检测数据KEY的格式
		final String SEPA = "@@@";
		boolean res = true;
		int cnt = UtilStr.SubStrCnt(key, SEPA);
		if(cnt != 2) 
			res = false;
//		String[] atom = key.split(SEPA);
		
		return res;
	}
	private boolean checkFmtValue(String val) {
		// 检测 value 的格式
		boolean res = true;
		if(!val.startsWith("[")) 
			res = false;
		if(res) {
			int pos = val.indexOf(']');
			if(pos == -1) 
				res = false;
			if(res) {
				String rank = val.substring(1, pos); // 无中括号[]
				res = this.checkFmtRank(rank);
			}
		}
		
		return res;
	}
	private boolean checkFmtRank(String val) {
		// 检测 value 中 rank 部分的格式
		boolean res = true;
		String[] atom = val.trim().split(", ");
		int pw = -1;
		for(int i = 0; i < atom.length && res; ++i) {
			pw = atom[i].indexOf('=');
			if(pw == -1) {
				res = false;
			} else {
				try { 
					// value 字段必须为Float, 可有空白字符
					Float.parseFloat(atom[i].substring(pw+1).trim());
				} catch (NumberFormatException e) {
					res = false;
				}
			}
		}
		
		return res;
	}
	
	public static void test(String[] args) {
		// TODO Auto-generated method stub
		String[] lines = new String[] {
			"plat@@@tg2IyK0QoCoTT2QCTT2T1ZHfpULBDBEV@@@chuchuang\t[50010511=1.0, ",
			"plat@@@tg2IyK0QoCoTT2QCTT2T1ZHfpULBDBEV@@@chuchuang\t[50010511=1.0, 50015757=1.0]",
			"plat@@@tg2IyK0QoCoTT2QCTT2T1@@@chuchuang\t[50010511=1., 50015757=1]",
			"emar@@@1373156401204943128035@@@yigao\t[50013085=1.0]\t[source=211.143.198.95]",
			"emar@@@1373156401204943128035@@@yigao\t[50013085=1.0]",
			"emar@@@13731564011628749727@@@yigao\t[350301=2.0, 50002807=2.0]\t[source=211.143.198.95]"
		};
		
		boolean[] isture = new boolean[] {
				false, true, true, true, false, true
		};
		
		GFilter gf = new GFilter();
		
		for(int i = 0; i < lines.length; ++i) {
			System.out.print("\ti=" + i);
			Assert.assertEquals(isture[i], gf.filter(lines[i]));
		}
	}
	
	public static void main(String[] args) {
		if(args.length < 2) {
			System.out.println("Usage: <out-local> <in1-HDFS> [in2, ..]");
			System.exit(2);
		}
		try {
			GFilter gf = new GFilter(args[1], args[0]);
			if(args.length > 2) {
				for(int i = 2; i < args.length; ++i) 
					gf.listFile(args[i]);
			}
			gf.reload();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

}
