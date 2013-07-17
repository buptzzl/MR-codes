package com.emar.recsys.user.util.itemclassify;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.emar.util.HdfsIO;

/**
 * 淘宝4级分类信息
 * @author zhoulm
 *
 */
public class ItemClass {
	
	private static final String[] classLPath = new String[] {
		"/resources/classifyType/TB_level1.txt",
		"/resources/classifyType/TB_level2.txt",
		"/resources/classifyType/TB_level3.txt",
		"/resources/classifyType/TB_level4.txt"
	};
	// HDFS上的文件路径 类别顺序遵循 LPath
	private static final String[] classGPath = new String[] {
		"/",
		"/",
		"/",
		""
	};  
	// 多层词典，每层为{cid: pid}， 支持基于 cid 向上&向下查找 
	private List<HashMap<String, StrPair>> classID;
	
	public static final int IdxNoExist = -1;  // cid not exist
	public static final String CidNoExist = "0"; // cid not get.
	private static final String SEPA = "\t";
	private static final int COL = 4;  // 列数
	
	public static class StrPair {
		private String cid = null;
		private String desc = null;
		
		public StrPair() { }
		public StrPair(String id, String desc) {
			this.cid = id;
			this.desc = desc;
		}
		
		// for Hash_Map use 
		public boolean equals(Object obj) {
			if(this == obj) {
				return true;
			}
			if(!(obj instanceof StrPair)) {
				return false;
			}
			
			StrPair sobj = (StrPair)obj;
			return sobj.cid == this.cid ;
		}
		public int hashCode() {
			return cid.hashCode();
		}
		
		public int compareTo(Object obj) {
			if(this == obj) {
				return 0;
			}
			if(!(obj instanceof StrPair)) {
				return -1;
			}
			
			StrPair sobj = (StrPair)obj;
			return this.cid.compareTo(sobj.cid);
		}
		
		public String toString() {
			return cid + "_" + desc;
		}
		
	}

	private void defaultLoad() {
		for(int i = 0; i < ItemClass.classGPath.length; ++i) {
			this.classID.add(new HashMap<String, StrPair>());
		}
		if(!this.fileLoad(ItemClass.classLPath)){
			// 必须有一个成功才可用
			assert this.fileLoad(ItemClass.classGPath);
		}
	}
	private boolean fileLoad(String[] inputs){
		// TODO read local or global file
		for(int i = 0; i < inputs.length; ++i) {
			List<String> lines = HdfsIO.readFile(this.getClass(), inputs[i]);
			if(lines == null) {
				return false;
			} else {
				for(String line : lines) {
					this.data2class(line, i);
				}
			}
		}
		return true;
	}
	// 用每行类型数据 建立 类别
	private void data2class(String line, int level) {
		// TODO
		if(line == null) {
			return;
		}
		String[] atoms = line.trim().split(SEPA);
		if(atoms.length != COL) {
			return;
		}
		HashMap<String, StrPair> tmpMap = classID.get(level);
		tmpMap.put(atoms[0], new StrPair(atoms[3], atoms[2]));
		return;
	}
	
	/**
	 * @param cid 
	 * @return the leve exist cid.
	 */
	public boolean isexistCID(String cid, int level) {
		if(cid == null || level < 0 || this.classID.size() <= level) {
			return false;
		}
		
		HashMap<String, StrPair> tmpMap = classID.get(level);
		return tmpMap.containsKey(cid);
	}
	
	public StrPair searchCID(String cid, int level) {
		if(this.isexistCID(cid, level)) {
			return this.classID.get(level).get(cid);
		}
		return null;
	}
	
	/**
	 * @param cid
	 * @param rinfo 存储查找到的value
	 * @return 
	 */
	public int searchCID(String cid, StrPair rinfo) {
		if(cid == null) {
			return -1;
		}
		
		HashMap<String, StrPair> tmpMap;
		int level = -1;
		for(int i = 0; i < this.classID.size(); ++i) {
			tmpMap = classID.get(i);
			if(tmpMap.containsKey(cid)) {
				rinfo = tmpMap.get(cid);
				level = i;
				break;
			}
		}
		
		return level;
	}
	
	/**
	 * @param cid
	 * @return list: (cid, desc)pair; maybe list==[]
	 */
	public List<StrPair> searchParCid(String cid) {
		if(cid == null) {
			return null;
		}
		
		StrPair rinfo = null;
		int level = this.searchCID(cid, rinfo);
		ArrayList<StrPair> res = new ArrayList<StrPair>(level + 1);
		while(level != -1) {
			res.add(new StrPair(cid, rinfo.desc));
			cid = rinfo.cid;
			rinfo = this.searchCID(cid, level - 1);
		}
		return res;
	}
	
	

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO 补充单元测试代码

	}

}
