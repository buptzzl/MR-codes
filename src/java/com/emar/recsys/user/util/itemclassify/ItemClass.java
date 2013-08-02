package com.emar.recsys.user.util.itemclassify;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import com.emar.util.HdfsIO;

/**
 * 淘宝4级分类信息
 * @author zhoulm
 *
 */
public class ItemClass {
	
	private static final String[] classLPath = new String[] {
		"/com/emar/util/resource/classify/firstcate",
		"/com/emar/util/resource/classify/secondcate",
		"/com/emar/util/resource/classify/thirdcate",
		"/com/emar/util/resource/classify/fourthcate"
	};
	// HDFS上的文件路径 类别顺序遵循 LPath
	private static final String[] classGPath = new String[] {
		"/user/zhouliaoming/data/classify/firstcate",
		"/user/zhouliaoming/data/classify/secondcate",
		"/user/zhouliaoming/data/classify/thirdcate",
		"/user/zhouliaoming/data/classify/fourthcate"
	};  
	// 多层词典，每层为{cid: pid}， 支持基于 cid 向上&向下查找 
	private List<HashMap<String, StrPair>> classID;
	
	public static final int IdxNoExist = -1;  // cid not exist
	public static final String CidNoExist = "0"; // cid not get.
	private static final String SEPA = "\t";
	private static final int COL = 4;  // 列数
	
	public static class StrPair {
		private String cid;
		private String desc;
		
//		public StrPair() { }
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
		if(this.classID == null) {
			this.classID = new ArrayList<HashMap<String, StrPair>>();
		}
		this.classID.clear();
		
		for(int i = 0; i < ItemClass.classGPath.length; ++i) {
			this.classID.add(new HashMap<String, StrPair>());
		}
		
		if(!this.fileLoad(ItemClass.classLPath)){
			// 必须有一个成功才可用
			assert this.fileLoad(ItemClass.classGPath);
		}
	}
	private boolean fileLoad(String[] inputs){
		// 调用HdfsIO中的通用方法
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
	 * 判断当前classid是否存在
	 * @param cid 
	 * 
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
	 * 在整个Map中查找 cid
	 * @param cid
	 * @param rinfo 存储查找到的value
	 * @return -1指不存在, >=0 为存在
	 */
	public int searchCID(String cid) {
		if(cid == null) {
			return -1;
		}
		
		HashMap<String, StrPair> tmpMap;
		int level = -1;
		for(int i = 0; i < this.classID.size(); ++i) {
			tmpMap = classID.get(i);
			if(tmpMap.containsKey(cid)) {
				level = i;
				break;
			}
		}
		
		return level;
	}
	
	/**
	 * 
	 * @param cid
	 * @return list: (cid, desc)pair; 
	 */
	public List<StrPair> searchParCid(String cid) {
		if(cid == null) {
			return null;
		}
		
		List<StrPair> res = null;
		StrPair rinfo;
		int level = this.searchCID(cid);
		if(level != -1) {
			res = new ArrayList<StrPair>(level + 1);
			while(level != -1) {
				rinfo = this.searchCID(cid, level);
				res.add(new StrPair(cid, rinfo.desc));
				cid = rinfo.cid;
				level -= 1;
			}
		}
		
		return res;
	}
	// 除去返回类型不同， 其他同searchParCid
	public String[] searchParArr(String cid) {
		if(cid == null) {
			return null;
		}
		String[] res = null;
		String id;
		int level = this.searchCID(cid);
		if(level != -1) {
			res = new String[level + 1];
			while(level != -1) {
				id = this.searchCID(cid, level).cid;
//				res.add(new StrPair(cid, rinfo.desc));
				res[level] = cid;
				cid = id;
				level -= 1;
			}
		}
		
		return res;
	}
	
	// 返回第K层的所有类别ID
	public Set<String> getClassID(int level) {
		if(level < 0 || level >= classID.size())
			return null;
		
		return classID.get(level).keySet();
	}
	
	private static ItemClass instance;
	private ItemClass() {
		defaultLoad();
		int cnt = 0;
		for(HashMap<String, StrPair> tmap: this.classID) {
			cnt += tmap.size();
		}
		System.out.println("[Info] ItemClass::init load-classify, data-level-size=" 
				+ this.classID.size() 
				+ "\ndata-total-size=" + cnt);
		
	}
	public static ItemClass getInstance() {
		if(ItemClass.instance == null) {
			instance = new ItemClass();
		}
		
		return ItemClass.instance;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		//  补充单元测试代码
		ItemClass itc = ItemClass.getInstance();
		System.out.println("[Info] " + itc.searchParCid("50026555")
				+ "\n" + itc.searchParCid("50018699"));
		
	}

}
