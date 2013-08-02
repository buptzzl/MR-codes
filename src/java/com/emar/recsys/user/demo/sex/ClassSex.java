package com.emar.recsys.user.demo.sex;

import java.util.*;

import com.emar.recsys.user.util.UtilStr;
import com.emar.recsys.user.util.itemclassify.ItemClass;
import com.emar.recsys.user.util.itemclassify.ItemClass.StrPair;

/**
 * 统计每个类别的 性别频率
 * @author zhoulm
 *
 */
public class ClassSex {
	
	// 内部使用的pair类
	private static class IntPair {
		public int first;
		public int second;
		public List<Integer> rawlist; // 存储某个类别下的所有信息数组的下标索引
		
		public IntPair(int f, int s) {
			first = f;
			second = s;
		}
		
	}
	
	private static Map<String, IntPair> classDistribute;  //全部类别的男女频率
	private static Map<String, IntPair> classFilter;  // 过滤后的结果
//	private static Map<String, Integer> rawdata; // 存储每个用户的信息数组的字串
	private static List rawdata;  // time,name,price,domain
	private	static ItemClass ics;
	private static boolean issum;  // 指示全局特征向量完成生成
	
	static {
		classDistribute = new HashMap<String, IntPair>(30000,0.9f);
		classFilter = new HashMap<String, IntPair>(1000, 0.95f);
//		rawdata = new HashMap<String, Integer>(10000, 0.95f);
		rawdata = new ArrayList<String>();
		ics = ItemClass.getInstance();
		issum = false;
	}
		
	
	public static boolean parseUser(String u) {
		// TODO 解析一条用户数据, 确定atom1中无字符 \x01,如果有则在Reduce中替换掉
		// prodCnt\x01[atom1, atom2...]\x01Sneg\x01Spos\x01\[s1\x01name1, s2\x01name2...(s*!=0)]
		if(u == null) 
			return false;
		
		String[] atom = u.split("\u0001");
		if (atom.length < 5)
			return false;
		
		int sneg = Integer.parseInt(atom[2]), spos = Integer.parseInt(atom[3]);
		int idxRaw = 0;
		
		// 解析数组
		List alist = new ArrayList();
		int deep = UtilStr.str2list(atom[1], "[", "]", ", ", alist);
		String cid = null;
		IntPair ipair = null;
		for(int i = 0; i < alist.size(); ++i) {
			List ali = (List) alist.get(i);
			cid = (String) ali.get(1);
			String[] pars = ics.searchParArr(cid);  // 不可能返回NULL
			for(int j = 0; j < pars.length; ++j) {
				if(classDistribute.containsKey(pars[j])) 
					ipair = classDistribute.get(pars[j]);
				else
					ipair = new IntPair(0, 0);
				ipair.first += sneg;
				ipair.second += spos;
				ipair.rawlist.add(idxRaw); 
				classDistribute.put(pars[j], ipair);  // update 
				
			}
		}
		
		return true;
	}
	
	/**
	 * TODO 商品名的特征向量生成。
	 * 
	 * 基于以识别性别的总数的 上下限，过滤有效结果。
	 * @param mnsum
	 * @param mxsum
	 */
	public static void filterSum(int mnsum, int mxsum) {
		// TODO
	}
	
	public static void dump(String output) {
		// TODO 将所有结果保存, 需要计算概率 spos/(sneg+spos)
		
	}
	
	private ClassSex ins;  
	private ClassSex() {};
	public ClassSex getInstance() {
		// 全局只能有1个实例
		if(ins == null) {
			ins = new ClassSex();
		}
		return ins;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
