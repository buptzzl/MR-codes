package com.emar.recsys.user.demo.sex;

import java.util.*;

import com.emar.recsys.user.feature.FeatureType;
import com.emar.recsys.user.log.LogParse;
import com.emar.recsys.user.util.UtilStr;
import com.emar.recsys.user.util.itemclassify.ItemClass;
import com.emar.recsys.user.util.itemclassify.ItemClass.StrPair;

/**
 * 对每个已分的类别， 生成训练数据
 * @fmt score_pos, score_neg, uid, classID-list, feature-list.
 * @author zhoulm
 *
 */
public class ClassSex {
	
	public static final String SEPA = LogParse.SEPA;
	
	// 内部使用的pair类  用户对象： 
	public static class IntPair {
		public int scorePos;
		public int scoreNeg;
		public String uid;
		public String[] classes;
		public List<String> features;
		
		public IntPair(int f, int s, String uid) {
			scorePos = f;
			scoreNeg = s;
			this.uid = uid;
			classes = null;
			features = new ArrayList<String>();
		}
		
	}
	
//	private static Map<String, IntPair> classDistribute;  //全部类别的男女频率
//	private static Map<String, IntPair> classFilter;  // 过滤后的结果
//	private static Map<String, Integer> rawdata; // 存储每个用户的信息数组的字串
//	private static List<String> rawdata;  // 无去重的全局数据 time,name,price,domain
	private	static ItemClass ics;
	private static boolean issum;  // 指示全局特征向量完成生成
	
	static {
//		classDistribute = new HashMap<String, IntPair>(30000,0.9f);
//		classFilter = new HashMap<String, IntPair>(1000, 0.95f);
//		rawdata = new HashMap<String, Integer>(10000, 0.95f);
//		rawdata = new ArrayList<String>();
		ics = ItemClass.getInstance();
		issum = false;
	}
		
	
	public static IntPair parseUser(String u) {
		// TODO 解析一条用户数据, 确定atom1中无字符 \x01,如果有则在生成时的Reduce中替换掉
		// prodCnt\x01[atom1, atom2...]\x01Sneg\x01Spos\x01\[s1\x01name1, s2\x01name2...(s*!=0)]
		if(u == null) 
			return null;
		
		String[] atom = u.split("\u0001");
		if (atom.length < 5)
			return null;
		
		int sneg = Integer.parseInt(atom[2]), spos = Integer.parseInt(atom[3]);
		int idxRaw = 0;
		
		// 解析数组
		List alist = new ArrayList();
		int deep = UtilStr.str2list(atom[1], "[", "]", ", ", alist);
		String cid = null;
		IntPair ipair = new IntPair(spos, sneg, atom[0].split(SEPA)[0]);
		for(int i = 0; i < alist.size(); ++i) {
			List ali = (List) alist.get(i);
			cid = (String) ali.get(1);
			String[] pars = ics.searchParArr(cid);  // 不可能返回NULL
			for(int j = 0; j < pars.length; ++j) {
				if(classDistribute.containsKey(pars[j])) 
					ipair = classDistribute.get(pars[j]);
				else
					ipair = new IntPair(0, 0);
				ipair.scorePos += sneg;
				ipair.scoreNeg += spos;
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
		// 
		
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
