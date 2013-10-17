package com.emar.recsys.user.demo.sex;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.util.*;

import org.json.JSONArray;
import org.json.JSONObject;

import com.emar.recsys.user.demo.IKeywords;
import com.emar.recsys.user.feature.FeatureType;
import com.emar.recsys.user.item.ItemAttribute;
import com.emar.recsys.user.item.ItemFeature;
import com.emar.recsys.user.log.LogParse;
import com.emar.recsys.user.util.DateParse;
import com.emar.recsys.user.util.UtilStr;
import com.emar.recsys.user.util.itemclassify.ItemClass;
import com.emar.recsys.user.util.mr.HdfsIO;

/**
 * 对每个已分的类别， 生成训练数据
 * 
 * @fmt score_pos, score_neg, uid, classID-list, feature-list, confidence.
 * @author zhoulm
 * TODO: 新特征识别：  对标注的词性 按 “-”分割出。 对 单位 归并出具体的 单位词、 数字。
 */
public class ClassSex {

	public static final String SEPA_MR = LogParse.SEPA_MR,
			FMT_T = "yyyymmddHHMMss";
	private static final String[][] BadCase = new String[][] { { "圣女果" }, // 女性
			{} };

	// 内部使用的pair类 用户对象：
	public static class IntPair {
		public float sfemale;
		/** 男性概率。 用负数表示， 绝对值与女性概率之和为1. */
		public float smale;  
		/** 总的男女属性商品 个数 */
		public int confidence;  // @add
		public String uid;
		public List<String[]> classes; // 数组存储的类别按由粗到细的顺序
		public List<String> features;

		public IntPair(float f, float s, String uid) {
			sfemale = f;
			smale = s;
			confidence = 1;
			this.uid = uid;
			classes = new ArrayList<String[]>();
			features = new ArrayList<String>();
		}

		@Override
		public String toString() {
			String uniqeF = new HashSet<String>(features).toString();
//			uniqeF = uniqeF.substring(1, uniqeF.length() - 1);

			List<Set<String>> classMerge = new ArrayList<Set<String>>();
//			for (int i = 0; i < 10; ++i) {
//				classMerge.add(new HashSet<String>(10, 0.9f));
//			}
			for (String[] carr : classes) {
				for (int i = 0; i < carr.length; ++i) {
					if(classMerge.size() < (i + 1)) 
						classMerge.add(new HashSet<String>());  // 动态申请
					classMerge.get(i).add(carr[i]);
				}
			} // FMT: level0:[cid...], level1:[cid...] ...
			String uniqueC = classMerge.toString();

			// TODO 根据训练格式的要求输出, 采用\t做分割符方便存储多级类目信息
			return String.format("%.4f\t%.4f\t%s\t%s\t%s\t%d", sfemale, smale, uid,
					uniqeF, uniqueC, confidence);
		}

	}

	// private static Map<String, IntPair> classDistribute; //全部类别的男女频率
	// private static Map<String, IntPair> classFilter; // 过滤后的结果
	// private static Map<String, Integer> rawdata; // 存储每个用户的信息数组的字串
	// private static List<String> rawdata; // 无去重的全局数据 time,name,price,domain
	private static ItemClass ics;
	public static boolean issum, badCase; // 指示全局特征向量完成生成

	static {
		// classDistribute = new HashMap<String, IntPair>(30000,0.9f);
		// classFilter = new HashMap<String, IntPair>(1000, 0.95f);
		// rawdata = new HashMap<String, Integer>(10000, 0.95f);
		// rawdata = new ArrayList<String>();
		ics = ItemClass.getInstance();
		issum = false;
		badCase = true;
	}

	public static IntPair parseUserJson(String u) {
		if (u == null)
			return null;
		String[] atom = u.split(SEPA_MR);
		if (atom.length < 2)
			return null;
		if (2 < atom.length) {
			for (int i = 1; i < atom.length; ++i)
				atom[1] = atom[1] + atom[i];
		}
		JSONObject jobj;
		JSONArray jRawLog;
		try {
			jobj = new JSONObject(atom[1]);
			jRawLog = jobj.getJSONArray(IKeywords.RawLog);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		int sfemale = jobj.getInt(IKeywords.SPos);
		int sreduce = jobj.optInt(IKeywords.SSum, sfemale + 1);  // sfemale-smale.
		if(sreduce == (sfemale + 1)) {  // GOrder 中先后使用了两种key
			sreduce = jobj.optInt(IKeywords.SReduce);  
		}
		sreduce = 2*sfemale - sreduce;  // change to sum.
		IntPair ipair;
		if (sreduce == 0)
			ipair = new IntPair(sfemale / (float) sreduce, (sfemale - sreduce)
					/ (float) sreduce, atom[0]);
		else
			ipair = new IntPair(0.0f, 0.0f, atom[0]);
		ipair.confidence = sreduce == 0 ? 1 : sreduce;
		String time, cid, cname, name, price, domain;
		for (int i = 0; i < jRawLog.length(); ++i) {
			JSONArray item = jRawLog.getJSONArray(i);

			time = item.getString(0);
			cid = item.getString(1);
			cname = item.getString(2);
			name = item.getString(3);
			price = item.getString(4);
			domain = item.getString(5);

			if (badCase) { // 对基于 产品名识别的 Badcase 进行过滤
				for (int j = 0; j < BadCase[0].length; ++j) {
					if (name.indexOf(BadCase[0][j]) != -1) {
						sreduce -= 1;
						sfemale -= 1;
						break;
					}
				}
				for (int j = 0; j < BadCase[1].length; ++j) {
					if (name.indexOf(BadCase[1][j]) != -1) {
						sreduce -= 1;
						break;
					}
				}
			}
			// 不过滤重复的类别
			String[] cidTree = ics.searchParArr(cid);
			if (cidTree != null)
				ipair.classes.add(cidTree);
			/**  // 仅仅抽取词，进行特征选择
			// time
			int[] ftime = DateParse.getWeekDayHour(time, FMT_T);
			if (ftime != null) {
				ipair.features.add(FeatureType.concat(FeatureType.ORDER,
						FeatureType.SEG, FeatureType.DATE, FeatureType.SEG,
						ftime[0]));
				ipair.features.add(FeatureType.concat(FeatureType.ORDER,
						FeatureType.SEG, FeatureType.HOUR, FeatureType.SEG,
						ftime[1]));
			}
			// classname
			String[] cnameArr = cname.split("\\/");
			for (int j = 0; j < cnameArr.length; ++j) {
				ipair.features.add(FeatureType.concat(FeatureType.ORDER,
						FeatureType.SEG, FeatureType.CLASS, FeatureType.SEG,
						cnameArr[j]));
			}
			ItemFeature.extractFPrice(price, ipair.features);
			// domain
			int idxHost = domain.indexOf('.');
			if (idxHost != -1)
				ipair.features.add(FeatureType.concat(FeatureType.ORDER,
						FeatureType.SEG, FeatureType.URL,
						domain.substring(idxHost)));
			ipair.features.add(FeatureType.concat(FeatureType.ORDER,
					FeatureType.SEG, FeatureType.URL, domain.substring(0,
							idxHost == -1 ? domain.length() : idxHost)));
			*/
			// prod_name
			ItemAttribute iatr;
			try {
				iatr = new ItemAttribute(name);
				List<String> words = iatr.getAttribute();
				ipair.features.addAll(words); // 词与词性， 特征不添加前缀
			} catch (ParseException e) {
			}
		}
		if (badCase && sreduce > 0) {
			ipair.smale = (sreduce - sfemale) / (float) sreduce;
			ipair.sfemale = sfemale / (float) sreduce;
		}
		((ArrayList)ipair.features).trimToSize();
		return ipair;
	}

	@Deprecated
	public static IntPair parseUser(String u) {
		// TODO 解析一条用户数据, 确定atom1中无字符 \x01,如果有则在生成时的Reduce中替换掉
		// prodCnt\x01[atom1, atom2...]\x01Sneg\x01Spos\x01\[s1\x01name1,
		// s2\x01name2...(s*!=0)]
		if (u == null)
			return null;

		String[] atom = u.split("\u0001");
		if (atom.length < 5)
			return null;

		int ssum = Integer.parseInt(atom[2]), sfemale = Integer
				.parseInt(atom[3]);
		int idxRaw = 0;
		IntPair ipair;
		if (ssum != 0)
			ipair = new IntPair((float) sfemale / ssum,
					(ssum - sfemale) / ssum, atom[0].split(SEPA_MR)[0]);
		else
			ipair = new IntPair(0.0f, 0.0f, atom[0].split(SEPA_MR)[0]);
		// 解析数组
		List alist = new ArrayList();
		try {
			int deep = UtilStr.str2list(atom[1], "[", "]", ", ", alist);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("[Error] ClassSex::parseUser() str2list:\n"
					+ "input=" + atom[1] + "\nalist=" + alist);
			return null;
		}
		UtilStr.listDeepTrim(alist);
		String time, cid, cname, name, price, domain;
		// Set<String> fcids = new HashSet<String>();
		for (int i = 0; i < alist.size(); ++i) {
			List<String> ali = (List) alist.get(i);
			if (ali == null || ali.size() < 6) {
				System.out.println("[Error] ClassSex::parseUser() \n" + ali
						+ "\ninput=" + atom[1] + "\nout-parse=" + alist);
				continue;
			}
			// try {
			time = (String) ali.get(0);
			// } catch (Exception e) {
			// e.printStackTrace();
			// System.out.println("[Info] parseUser() line1=" + ali
			// + "\ninput=" + atom[1]);
			// return ipair;
			// }
			cid = (String) ali.get(1);
			cname = (String) ali.get(2);
			name = (String) ali.get(3);
			price = (String) ali.get(4);
			domain = (String) ali.get(5);
			if (badCase) { // 对基于 产品名识别的 Badcase 进行过滤
				for (int j = 0; j < BadCase[0].length; ++j) {
					if (name.indexOf(BadCase[0][j]) != -1) {
						ssum -= 1;
						sfemale -= 1;
						break;
					}
				}
				for (int j = 0; j < BadCase[1].length; ++j) {
					if (name.indexOf(BadCase[1][j]) != -1) {
						ssum -= 1;
						break;
					}
				}
			}
			// if(!fcids.contains(cid)) { // 过滤重复的类别
			String[] cidTree = ics.searchParArr(cid);
			if (cidTree != null)
				ipair.classes.add(cidTree);
			// fcids.add(cid);
			// }
			// time
			int[] ftime = DateParse.getWeekDayHour(time, FMT_T);
			if (ftime != null) {
				ipair.features.add(FeatureType.concat(FeatureType.ORDER,
						FeatureType.SEG, FeatureType.DATE, FeatureType.SEG,
						ftime[0]));
				ipair.features.add(FeatureType.concat(FeatureType.ORDER,
						FeatureType.SEG, FeatureType.HOUR, FeatureType.SEG,
						ftime[1]));
			}
			// classname
			String[] cnameArr = cname.split("\\/");
			for (int j = 0; j < cnameArr.length; ++j) {
				ipair.features.add(FeatureType.concat(FeatureType.ORDER,
						FeatureType.SEG, FeatureType.CLASS, FeatureType.SEG,
						cnameArr[j]));
			}
			ItemFeature.extractFPrice(price, ipair.features);
			// domain
			int idxHost = domain.indexOf('.');
			if (idxHost != -1)
				ipair.features.add(FeatureType.concat(FeatureType.ORDER,
						FeatureType.SEG, FeatureType.URL,
						domain.substring(idxHost)));
			ipair.features.add(FeatureType.concat(FeatureType.ORDER,
					FeatureType.SEG, FeatureType.URL, domain.substring(0,
							idxHost == -1 ? domain.length() : idxHost)));
			// prod_name
			ItemAttribute iatr;
			try {
				iatr = new ItemAttribute(name);
				List<String> words = iatr.getAttribute();
				ipair.features.addAll(words); // 词与词性， 特征不添加前缀
			} catch (ParseException e) {
			}
		}
		if (badCase && ssum > 0) {
			ipair.smale = (ssum - sfemale) / (float) ssum;
			ipair.sfemale = sfemale / (float) ssum;
		}
		return ipair;
	}

	/**
	 * 存储结果
	 * 
	 */
	public static void dump(String output, String input) throws IOException {
		FileWriter fw = new FileWriter(new File(output));

		List<String> lines = HdfsIO.readFile(null, input);
		IntPair ipair = null;
		for (int i = 0; i < lines.size(); ++i) {
//			ipair = ClassSex.parseUser(lines.get(i));
			ipair = ClassSex.parseUserJson(lines.get(i));
			if (ipair != null)
				fw.write (ipair + "\n");
		}
		fw.close();
	}

	private ClassSex ins;

	private ClassSex() {
	};

	public ClassSex getInstance() {
		// 全局只能有1个实例
		if (ins == null) {
			ins = new ClassSex();
		}
		return ins;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			String ins = "D:/Data/MR-codes/data/test/goodSex.3", outs = "D:/Data/MR-codes/data/test/good_sex_100_f";
			System.out.println("[Info] ClassSex::main args="
					+ Arrays.asList(args));
//			 ClassSex.dump(outs, ins);
			ClassSex.dump(args[0], args[1]);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
