package com.emar.recsys.user.demo;

import org.json.JSONObject;

/**
 * 人口属性通用方法的集合
 * @author zhoulm
 * 
 */
public class UtilDemo {
	
	public static final String j_sepa = "\t", k_uid = IKeywords.KUid; 
	
	/** 
	 * TODO: 将代码转移到 UtilStr.java 
	 * 解析一个JSON, 并处理可能存在 \t 分隔符 的异常.
	 * 注： 无效串时返回空的JSONObject(). k == null 时不处理非JSON部分 与 提取KEY的功能。
	 * UT-done.
	 * @param k 抽取Uid对应的value,非完全JSON时取第idx_k个元素，最终结果存储在k中。
	 * @param sepa  非完全JSON时，指定分割符。 第1个原子必须为JSON。
	 * @param idx_j 非完全JSON时，分割后JSON串所在的IDX。
	 * @param idx_k 非完全JSON时，分割后Uid所在的IDX。并插入到JSON串。
	 */
	public static JSONObject parseJsonLine(String line, StringBuffer k, String sepa,
			int idx_j, int idx_k) {
		JSONObject j_res = new JSONObject();
		if (line == null || line.trim().length() < 3 || k == null) {
			return j_res;
		}			
//		if (sepa == null) 
//			sepa = j_sepa; // 使用默认的分割符
		line = line.trim();
		String i_key;
		if (line.charAt(0) == '{') {
			j_res = new JSONObject(line);
			if (k.length() != 0) {
				k.delete(0, k.length());
				k.append(j_res.getString(k_uid));  // 提取 value
			}
		} else {  // 非全为JSON，分割。
			String[] atoms = line.split(sepa);
			if (atoms.length >= idx_j && atoms.length >= idx_k) {
				j_res = new JSONObject(atoms[idx_j]);
				if (k.length() != 0) {
					i_key = k.toString();
					k.delete(0, k.length());
					k.append(atoms[idx_k]);
					j_res.put(i_key, k);  // 提取 & 更新
				}
			}
		}
		
		return j_res;
	}


	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
