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
	 * 解析一个JSON, 并处理可能存在 \t 分隔符 的异常.
	 * 注： 无效串时返回空的JSONObject(). k == null 时不处理非JSON部分 与 提取KEY的功能。
	 * UT-done.
	 * @ret k 抽取串 k 对应的value, 默认为数组的第0个
	 * @param sepa  非完全JSON时，指定分割符。 第1个原子必须为JSON。
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
		} else {
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
