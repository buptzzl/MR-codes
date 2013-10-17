package com.emar.recsys.user.util;

import org.antlr.grammar.v3.ANTLRv3Parser.finallyClause_return;
import org.json.JSONArray;
import org.json.JSONObject;

import com.emar.recsys.user.demo.IKeywords;

/**
 * 人口属性通用方法的集合
 * 
 * @author zhoulm
 * @UT done.
 */
public class UtilJson {

	public static final String j_sepa = "\t", k_uid = IKeywords.KUid;

	/**
	 * 解析一个JSONObject, 并处理可能存在 \t 分隔符 的异常. 注： 无效串时返回空的JSONObject(). k == null
	 * 时不处理非JSON部分 与 提取KEY的功能。 UT-done.
	 * 
	 * @param k
	 *            抽取Uid对应的value,非完全JSON时取第idx_k个元素，最终结果存储在k中。
	 * @param sepa
	 *            非完全JSON时，指定分割符。 第1个原子必须为JSON。
	 * @param idx_j
	 *            非完全JSON时，分割后JSON串所在的IDX。
	 * @param idx_k
	 *            非完全JSON时，分割后Uid所在的IDX。并插入到JSON串。
	 */
	public static JSONObject parseJsonLine(String line, StringBuffer k,
			String sepa, int idx_j, int idx_k) {
		JSONObject j_res = null;
		if (line == null || line.trim().length() < 3 || k == null) {
			return j_res;
		}
		// if (sepa == null)
		// sepa = j_sepa; // 使用默认的分割符
		line = line.trim();
		String i_key;
		if (line.charAt(0) == '{') {
			j_res = new JSONObject(line);
		} else { // 非全为JSON，分割。
			String[] atoms = line.split(sepa);
			if (atoms.length >= idx_j && atoms.length >= idx_k) {
				j_res = new JSONObject(atoms[idx_j]);
				j_res.put(k_uid, atoms[idx_k].trim());
			}
		}
		if (k != null && j_res.has(k_uid)) {
			if (k.length() != 0) {
				k.delete(0, k.length());
			}
			k.append(j_res.getString(k_uid)); // 提取 value
		}

		return j_res;
	}
	/** 
	 * 解析JSONObject|JSONArray, 对ARRAY 则插入形成 JSONObject. 
	 * 对多字符串类型，须指定Uid对应的字段并插入。
	 * @param keyArr 当Str为Array类型时，构建JSONObject时插入的key.
	 * @param idx_k  指定Uid对应的字段下标，并插入JSONObject。<0时不插入
	 */
	public static JSONObject parseJson(String line, String sepa, String keyArr,
			int idx_j, int idx_k) {
		final int N = 2;
		final char JOBJ_1 = '{', JOBJ_2 = '}', JARR_1 = '[', JARR_2 = ']';
		JSONObject j_res = null;
		
		if (line == null || line.trim().length() < N) {
			return j_res;
		}
		line = line.trim();
		if (line.charAt(0) == JOBJ_1 && line.charAt(line.length() - 1) == JOBJ_2) {
			j_res = new JSONObject(line);
		} else if (line.charAt(0) == JARR_1 && line.charAt(line.length() - 1) == JARR_2) {
			JSONArray j_arr = new JSONArray(line);
			j_res = new JSONObject();
			j_res.put(keyArr, j_arr);
		} else {
			String[] atoms = line.split(sepa);
			if ((0 <= idx_j && atoms.length >= idx_j) 
					&& (0 <= idx_k &&atoms.length >= idx_k)) {
				j_res = parseJson(atoms[idx_j], null, keyArr, -1, -1);
				j_res.put(k_uid, atoms[idx_k].trim()); // 加入UID
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
