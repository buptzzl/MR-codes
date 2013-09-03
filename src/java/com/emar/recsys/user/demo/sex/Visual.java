package com.emar.recsys.user.demo.sex;

import java.io.*;

import org.json.JSONArray;
import org.json.JSONObject;

import com.emar.recsys.user.demo.IKeywords;
import com.emar.recsys.user.log.LogParse;
import com.emar.recsys.user.model.ins.ParseOrder;
import com.emar.recsys.user.model.ins.ParseOrder.ClassType;

/**
 * 性别结果 规范化，实现可视化。
 * @author zhoulm
 *
 */
public class Visual {
	public static final String SEPA_MR = LogParse.SEPA_MR, SEPA = LogParse.SEPA;
	
	/** 解析json串  */
	public static void saveJson(String out, String in) throws Exception {
		String line;
		final String uid = "uid", female = "female", male = "male", goods = "goods";
		JSONObject jobj, jout;
		JSONArray jRawLog;
		
		final ParseOrder parser = new ParseOrder();
		parser.init(ClassType.BinaryClass.toString(), "debug");
		
		BufferedWriter fout = new BufferedWriter(new FileWriter(out));
		BufferedReader fin = new BufferedReader(new FileReader(in));
		while((line = fin.readLine()) != null) {
//			parser.parseLine(line);
			String[] atom = line.split(SEPA_MR);
			if (atom.length < 2)
				continue;
			if (2 < atom.length) {
				for (int i = 1; i < atom.length; ++i)
					atom[1] = atom[1] + atom[i];
			}
			
			try {
				jobj = new JSONObject(atom[1]);
				jRawLog = jobj.getJSONArray(IKeywords.RawLog);
			} catch (Exception e) {
				e.printStackTrace();
				continue;
			}
			int sfemale = jobj.getInt(IKeywords.SPos);
			int sreduce = jobj.optInt(IKeywords.SSum, sfemale + 1); 
			sreduce = 2*sfemale - sreduce;
			
			jout = new JSONObject();
			jout.put(uid, atom[0]).put(female, sfemale / (float) sreduce)
				.put(male, (sfemale - sreduce) / (float) sreduce)
				.put(goods, jRawLog);
			
			fout.write(jout.toString());
			fout.newLine();
		}
		fin.close();
		fout.close();
	}
	
	public static void main(String[] args) {
		try {
			saveJson(args[0], args[1]);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
}
