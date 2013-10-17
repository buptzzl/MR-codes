package com.emar.recsys.user.demo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.json.JSONArray;
import org.json.JSONObject;

import com.emar.recsys.user.util.UtilJson;


/**
 * 基于JSON 进行用户消费分层。 不处理用户消费兴趣对应的权重（单机版）。
 * @author zhoulm
 * @ref PredictMerge.java
 * FMT:
 * UID:PREDICT{ UID_CONSUMER:{CID1:[level1]} }
 */
public class UserConsumeHierary {
	private static final String RawLog = IKeywords.RawLog, 
			KUid = IKeywords.KUid, UID_CONSUMER = IKeywords.UID_CONSUMER, 
			UID_CONS_W = IKeywords.UID_CONS_W;
	private static final int IdxClass = 1, IdxPrice = 4, 
			i_price=0, i_level=1, i_info=2, i_weight=3;
	
	private static StringBuffer kUid;
	
	private HashMap<String, ConsumerHierary> h_consumers;//cid:hierary
	private String pathIn, pathOut;  // 原始JSON 文件路径
	/** uid:[ cid:[ [prices], [levels], [name,LEVEL_INFO], [tmp-weights] ],..] 缓存全部用户ID  */
	private HashMap<String, String[][][]> cons_user; 
	private boolean f_load, debug; 
	
	/** 处理文件异常  */
 	public UserConsumeHierary(String pIn, String pOut) throws IOException {
 		pathIn = pIn;
 		pathOut = pOut;
		h_consumers = new HashMap<String, ConsumerHierary>(16, 0.9f);
		cons_user = new HashMap<String, String[][][]>(16, 0.9f);
		kUid = new StringBuffer();
		f_load = false;
		debug = false;
		
		FileReader fr = new FileReader(pathIn); //处理文件异常
		fr.close();
		FileWriter fw = new FileWriter(pathOut);
		fw.close();
	}
	private static void setUid() {
		kUid.delete(0, kUid.length());
		kUid.append(IKeywords.KUid);
	}
	
	/** 增量处理文件   
	 * @throws IOException */
	public boolean BufferProcess() throws IOException {
		int cnt = 0;
		String line;
		BufferedReader rjson;
		rjson = new BufferedReader(new FileReader(pathIn));
		while((line = rjson.readLine()) != null) {
			cnt ++;
			this.process(line, cnt);  // 处理业务隔离出来
		}
		rjson.close();
		this.f_load = true;
		this.getHierary();
		
		if (debug)
			System.out.println("[Info] UserConsumerHierary::BufferProcess line="
					+ cnt);
		return false;
	}
	
	/**  处理一个 JSON 数据行  */
	public boolean process(String line, int index, String... args) {
		final int idx_j = 1, idx_k = 0;
		final String sepa = "\t";
		UserConsumeHierary.setUid();// 使用前清理状态
		JSONObject l_json = UtilJson.parseJsonLine(line, kUid, sepa, idx_j, idx_k);
		this.doSegment(l_json);
		
		return true;
	}
	
	/**  加载1个用户的数据  */
	private void doSegment(JSONObject raw) {
		JSONArray rlogs = raw.getJSONArray(RawLog);
		if (rlogs.length() == 0)
			return;
		String cid, price;
		HashMap<String, HashMap<Float, Integer>> cid2prices = 
				new HashMap<String, HashMap<Float, Integer>>(4, 0.9f);
		ConsumerHierary h_cons;
		HashMap<Float, Integer> t_set;
//		List<Integer> l_weight = new ArrayList<Integer>();
		
		for (int i = 0; i < rlogs.length(); ++i) {
			cid = rlogs.getJSONArray(i).getString(IdxClass).trim(); //classify-id.
			price = rlogs.getJSONArray(i).getString(IdxPrice).trim();
			if (cid2prices.containsKey(cid)) {
				t_set = cid2prices.get(cid);
			} else {
				t_set = new HashMap<Float, Integer>(4, 0.9f);
				cid2prices.put(cid, t_set);
			}
			// price:freqence.
			try {
				t_set.put(Float.parseFloat(price), t_set.containsKey(price) ? (t_set.get(price) + 1):1);
			} catch (Exception e) {
				if (debug)
					System.out.println("[ERR] UserCons*-doSegment() price=" 
							+ price + " isn't float.");
			}
				
			if (h_consumers.containsKey(cid)) {
				h_cons = h_consumers.get(cid);
			} else {
				h_cons = new ConsumerHierary();
				h_consumers.put(cid, h_cons);
			}
			h_cons.update(price);
		}
		
		String[][][] c_val = new String[cid2prices.size()][4][];
		int idx = 0;
		for (Map.Entry<String, HashMap<Float, Integer>> ei : cid2prices.entrySet()) {
//			c_val[idx][0] = ei.getValue().toArray(new String[ei.getValue().size()]);
			t_set = ei.getValue();
			c_val[idx][i_price] = new String[t_set.size()];
			c_val[idx][i_weight] = new String[c_val[idx][i_price].length];
			int j = 0;
			TreeSet<Float> e_price = new TreeSet<Float>(t_set.keySet());// sort.
			for (Float fi : e_price) {
				c_val[idx][i_price][j] = fi.toString();
				c_val[idx][i_weight][j] = t_set.get(fi).toString();//frequence
				++j;
			}
			
//			c_val[idx][i_level] = new String[c_val[idx][i_price].length];//unuse.
			c_val[idx][i_info] = new String[]{ei.getKey(), ""};//class, emtpy-for-LEVEL_INFO.
			idx ++;
		}
		this.cons_user.put(kUid.toString(), c_val);//kUid存储userid,处理完1行前未被清空
		return;
	}
	
	/** 生成用户的消费层次 */
	private boolean getHierary() {
		if (!this.f_load) 
			return false;
		HashMap<Integer, Integer> level_info = new HashMap<Integer, Integer>(16,0.9f);
		String[][][] t3_arr;
		ConsumerHierary h_con;
		for (Map.Entry<String, String[][][]> ei : this.cons_user.entrySet()) {
			t3_arr = ei.getValue();
			
			for (int i = 0; i < t3_arr.length; ++i) {
				int l_tmp;
				
				h_con = this.h_consumers.get(t3_arr[i][i_info][0]);
				int[] l_freq = new int[h_con.getMaxLayer()];
				for (int j = 0; j < t3_arr[i][i_price].length; ++j) {
//					t3_arr[i][1][j] = h_con.getLayer(t3_arr[i][0][j])+"";
					l_tmp = h_con.getLayer(t3_arr[i][i_price][j]);
					if (l_tmp < 0) {
						if (debug)
							System.out.println("[ERR] UserCon--getHierary() layer="
									+ l_tmp + " unnormal");
					} else {
						l_freq[l_tmp] += Integer.parseInt(t3_arr[i][i_weight][j]);
					}
				}

				level_info.clear();
				for (int j = 0; j < l_freq.length; ++j) {
					if (l_freq[j] != 0) 
						level_info.put(j, l_freq[j]);
				}
				t3_arr[i][i_info][1] = level_info.toString();
				
				if (debug) 
					System.out.println(String.format("[Info] UserConsumeHierary::getHierary" 
							+ " \nrange=%s \nprices=%s \nlevels=%s",h_con,
							Arrays.asList(t3_arr[i][i_price]), Arrays.asList(t3_arr[i][i_info])));
			}
		}
		
		return true;
	}
	
	/** 保存结果. kuid:uid,kcid:[cid_i,level_i]..  */
	public void dump() throws IOException {
		String[][][] t3_arr;
		int cnt = 0;
		
		BufferedWriter wbuf;
		wbuf = new BufferedWriter(new FileWriter(pathOut));
		for (Map.Entry<String, String[][][]> ei : this.cons_user.entrySet()) {
			t3_arr = ei.getValue();
			
			JSONObject j_root = new JSONObject(), j_cons = new JSONObject();
			j_root.put(KUid, ei.getKey());
			for (int i = 0; i < t3_arr.length; ++i) {
//				JSONArray jarr_cons = new JSONArray(t3_arr[i][1]);
				j_cons.put(t3_arr[i][i_info][0], t3_arr[i][i_info][1]);//cid:[level-info]
			}
			j_root.put(UID_CONS_W, j_cons);
			
			wbuf.write(j_root.toString());
			wbuf.newLine();
			
			cnt ++;
		}
		wbuf.close();
		
		if (debug) 
			System.out.println("[Info] UserConsumerHierary::dump write-line=" + cnt);
	}
	
	/** 增加|更新 一个 K-V 到JSON */
//	public boolean update(JSONObject raw, String k, String v) ;

	public static void main(String[] args) {
		// UT
		String[] paths = new String[] {
				"data/test/user_consumer_level.txt",
				"data/test/user_consumer_level.res"
		};
		UserConsumeHierary u_c;
		try {
			u_c = new UserConsumeHierary(paths[0], paths[1]);
			u_c.BufferProcess();
			u_c.dump();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
	}
	
}
