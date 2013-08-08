package com.emar.recsys.user.model;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.*;
import java.util.Map.Entry;

import com.emar.recsys.user.util.UtilObj;

/**
 * 转换binary特征的表示， 类别标示写在第1列
 * @author zhoulm
 * 
 */
public class DataNormalize {

	private HashMap<String, Integer> features;
	private static final String infof = "% ARF file for the user-behave-targeting\n";
	private static final String relationf = "\n@relation user-behavoir-targeting\n";
	private static final String classf = "@attribute classtype {0, 1}\n";
	private static final String dataf = "\n@data\n";
	
	private StringBuffer dataBuf;
	private String inpath, outpath;
	private String sepa;

	private int idxClass;

	public DataNormalize(String in, String out, String sepa) throws Exception {
		this(in, out);
		this.sepa = sepa;
	}

	public DataNormalize(String in, String out) throws Exception {
		if (in == null || out == null) {
			System.out
					.println("[ERROR] DataNormalize::init inpath or outpath is NULL.");
			throw new Exception();
		}
		this.inpath = in;
		this.outpath = out;
		this.sepa = ", ";
		this.dataBuf = new StringBuffer();
		this.features = new HashMap<String, Integer>(1 << 10, 0.95f);
	}

	/**
	 * 写本地数据
	 * 
	 * @throws IOException
	 */
	public void writeData() throws IOException {
		File f = new File(this.outpath);
		if (!f.exists()) {
			f.createNewFile();
		}
		BufferedWriter of = new BufferedWriter(new FileWriter(f));
		of.write(infof);
		of.write(relationf);
		of.write(this.setAttritbute());  
		of.write(classf);
		of.write(dataf);

		BufferedReader rf = new BufferedReader(new FileReader(this.inpath));
		String line;
		while ((line = rf.readLine()) != null) {
			of.write(this.setData(line));
		}
		rf.close();
		of.close();
	}

	/**
	 * 添加所有特征类目信息
	 */
	private String setAttritbute() {
		dataBuf.delete(0, dataBuf.length());
		List<Entry<String, Integer>> forder = UtilObj
				.entrySort(features, false);
		for (Entry<String, Integer> as : forder) {
			// 替换掉空格 按arff格式标示
			dataBuf.append(String.format("@attribute %s {0, 1}\n", 
					as.getKey().replace(' ', '-')));  
		}
		return dataBuf.toString();
	}

	// data 的稀疏数据格式， 仅保留清洗后的结果
	private String setData(String line) {
		if (line == null) {
			return null;
		}
		Integer tmp;
		Set<Integer> arrSet = new HashSet<Integer>();
		
		dataBuf.delete(0, dataBuf.length());
		
		String[] atom = line.split(sepa);
		dataBuf.append("{");
		for (String s : atom) {
			s = s.trim();
			tmp = features.get(s);
			if(tmp != null) {  
				arrSet.add(tmp);
//				dataBuf.append(String.format("%d 1%s",tmp,sepa));
			}
		}
		List<Integer> arr = Arrays.asList(arrSet.toArray(new Integer[0]));
		Collections.sort(arr);  // asc
		for(Integer t: arr) {
			dataBuf.append(String.format("%d 1%s", t, sepa));
		}
		dataBuf.deleteCharAt(dataBuf.length() - sepa.length());
		dataBuf.append(this.setClass(atom[idxClass]));
		
		dataBuf.append("}\n");
		return dataBuf.toString();
	}
	private String setClass(String vclass) {
		if(vclass.equals("0")) {
			return "";
		}
		return String.format("%s%d 1", sepa, this.features.size()); 
	}

	public void init(boolean cntfreq) throws IOException {
		BufferedReader rf = new BufferedReader(new FileReader(this.inpath));
		String line;
		String[] atom;
		String tmp;
		while ((line = rf.readLine()) != null) {
			atom = line.split(this.sepa);
			if (atom.length < this.idxClass) {
				System.out
						.println("[Warn] DataNormalize::init() class-type is unknown.");
				continue;
			}
			for (int i = 0; i < this.idxClass; ++i) {
				tmp = atom[i].trim();
				if (cntfreq) {  // 统计频率信息
					if (!this.features.containsKey(tmp)) {
						this.features.put(tmp, 1);
					} else {
						this.features.put(tmp, this.features.get(tmp) + 1);
					}
				} else {
					if (!this.features.containsKey(tmp)) {
						this.features.put(tmp, this.features.size());
					}
				}
			}
			for (int i = this.idxClass + 1; i < atom.length; ++i) {
				tmp = atom[i].trim();
				if (cntfreq) {
					if (!this.features.containsKey(tmp)) {
						this.features.put(tmp, 1);
					} else {
						this.features.put(tmp, this.features.get(tmp) + 1);
					}
				} else {
					if (!this.features.containsKey(tmp)) {
						this.features.put(tmp, this.features.size());
					}
				}
			}
		}
		rf.close();
	}
	// 按频率裁剪特征集合的数量，生成新的特征顺序, 有效区间 [fmin, fmax]
	public void featureTrim(Integer fmin, Integer fmax) {
		if(fmin == null || (fmax != null && fmax < fmin)) {
			return; //
		}
		int cnt = 0;
		Integer f = 0;
		String k;
		Iterator<String> itr = this.features.keySet().iterator();
		while(itr.hasNext()) {
			k = itr.next();
			f = this.features.get(k);
			if(f < fmin || (fmax != null && f > fmax)) {
				itr.remove();
				this.features.remove(k);
			} else {
				this.features.put(k, cnt);  // 更新特征下标？
				cnt += 1;
			}
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		DataNormalize nor;
		try {
			nor = new DataNormalize("D:/Downloads/telnet/train.txt",
					"D:/Data/MR-codes/data/test.txt", ", ");
			nor.init(true);
			System.out.println("[Test] " + nor.features.size());
			nor.featureTrim(2, null);
			System.out.println("[Test] " + nor.features.size());
			nor.writeData();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
