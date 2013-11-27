package com.emar.recsys.user.model.ins;

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
import com.emar.recsys.user.util.UtilStr;

/**
 * 转换字符串表达的特征 为 binary特征， 类别标示写在最后一列，生成 arff文件。
 * @author zhoulm
 * 
 */
public class DataNormalize {

	private HashMap<String, Integer> features;
//	private static final String infof = "% ARF file for the user-behave-targeting\n";
	private static final String relationf = "\n@relation user-behavoir-targeting\n";
//	private static final String classf = "@attribute classtype {0, 1}\n";
//	private static final String regressf = "@attribute classtype numeric\n";
	private static final String dataf = "\n@data\n";
	
	public IAttrParser Parser;
	private StringBuffer dataBuf;
	private String inpath, outpath, fpath, sepa;
//	private String classInfo;
	
	private int idxClass;
	private boolean flagRegress;
	public boolean debug;

	public DataNormalize(String out, String in, String parser, String sepa) throws Exception {
		this(in, out, sepa);
		this.sepa = sepa;
	}

	public DataNormalize(String out, String in, String parser) throws Exception {
		if (in == null || out == null) {
			System.out
					.println("[ERROR] DataNormalize::init inpath or outpath is NULL.");
			throw new Exception();
		}
		this.inpath = in;
		this.outpath = out;
		this.fpath = out + ".feature";
		this.sepa = ", ";
		this.dataBuf = new StringBuffer();
		this.features = new HashMap<String, Integer>(1 << 10, 0.95f);
		this.Parser = (IAttrParser)Class.forName(parser).newInstance();  // 基于反射
	}

	/**
	 * 生成arff文件， 写本地数据。 并写特征映射表。
	 * 
	 * @throws IOException
	 */
	public void writeData() throws IOException {
		File f = new File(this.outpath);
		if (!f.exists()) {
			f.createNewFile();
		}
		
		int fsize = 0;  // @debug
		for(int i = 0; i < Parser.CntClass.length; ++i)
			Parser.CntClass[i] = 0;
		BufferedWriter of = new BufferedWriter(new FileWriter(f));
//		of.write(infof);
		of.write(relationf);
		of.write(this.setAttribute());  
		of.write(this.Parser.getAttribute());
		of.write(dataf);
		BufferedReader rf = new BufferedReader(new FileReader(this.inpath));
		String line, ltrain;
		while ((line = rf.readLine()) != null) {
			of.write(this.setData(line));
			of.newLine();
			fsize += 1;
		}
		rf.close();
		of.close();
		
		this.writeFeatures(this.fpath);
		System.out.println("[Info] data's class distribution: \n" + Parser.getStaticInfo()
				+ "\nfile size=" + fsize);
	}
	/** 特征名规划化。 */
	private String nameNormalize(String f) {
		if(f == null) {
			return "";  // 无效字符
		}
		
		return f.trim().replace(' ', '-').replace('%', '~')
				.replace('\'', '^').replace(',', '*');
	}

	/**
	 * 添加所有特征类目信息
	 */
	private String setAttribute() {
		dataBuf.delete(0, dataBuf.length());
		List<Entry<String, Integer>> forder = UtilObj
				.entrySort(features, false);
		for (Entry<String, Integer> as : forder) {
			// 替换掉空格 按arff格式标示
			dataBuf.append(String.format("@attribute %s {0, 1}\n", 
					as.getKey()));  
		}
		return dataBuf.toString();
	}

	// data 的稀疏数据格式， 仅保留清洗后的结果
	private String setData(String line) {
		if (line == null) {
			return "";
		}
		Integer tmp;
		Set<Integer> arrSet = new HashSet<Integer>();
		
		dataBuf.delete(0, dataBuf.length());
		dataBuf.append("{");

		// 基于新的解析方式。
		if(Parser.parseLine(line) == null)
			return "";
		Object[] fs = Parser.getFeatures();
		for(Object s: fs) {
			tmp = features.get((String)s);
			if(tmp != null)
				arrSet.add(tmp);
		}
			
		List<Integer> arr = Arrays.asList(arrSet.toArray(new Integer[0]));
		Collections.sort(arr);  // asc
		for(Integer t: arr) {
			dataBuf.append(String.format("%d 1%s", t, sepa));
		}
//		dataBuf.deleteCharAt(dataBuf.length() - sepa.length());
		// 添加目标字段信息
//		dataBuf.append(this.setClass(atom[idxClass]));
		dataBuf.append(String.format("%d %s", this.features.size(), 
				(String)this.Parser.getClassify()));
		String ctmp = (String )this.Parser.getClassify();
		dataBuf.append("}");
		// 添加实例的权重信息
		Float weight = (Float)Parser.getWeight();
		if(weight != null) 
			dataBuf.append(String.format(",  { %.4f }", weight));
		return dataBuf.toString();
	}
	
	private String setClass(String vclass) {
		if(vclass.equals("0")) {
			return "";
		}
		return String.format("%s%d 1", sepa, this.features.size()); 
	}
	
	private void writeFeatures(String path) throws IOException {
		if(path == null || this.features == null) 
			return;
		
		File f = new File(path);
		if (!f.exists()) {
			f.createNewFile();
		}
		BufferedWriter wbuf = new BufferedWriter(new FileWriter(path));
		wbuf.write(this.features.toString());
		wbuf.close();
	}

	/**
	 * 收集所有特征，形成索引ID 
	 */
	public void init(boolean cntfreq) throws IOException {
		BufferedReader rf = new BufferedReader(new FileReader(this.inpath));
		String line;
		String[] atom;
		String tmp;
		while ((line = rf.readLine()) != null) {
			/// 基于新的特征解析方式
			if(Parser.parseLine(line) == null) {
				System.out.println("[Info] init()\ninput=" + line
						+ "\nParse-result:\n" + Parser);
				continue;
			}
			Object[] fs = Parser.getFeatures();
			for(int i = 0; i < fs.length; ++i) {
				tmp = (String) fs[i];
				tmp = this.nameNormalize(tmp);
				if(tmp.length() == 0)
					continue;
				if(cntfreq) {
					// 统计频率信息
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
	
	public void setRegress(boolean b) {
		this.flagRegress = b;
	}
	public boolean getRegress() {
		return this.flagRegress;
	}
	
	/** 按频率裁剪特征集合的数量，生成新的特征顺序, 有效区间 [fmin, fmax] */
	public void featureTrim(Integer fmin, Integer fmax) {
		if(fmin == null || (fmax != null && fmax < fmin)) {
			return; //
		}
		int cnt = 0;
		int cnt_rm = 0, cnt_tot = 0, cnt_badkey = 0;  // for debug.  
		Integer f = 0;
		String k;
		Iterator<String> itr = this.features.keySet().iterator();
		while(itr.hasNext()) {
			cnt_tot += 1;
			k = itr.next();
			int [] scnt = UtilStr.strCharCnt(k);
			if(scnt != null && k.length() < 4 
					&& (scnt[1] + scnt[2] + scnt[3]) == k.length()) {
				// 按特征字 的内容过滤
				itr.remove();
				this.features.remove(k);
				cnt_badkey += 1;
				continue;
			}
			f = this.features.get(k);
			if(f < fmin || (fmax != null && f > fmax)) {
				cnt_rm += 1;
				itr.remove();
				this.features.remove(k);
			} else {
				this.features.put(k, cnt);  // 更新特征下标？
				cnt += 1;
			}
		}
		if(debug) {
			System.out.printf("[Info] DataNormalize::featureTrim() " +
					"fmin=%d\tfmax=%d\tcnt_total=%d\tbad_key=%d\tcnt_remove=%d\tcnt_retail=%d\n", 
					fmin, fmax, cnt_tot, cnt_badkey, cnt_rm, cnt);
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println("Usage: java -DEfeatureMin=3 -DEdebug=debug " +
				"-DEmodel=RegressClassFemal -DEweight=1 -DEout=outpath -DEin=inpath" +
				"-DEwmodel=Female -jar myjar.jar");
		System.out.println("[Info] " + System.getProperties());
		DataNormalize nor;
		try {
//			TODO 更新单测的代码
//			nor = new DataNormalize(
//					"D:/Data/MR-codes/data/test/test.arff",
//					"D:/Data/MR-codes/data/test/train.txt",
//					"com.emar.recsys.user.model.ins.ParseOrder");
			String reflectParser = "com.emar.recsys.user.model.ins.ParseOrder";
			String pin = System.getProperty("Ein");
			String pout = System.getProperty("Eout");
			String fmn = System.getProperty("EfeatureMin", "1");
			int fmin = Integer.parseInt(fmn);
//			String fmax = System.getProperty("featureMax", null);
			String debug = System.getProperty("Edebug", "debug"); 
			String fmodel = System.getProperty("Emodel", "BinarySex");
			String insWeight = System.getProperty("Ewmodel", "One");
//			String fweight = System.getProperty("Eweight", "0"); 
			
			nor = new DataNormalize(pout, pin, reflectParser);
			nor.Parser.init(fmodel, debug, insWeight);  //for ParseOrder.
			
			nor.debug = true;
			nor.init(true);
			System.out.println("[Info] before trim: " + nor.features.size());
			
			nor.featureTrim(fmin, null);
			System.out.println("[Test] after trim: " + nor.features.size());
			nor.writeData();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
