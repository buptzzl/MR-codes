package com.emar.util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.hive.ql.parse.HiveParser.booleanValue_return;
//import org.apache.hadoop.hive.ql.parse.HiveParser.primitiveType_return;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;

//import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 购物客数据检索类
 * 
 * @author zhoulm
 */
public final class MateridGWK { // extends UDF {

	private static final String gpath = "/hive/warehouse/materid/gwk_win_material_c201306.dat";
	private static final String lpath = "resourse/gwk_win_material_c201306.dat";

	private static HashMap<Integer, String[]> MaterList = new HashMap<Integer, String[]>();

	private static boolean initSucc = false;
	private static final String SEPA = ",";
	private static final int N = 13;
	
	public static final int idxKeyword = 0, idxHigh = 5, idxWide = 6, idxCTime = 11;

	static {
		initSucc = load(); // && loadDesc(); // ֧�ֱ�������
		System.out
				.println("[Info] MateridGWK::static load material data over!\nItem size="
						+ MaterList.size());
	}

	// 本地文件加载时的初始化
	public static void localInit() {
		initSucc = localLoad();
		System.out.println("[Info] load IP-data over!\nipAreaList size="
				+ MaterList.size());
	}

	public String getKeyword(int mid) {
		String[] arr = find(mid);
		if(arr != null) {
			return arr[0];
		}
		return null;
	}
	
	public String[] find(int mid) {
		if (!initSucc) {
			return null;
		}

		return this.MaterList.get(mid);
	}

	private static boolean load() {
		FSDataInputStream in = null;
		Configuration conf = new Configuration();
		FileSystem fs = null;

		try {
			fs = FileSystem.get(URI.create(gpath), conf);
			in = fs.open(new Path(gpath));
			String line = null;
			while ((line = in.readLine()) != null) {
				String[] arr = line.split(SEPA);
				if (arr.length >= N) {
					String[] val = new String[N - 1];
					System.arraycopy(arr, 1, val, 0, N - 1);
					MaterList.put(Integer.parseInt(arr[0]), val);
				}
			}
			MateridGWK.initSucc = true;
		} catch (IOException e) {
			e.printStackTrace();
			MateridGWK.initSucc = false;
		} finally {
			IOUtils.closeStream(in);
		}

		return MateridGWK.initSucc;
	}

	private static boolean localLoad() {
		boolean succ = false;
		try {
			InputStream ins = Ip2AreaUDF.class.getResourceAsStream(lpath);
			BufferedReader breader = new BufferedReader(new InputStreamReader(
					ins));
			String line = null;
			while ((line = breader.readLine()) != null) {
				String[] arr = line.split(SEPA);
				if (arr.length >= N) {
					String[] val = new String[N - 1];
					System.arraycopy(arr, 1, val, 0, N - 1);
					MaterList.put(Integer.parseInt(arr[0]), val);
				}
			}
			breader.close();
			MateridGWK.initSucc = true;
		} catch (IOException e) {
			e.printStackTrace();
			MateridGWK.initSucc = false;
		}
		return MateridGWK.initSucc;
	}

	private static MateridGWK mater = null;

	private MateridGWK() {
	}
	public static MateridGWK getInstance() {
		if (mater == null) {
			mater = new MateridGWK();
			if (mater.initSucc == false) {
				mater.initSucc = MateridGWK.localLoad();
				if (mater.initSucc == false) {
					mater = null;
					System.out
							.print("[ERROR] Ip2AreaUFD failed to get instance.");
				}
			}
		}
		return mater;
	}

	public static void main(String[] args) {
		MateridGWK g = MateridGWK.getInstance();
		String[] a = g.find(10405);
		System.out.print("[UnitTest] "
				+ "\n" + Arrays.asList(a)  + 
				"\n"+ a[MateridGWK.idxKeyword] + "\t" + a[MateridGWK.idxHigh] + 
				"\t" + a[MateridGWK.idxWide] + "\t" + a[MateridGWK.idxCTime]
				+ "\n" + g.find(10));
	}
}
