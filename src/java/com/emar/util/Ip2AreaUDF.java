package com.emar.util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @author jiaqiang, zhoulm
 * 2012.06.07
 */
public final class Ip2AreaUDF { //  extends UDF {
	
	//ip����ӳ���ļ�·��
	// "/hive/warehouse/p_ip_dstc_map/ip_dstc.dat";
	private static String ipAreaMapPath = "/hive/warehouse/p_ip_dstc_map/ip_dstc_ne.dat";
	private static final String locIpAreaMapPath = "resourse/ip_dstc_ne.dat";
	private static String ipDescPath = "/hive/warehouse/p_ip_dstc_map/area_mn_desc.dat";
	
	private static List<IpArea> ipAreaList = new ArrayList<IpArea>();
	private static HashMap<String, String> areaInfo = new HashMap<String, String>();
	
	//��ʹ���ɹ���ʶ
	private static boolean initSucc = false;
	
	static {
		initSucc = load();  // && loadDesc();  // ֧�ֱ�������
		if (initSucc) {
//			areaInfo.put("null", "0");  // abroad IP
			//���� 
			Collections.sort(ipAreaList);
		}
		System.out.println("[Info] static load IP-data over!\nipAreaList size="
				+ ipAreaList.size() + "\tareainfo size=" + areaInfo.size());
	}
	
	// 本地文件加载时的初始化
	public static void localInit() {
		initSucc = localLoad() && localLoadDesc();
		if(initSucc) {
//			areaInfo.put("null", "0");  // abroad IP
			Collections.sort(ipAreaList);
		}
		System.out.println("[Info] load IP-data over!\nipAreaList size="
				+ ipAreaList.size() + "\tareainfo size=" + areaInfo.size());
	}
	
	/**
	 * ���ipת������ID
	 * @param ip ���ip��ַ����"172.16.2.93"
	 * @return
	 */
	public Text evaluate(final Text ip) {
		if (!initSucc) { //��ʹ��ʧ��
			return null;
		}
		
		if (ip == null) {
			return null;
		}
				
		String ipStr = ip.toString().trim();
		if (ipStr == null || ipStr.length() == 0) {
			return null;
		}
		
		//���ipת���ɳ�����
		Long ipValue = ip2long(ipStr);
		if (ipValue == null) {
			return null;
		}

		//���Ҳ���������id
		return new Text(find(ipValue)+"");
	}
	
	public Integer evaluateStr(String ip) {
		if(!initSucc || ip == null) {
			return null;
		}
		
		Long ipValue = this.ip2long(ip.trim());
		if (ipValue == null) {
			return null;
		}
		
		return find(ipValue);
	}
	public String[] evaluateMore(String ip) {
		if(!initSucc || ip == null) {
			return null;
		}
		
		Long ipValue = this.ip2long(ip.trim());
		if (ipValue == null) {
			return null;
		}
		
		return findMore(ipValue);
	}
	
	/**
	 * �����ipת���ɳ�����
	 * @param ip
	 * @return
	 */
	private Long ip2long(String ip) {
		String[] arr = ip.split("\\.");
		int len = arr.length;
		if (len != 4) { //�Ƿ���ʾ
			return null;
		}
		
		Long num = 0L;
		try {
			for (int i = 0; i <= 3; i++) {
				num += (Long.parseLong(arr[i]) << ((len-i-1) * 8));
			}
		} catch (Exception ex) {
			num = null;
		}
		
		return num;
	}

	/**
	 * ��hdfs�м���ip�����ļ�
	 * @return
	 */
	private static boolean load() {
		boolean succ = true;
		
		FSDataInputStream in = null;
		Configuration conf = new Configuration();
		FileSystem fs = null;
		
		try {
			fs = FileSystem.get(URI.create(ipAreaMapPath), conf);
			in = fs.open(new Path(ipAreaMapPath));
			String line = null;
			IpArea ipArea = null;
			
			while ((line = in.readLine()) != null) {
				line = line.replace("\u0001", " \u0001");  // ����ո�
				String[] arr = line.split("\u0001");
				if (arr.length >= 3) { //��Чֵ
					ipArea = new IpArea();
					try {
						ipArea.setStartIp(Long.parseLong(arr[0].trim()));
						ipArea.setEndIp(Long.parseLong(arr[1].trim()));
						String area = arr[2].trim();
						ipArea.setAreaId(area.length() > 0 ? Integer.parseInt(area) : null);
						if(arr.length == 7) {  // more info.
							String[] more = new String[5];
							System.arraycopy(arr, 2, more, 0, 5);
							ipArea.setAreaDesc(more);
						}
						
						if (!ipAreaList.contains(ipArea)) { //���������
							ipAreaList.add(ipArea);
						}
					} catch (Exception ex) {
						continue;
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
			succ = false;
		} finally {
			IOUtils.closeStream(in);
		}
		
		return succ;
	}
	
	private static boolean loadDesc() {
		boolean succ = true;
		
		FSDataInputStream in = null;
		Configuration conf = new Configuration();
		FileSystem fs = null;
		
		try {
			fs = FileSystem.get(URI.create(ipDescPath), conf);
			in = fs.open(new Path(ipDescPath));
			String line;
			while ((line = in.readLine()) != null) {
				String[] atoms = line.split(",");
				if(atoms.length == 4) {
					areaInfo.put(atoms[0], line.substring(line.indexOf(",")+1));
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
			succ = false;
		} finally {
			IOUtils.closeStream(in);
		}
		return succ;
	}
	
	private static boolean localLoadDesc() {
		try {
			BufferedReader breader = new BufferedReader(new FileReader(ipDescPath));
			String line = null;
			String[] atoms = null;
			while((line = breader.readLine()) != null) {
				atoms = line.split(",");
				if(atoms.length == 2) {
					areaInfo.put(atoms[0], atoms[1]);
				}
			}
			breader.close();
		} catch (IOException e) {
			return false;
		}
		
		return true;
	}
	private static boolean localLoad() {
		try {
			InputStream ins = Ip2AreaUDF.class.getResourceAsStream(locIpAreaMapPath);
			BufferedReader breader = new BufferedReader(new InputStreamReader(ins));
//			BufferedReader breader = new BufferedReader(new FileReader(locIpAreaMapPath));

			String line = null;
			IpArea ipArea = null;
			while((line = breader.readLine()) != null) {
				line = line.replace("\u0001", " \u0001");  // ����ո�
				String[] arr = line.split("\u0001");
				if (arr.length >= 3) { //��Чֵ
					ipArea = new IpArea();
					try {
						ipArea.setStartIp(Long.parseLong(arr[0].trim()));
						ipArea.setEndIp(Long.parseLong(arr[1].trim()));
						String area = arr[2].trim();
						ipArea.setAreaId(area.length() > 0 ? Integer.parseInt(area) : null);
						if(arr.length == 7) {  // more info.
							String[] more = new String[4];
							System.arraycopy(arr, 2, more, 0, 4);
							ipArea.setAreaDesc(more);
						}
						if (!ipAreaList.contains(ipArea)) { //���������
							ipAreaList.add(ipArea);
						}
					} catch (Exception ex) {
						continue;
					}
				}
			}
			breader.close();
			
			return true;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}
	
	/**
	 * ����ip��Ӧ������id
	 * @param ip
	 * @return ��������ID,���򷵻�null
	 */
	private Integer find(long ip) {
		Integer areaID = null;
		/*
		IpArea ipArea = new IpArea();
		ipArea.setStartIp(ip);
		ipArea.setEndIp(ip);
		*/
		int low = 0;
		int high = ipAreaList.size() - 1;
		
		while (low <= high) {
			int middle = (low + high) / 2;
			IpArea tmp = ipAreaList.get(middle);
			long s = tmp.getStartIp().longValue();
			long e = tmp.getEndIp().longValue();
			
			if (ip >= s && ip <= e) { //�ڼ�,�ҵ�
				areaID = tmp.getAreaId();
				break;
			} else if (ip < s) {
				high = middle - 1;
			} else {
				low = middle + 1;
			}
		}
		
		return areaID;
	}
	private String[] findMore(long ip) {
		String[] areaID = null;
		int low = 0;
		int high = ipAreaList.size() - 1;
		
		while (low <= high) {
			int middle = (low + high) / 2;
			IpArea tmp = ipAreaList.get(middle);
			long s = tmp.getStartIp().longValue();
			long e = tmp.getEndIp().longValue();
			
			if (ip >= s && ip <= e) { //�ڼ�,�ҵ�
				areaID = tmp.getAreaDesc();
				break;
			} else if (ip < s) {
				high = middle - 1;
			} else {
				low = middle + 1;
			}
		}
		
		return areaID;
	}
	
	public String getIPPath() {
		return ipAreaMapPath;
	}
	public void setIPPath(String path)	{
		if(path == null) {
			return ;
		}
		
		ipAreaMapPath = path;
		return ;
	}
	
	public String getProvince(String cityid) {
		if(cityid == null || !areaInfo.containsKey(cityid)) {
			return null;
		}
		return areaInfo.get(cityid);
	}
	public String getIPDescPath() {
		return ipDescPath;
	}
	public void setIPDescPath(String path)	{
		if(path == null) {
			return ;
		}
		ipDescPath = path;
	}
	
	/**
	 * IP����ӳ����
	 */
	static class IpArea implements Comparable<IpArea> {
		Long startIp;
		Long endIp;
		Integer areaId;
		String[] areaDesc; 
		
		@Override
		public int compareTo(IpArea o) {
			return startIp.compareTo(o.startIp);
		}

		public String toString() {
			return "{'start_ip':" + startIp + ",'end_ip':" + endIp + ",'area_id':" 
					+ areaId + "}";
		}
		
		public Long getStartIp() {
			return startIp;
		}

		public void setStartIp(Long startIp) {
			this.startIp = startIp;
		}

		public Long getEndIp() {
			return endIp;
		}

		public void setEndIp(Long endIp) {
			this.endIp = endIp;
		}

		public Integer getAreaId() {
			return areaId;
		}

		public void setAreaId(Integer areaId) {
			this.areaId = areaId;
		}
		
		public void setAreaDesc(String[] desc) {
			this.areaDesc = desc;
		}
		public String[] getAreaDesc() {
			return this.areaDesc;
		}
		public String getOneArea(Integer i) {
			if(i < 0 || this.areaDesc.length <= i) {
				return null;
			}
			return this.areaDesc[i];
		}
		
		@Override
		public boolean equals(Object obj) {
			IpArea ia = (IpArea) obj;
			return ia.startIp.equals(this.startIp) && ia.endIp.equals(this.endIp) && ia.areaId.equals(this.areaId);
		}
	}
	
	private static Ip2AreaUDF iparea = null;
	private Ip2AreaUDF() { }
	public static Ip2AreaUDF getInstance() {
		if(iparea == null) {
			iparea = new Ip2AreaUDF();
			if(iparea.initSucc == false) {
				iparea.initSucc = Ip2AreaUDF.localLoad();
				if(iparea.initSucc == false) {
					iparea = null;
					System.out.print("[ERROR] Ip2AreaUFD failed to get instance.");
				}
			}
		}
		return iparea;
	}

	public static void main(String[] args) {
		Ip2AreaUDF ip = Ip2AreaUDF.getInstance();
		/*// 本地运行版本
		ip.setIPDescPath("D:/Data/Resources/area_mn_map.dat");  
//		ip.getIPDescPath();
		ip.setIPPath("D:/Data/Resources/ip_dstc_ne.dat");
//		ip.getIPPath();
		ip.localInit();
		*/
		System.out.print("[Info] " + ip.ipAreaList.size());
		String[] more = ip.evaluateMore("14.151.196.228");
		
		System.out.print("[UnitTest] \n"
				+ ip.evaluateStr("92.34.11.1")
				+ "\n" + ip.evaluateStr("14.151.196.228")
				+ "\nlen=" + more.length + "\tval1="+ more[0] + "\tval2=" + more[1]
				);
	}
}
