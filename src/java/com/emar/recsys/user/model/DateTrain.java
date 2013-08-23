package com.emar.recsys.user.model;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import com.emar.recsys.user.feature.FeatureDriver;


/**
 * 基于PV&Click日志抽取 LR训练用数据
 * @author zhoulm
 * 
 */
public class DateTrain {
	private HashMap<String, String> clickid;
	private HashSet<String> clickad;  // 广告
	private HashSet<String> clickcamp;  // 创意
//	private FeatureDriver fdriver;
	private Random random;
	
	private static final String SEPA = "\u0001";
	private int idxPvUid, idxCamp, idxTime;
	private int N;
	private int CntClick, CntClcBad, CntClcEmp, CntClcBadCase, CntClcTotal,
		CntPV, CntPVBad, CntPVEmp, CntPVBadCase, 
		CntOrder, CntOrdBad, CntOrdEmp;
	
	public enum STATUS{ NULL, CLICK, PV, ORDER };
	public STATUS Stat; 
	
	public DateTrain() {
		clickid = new HashMap<String, String>(1<<20);
		clickad = new HashSet<String>(1<<20);
		clickcamp = new HashSet<String>(1<<20);
//		fdriver = FeatureDriver.getInstance();
		Stat = STATUS.NULL;
		random = new Random();
		CntClick = 0; 
		CntPV = 0;
		CntOrder = 0;
		CntClcBad = 0; 
		CntPVBad = 0;
		CntOrdBad = 0;
		CntClcEmp = 0;
		CntPVEmp = 0;
		CntOrdEmp = 0;
		CntClcBadCase = 0;
		CntPVBadCase = 0;
		CntClcTotal = 0;
	}
	
	public void printJob() {
		System.out.println("[Info] DateTrain::static-info:" 
				+ "\nCntClick=" + CntClick + "\t CntClcBad=" + CntClcBad + "\tCntClcEmp=" + CntClcEmp + "\tCntClcBadCase=" + CntClcBadCase
				+ "\nCntPV=" + CntPV + "\tCntPVBad=" + CntPVBad + "\tCntPVEmp=" + CntPVEmp + "\tCntPVBadCase=" + CntPVBadCase
				+ "\nCntOrder=" + CntOrder + "\tCntOrdBad=" + CntOrdBad + "\tCntOrdEmp=" + CntOrdEmp
				+ "\nClick-map-size=" + clickad.size()
				);
	}
	
	public void setClickIdx() {
		this.N = 11;
		this.idxPvUid = 1;
		this.idxCamp = 10;
		this.idxTime = 7;
		
		FeatureDriver.idxAd = 9;  // mate_id
		FeatureDriver.idxDate = 7;
		FeatureDriver.idxIP = 6;
		FeatureDriver.idxUA = 5;
		FeatureDriver.idxUrl = 3;
		Stat = STATUS.CLICK;
	}
	public void setPVIdx() {
		this.N = 10;
		this.idxPvUid = 0;
		this.idxCamp = 9;
		this.idxTime = 6;
		
		FeatureDriver.idxAd = 8;  // mate_id
		FeatureDriver.idxDate = 6;
		FeatureDriver.idxIP = 5;
		FeatureDriver.idxUA = 4;
		FeatureDriver.idxUrl = 2;
		Stat = STATUS.PV;
	}
	public void setOrderIdx() {
		// TODO
	}
	
	// 对点击日志生成向量
	public void getClickData(List<String> clickpath, String outpath) throws IOException {
		this.setClickIdx();
		
		FSDataInputStream in = null;
		Configuration conf = new Configuration();
		FileSystem fs = null;
		String line;
		String[] atom;
		
//		FSDataOutputStream out = null;
//		out = ofs.create(new Path(outpath));
		FileWriter out = new FileWriter(new File(outpath));
//		FileSystem ofs = FileSystem.get(URI.create(outpath), conf);
		
		for(String path: clickpath) {
			try {
				fs = FileSystem.get(URI.create(path), conf);
				in = fs.open(new Path(path));
				String content;
				while ((line = in.readLine()) != null) {
					try {
						atom = line.trim().split(SEPA);
//						System.out.println(this.fdriver.evaluate(line, SEPA, N));
						String flist = FeatureDriver.evaluateList(atom, N, null);
						if(flist == null) {
							CntClcEmp += 1;
							continue;
						}
						clickad.add(atom[FeatureDriver.idxAd]);
//						out.write("1, " + flist + "\n".getBytes());
						out.write("1, " + flist + "\n");
//						IOUtils.copyBytes(in, out, conf)
						this.clickid.put(atom[idxPvUid], atom[idxTime]);
						CntClick += 1;
//						if((CntClick % 128) == 0) {
//							System.out.println("[Info] DateTrain::getClickData() flush&sync data, size=" + CntClick);
//							out.flush();
//							out.sync();
//						}
					} catch (ParseException e) {
						CntClcBad += 1;
					}
				}
			} catch (IOException e1) {
//				e1.printStackTrace();
			} finally {
				IOUtils.closeStream(in);
			}
		}
//	IOUtils.closeStream(out);
		out.close();
		
		this.printJob();
	}
	// 对展示日志生成向量
	public void getPVData(List<String> pvpath, String outpath, int rate) throws IOException {
		if(clickid == null || clickid.size() == 0) {
			return;  // data uncomplete.
		}
		this.setPVIdx();
		Calendar c = Calendar.getInstance();
		SimpleDateFormat d = new SimpleDateFormat("yyyyMMddHHmmss");
		
		FSDataInputStream in = null;
		Configuration conf = new Configuration();
		FileSystem fs = null;
		String line, time;
		String[] atom;
		
//		FSDataOutputStream out = null;
//		out = ofs.create(new Path(outpath));
		FileWriter out = new FileWriter(new File(outpath));
		FileSystem ofs = FileSystem.get(URI.create(outpath), conf);
		
		for(String path: pvpath) {
			try {
				fs = FileSystem.get(URI.create(path), conf);
				in = fs.open(new Path(path));
				while ((line = in.readLine()) != null) {
					if(random.nextInt(rate) != 0) {
						continue;
					}
					try {
						atom = line.trim().split(SEPA);
						time = this.clickid.get(atom[idxPvUid]);
						if(time != null) { // bad case check.
							c.setTime(d.parse(time));
							c.add(Calendar.HOUR_OF_DAY, -1);
							if(atom[idxTime].compareTo(d.format(c.getTime())) < 0) {
								CntPVBadCase += 1;
								continue;
							}
							c.add(Calendar.HOUR_OF_DAY, 1); // 
							c.add(Calendar.SECOND, -3);  
							if(atom[idxTime].compareTo(d.format(c.getTime())) > 0) {
								CntPVBadCase += 1;
								continue;
							}
						}
						String flist = FeatureDriver.evaluateList(atom, N, clickad);
						if(flist == null) {
							CntPVEmp += 1;
							continue;
						}
//						out.writeBytes("0, " + flist + "\n");
						out.write("0, " + flist + "\n");
						CntPV += 1;
//						if((CntPV % 4096) == 0) {
//							System.out.println("[Info] DateTrain::getPVData() flush&sync data, size=" + CntPV);
//							out.flush();
//							out.sync();
//						}
					} catch (ParseException e) {
						CntPVBad += 1;
					}
				}
			} catch (IOException e1) {
				e1.printStackTrace();
			} finally {
				IOUtils.closeStream(in);
			}
		}
		
//	IOUtils.closeStream(out);
	out.close();
	
		this.printJob();
	}
	// 对订单日志得到向量
	public void getOrderData() {
		// TODO
	}

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		System.out.println("[Info] input args:\t " + Arrays.asList(args));
		if(args.length != 4) {
			System.out.println("[Args Error] Usage: <inclick> <outclick> <inpv> <outpv>");
			System.exit(1);
		}
		Configuration conf = new Configuration();
		FileSystem fsys = FileSystem.get(URI.create(args[0]), conf);
		FileStatus[] fclcState = fsys.globStatus(new Path(args[0]));
		List<String> pclc = new ArrayList<String>();
		for(FileStatus fs: fclcState) {
			pclc.add(fs.getPath().toString());
			System.out.println("[Info] clickpath:\t" + pclc.get(pclc.size()-1));
		}
		String outclc = args[1];
		
		fsys = FileSystem.get(URI.create(args[2]), conf);
		FileStatus[] fpvState = fsys.globStatus(new Path(args[2]));
		List<String> ppv = new ArrayList<String>();
		for(FileStatus fs: fpvState) {
			ppv.add(fs.getPath().toString());
			System.out.println("[Info] pvpath:\t" + ppv.get(ppv.size()-1));
		}
		String outpv = args[3];	
		
		Date startTime = new Date(); 
		DateTrain dt = new DateTrain();
		dt.getClickData(pclc, outclc);
		dt.getPVData(ppv, outpv, 500);
		
		Date end_time = new Date(); 
		System.out.println("[INFO] The job took " + 
		    (end_time.getTime() - startTime.getTime()) /1000 + " seconds."); 
	}

}
