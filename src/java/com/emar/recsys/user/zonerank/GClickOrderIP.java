package com.emar.recsys.user.zonerank;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.emar.recsys.user.util.PriorPair;
import com.emar.recsys.user.util.UtilObj;
import com.emar.recsys.user.util.UtilStr;
import com.emar.util.Ip2AreaUDF;
import com.emar.util.exp.UrlPair;

/**
 * TODO UnitTest 从点击日志中收集 campid, userid, prod-info; 合并订单数据。 采用类似二次排序策略，
 * 优先分发Rank 对应的Mapper
 * 
 * @author zhoulm
 * 
 */
public class GClickOrderIP extends Configured implements Tool {
	// 定义Map & Reduce 中通用的对象
	private static final String MR_sepa = "\u0001", SComb = "@@@";
	private static final SimpleDateFormat datefmt = new SimpleDateFormat(
			"yyyyMMddHHmmss");

	// Reduce &MO setting info.
	public static final String moIp = "ip", IpDir = "ip/", moZone = "zone",
			ZoneDir = "zone/", moUid = "uid", UidDir = "uid/";

	public static class MapClick extends
			Mapper<LongWritable, Text, PriorPair, Text> {
		private static final int LenClc = 10;
//		private static final int IdxIP = 2, IdxPUid = 3, IdxDate = 4, IdxCamp = 5;
		// chuchuang
		private static final int IdxIP = 6, IdxPUid = 2, IdxDate = 7, IdxCamp = 10;
		private static final int IpLen = 5;
		private static final String[] IpDef = new String[] { "-1", "-1", "-1",
				"-1", "-1" };
		private static final String PCamp = "uCampRank", PUser = "uClassRank";

		private static Calendar c = Calendar.getInstance();
		private static Ip2AreaUDF iparea = Ip2AreaUDF.getInstance();
		private Text oval = new Text();
		private PriorPair okey = new PriorPair();
		private String PType;
		private int keyIdx;

		private static enum Counters {
			Useless, InC, OC, OCBad, UMout, UnIP, OIP
		};

		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			if (conf.getBoolean("isuid", false)) {
				PType = PUser;
				keyIdx = IdxPUid;
			} else {
				PType = PCamp;
				keyIdx = IdxCamp;
			}
			System.out.println("[Info] GClickOrderIP::MapClick::setup(1) para:isuid="
					+ conf.getBoolean("isuid", false) + "\tPType=" + PType
					+ "\tlog-key-index=" + keyIdx);
		}

		public void map(LongWritable key, Text val, Context context) {
			context.getCounter(Counters.InC).increment(1);

			String inpath = ((FileSplit) context.getInputSplit()).getPath()
					.toString();
			String[] rankinfo;
			if (inpath.indexOf(PType) >= 0) {
				rankinfo = val.toString().split("\t");
				// okey.set(String.format(MR_kcfmt + "%s" + MR_sub,
				// rankinfo[0]));
				okey.setFirst(rankinfo[0]); // campid or userid.
				okey.setFlag(false); // 升序时， Reduce端先到达
				oval.set(rankinfo[1]); // rank-info.
				try {
					context.write(okey, oval);
					context.getCounter(Counters.OC).increment(1);
				} catch (Exception e) {
					e.printStackTrace();
					context.getCounter(Counters.OCBad).increment(1);
				}
			} /*
				 * else if(inpath.indexOf(PUser) >= 0) { rankinfo =
				 * val.toString().split("\t"); okey.set(String.format(MR_kufmt + "%s" +
				 * MR_sub, rankinfo[0])); oval.set(rankinfo[1]);
				 * context.getCounter(Counters.InU).increment(1); try {
				 * context.write(okey, oval);
				 * context.getCounter(Counters.OU).increment(1); } catch (Exception e) {
				 * context.getCounter(Counters.OUBad).increment(1); } }
				 */else {
				String line = val.toString();
				line = line.replace("\u0001", "\u0001 ");
				String[] atom = line.split("\u0001");
				if (atom.length < LenClc) {
					context.getCounter(Counters.Useless).increment(1);
				} else {
					String ip = atom[IdxIP].trim();
					String[] ipinfo = iparea.evaluateMore(ip);

					List<String> ipzone;
					if (ipinfo == null || ipinfo.length != IpLen) {
						// System.err.println("[Error] map()" + ip + "\t" + ipinfo);
						ipzone = Arrays.asList(IpDef);
						context.getCounter(Counters.UnIP).increment(1);
					} else {
						ipzone = Arrays.asList(ipinfo);
					}
					// String platuser = atom[IdxPUid].trim();
					// String campid = atom[IdxCamp].trim();
					String outkey = atom[keyIdx].trim();
					//@for chuchuang
					Set<String> keyset = new HashSet<String>();
					for(String s: outkey.split(",")) {
						if(s.length() != 0) 
							keyset.add(s);
					}
					
					String uid = atom[IdxPUid].trim();
//					inpath = inpath.substring(0, inpath.lastIndexOf('/'));
//					String platid = inpath.substring(inpath.lastIndexOf('/'));//末尾两个//间值为platid
					String plat = inpath.substring(inpath.lastIndexOf('/')).split("_")[1];
					
					try {
						okey.setFirst(outkey);
						okey.setFlag(true);
						oval.set(ip + MR_sepa + ipzone.toString() + MR_sepa + uid + SComb + plat);
//						context.write(okey, oval);
						//@for chuchuang
						for(String s: keyset) {
							okey.setFirst(s);
							context.write(okey, oval);
						}
						context.getCounter(Counters.OIP).increment(1);
					} catch (Exception e) {
						e.printStackTrace();
						context.getCounter(Counters.UMout).increment(1);
					}
				}
			}
		}

		public void cleanup(Context context) {
		}
	}

	public static class FPairPartition extends Partitioner<PriorPair, Text> {
		@Override
		public int getPartition(PriorPair key, Text value, int numPartitions) {
			return Math.abs(key.getFirst().hashCode()) % numPartitions;
		}
	}

	public static class SortComparator extends WritableComparator {
		private PriorPair first, second;

		protected SortComparator() {
			super(PriorPair.class, true);
		}

		public int compare(WritableComparable a, WritableComparable b) {
			first = (PriorPair) a;
			second = (PriorPair) b;
			return first.compareTo(second);
		}

	}

	/**
	 * 按 first 字段归并 value-list
	 */
	public static class KeyGrouping extends WritableComparator {
		private PriorPair k1, k2;

		protected KeyGrouping() {
			super(PriorPair.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			k1 = (PriorPair) a;
			k2 = (PriorPair) b;
			return k1.getFirst().compareTo(k2.getFirst());
		}
	}

	public static class ReduceClick extends Reducer<PriorPair, Text, Text, Text> {

		private static enum Counters {
			ReduceIn, MinRank, MultiRank, Ip, IpErr, IpzoneErr, OrderErr, ROIP, UnROIp, ROZone, UnROZone, ROUid, UnROUid
		};

		private MultipleOutputs<Text, Text> mos = null;
		private Text okey = new Text(), oval = new Text();

		public void setup(Context context) throws IOException, InterruptedException {
			mos = new MultipleOutputs(context);
			super.setup(context);
		}

		public void reduce(PriorPair key, Iterable<Text> values, Context context) {
			String rkey = key.toString();

			String rank = null;
			String tmp;
			String[] arrTmp;
			Set<String> ipSet = new HashSet<String>(), 
					zoneSet = new HashSet<String>(), 
					uidSet = new HashSet<String>();
			// StringBuffer testBuf = new StringBuffer(); //@Test

			context.getCounter(Counters.ReduceIn).increment(1);
			for (Text t : values) {
				// testBuf.append("\n" + t.toString());
				tmp = t.toString();
				if (tmp.startsWith("[")) {
					if (rank == null) {
						Map<String, Float> mrank = UtilObj.Str2Map(tmp, "[]", ", ");
						if(mrank.size() > 1000) {
							List<Entry<String, Float>> slist = UtilObj.entrySortFloat(mrank, true);
							slist = slist.subList(0, 1000);  // 裁剪
							if(slist.get(0).getValue() < 10) 
								slist = slist.subList(0, 10);
							rank = slist.toString();
						} else {
							rank = tmp;
						}
						/*
						// 对 Rank-out 添加对应的来源字段
						// rank = tmp;
						String[] atoms = UtilStr.str2arr(rank);
						if (atoms.length > 1000) { // 裁剪rank信息
							int idxval = atoms[0].indexOf('=');
							float val = Float.parseFloat(atoms[0].substring(idxval).trim());
							int maxlen = 1000;
							if (val < 10) {
								maxlen = 10; // 长度裁剪
							}
							StringBuffer sbuf = new StringBuffer("[");
							for (int i = 0; i < maxlen; ++i) {
								idxval = atoms[i].indexOf('=');
								sbuf.append(atoms[i]);
								sbuf.append(", ");
							}
							sbuf.append("]");
							rank = sbuf.toString().replace(", ]", "");
						}
						*/

						oval.set(rank);
						context.getCounter(Counters.MinRank).increment(1);
					} else {
						context.getCounter(Counters.MultiRank).increment(1);
						System.out
								.print("[ERROR] GClcOrdIP::Reduce() rank duplicate.\nBaseRank:\t"
										+ rank + "\nNewRank:\t" + tmp);
					}
				} else {
					context.getCounter(Counters.Ip).increment(1);
					// key对应的value中没有Rank信息 或 Rank信息没有最先到达, 直接跳出
					if (rank == null) {
						context.getCounter(Counters.OrderErr).increment(1);
						return;
					}
					arrTmp = tmp.split(MR_sepa);
					if (arrTmp.length < 3) { // Map-out 结果不正常
						context.getCounter(Counters.IpErr).increment(1);
						System.out.print("[ERROR] GClcOrdIP::Reduce() map-out IPinfo:\t"
								+ tmp);
					} else {
						ipSet.add(arrTmp[0]);

						arrTmp = UtilStr.str2arr(arrTmp[1]); // 字符串转换
						if (arrTmp.length != 5) {
							context.getCounter(Counters.IpzoneErr).increment(1);
						} else {
							for (String s : arrTmp) {
								zoneSet.add(s);
							}
						}

						uidSet.add(arrTmp[2]);
					}
				}
			}
			
			for(String s: ipSet) {
				okey.set(s);
				try {
					mos.write(moIp, okey, oval, IpDir); // key=ip
					context.getCounter(Counters.ROIP).increment(1);
				} catch (Exception e) {
					e.printStackTrace();
					context.getCounter(Counters.UnROIp).increment(1);
				}
			}
			
			for(String s: zoneSet) {
				okey.set(s);
				try {
					mos.write(moZone, okey, oval, ZoneDir); // key=zone
					context.getCounter(Counters.ROZone).increment(1);
				} catch (Exception e) {
					e.printStackTrace();
					context.getCounter(Counters.UnROZone).increment(1);
				}
			}
			
			for(String s: uidSet) {
				okey.set(s);
				try {
					mos.write(moUid, okey, oval, UidDir);
					context.getCounter(Counters.ROUid).increment(1);
				} catch (Exception e) {
					e.printStackTrace();
					context.getCounter(Counters.UnROUid).increment(1);
				}
			}
		}

		public void cleanup(Context context) {
			if (mos == null) {
				return;
			}
			try {
				mos.close();
				super.cleanup(context);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		List<String> arr = Arrays.asList(args);
		System.out.println("[Info] input-args: " + arr);
		if (otherArgs.length < 4) {
			System.out.println("Usage: <in1> <in2> <out> isuid ");
			System.exit(2);
		}

		conf.setBoolean("isuid", otherArgs[3].equals("user"));
		Job job = new Job(conf, "[user and camp itemclassify rank]");
		job.setJarByClass(GClickOrderIP.class);
		job.setMapperClass(MapClick.class);
		job.setMapOutputKeyClass(PriorPair.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(ReduceClick.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(16);

		job.setPartitionerClass(FPairPartition.class);
		job.setSortComparatorClass(SortComparator.class);
		// job.setCombinerClass(.class);
		job.setGroupingComparatorClass(KeyGrouping.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		Path[] input_tot = FileInputFormat.getInputPaths(job);
		for (Path p : input_tot) {
			FileSystem fsystem = p.getFileSystem(job.getConfiguration());
			// FileStatus pstat = fsystem.getFileStatus(p); // For test.
			if (fsystem.isFile(p)) {
				System.out.println("[Info] inPath:" + p.toString());
			} else {
				FileStatus[] input_fs = fsystem.globStatus(p);
				for (FileStatus ps : input_fs) {
					System.out.println("[Info] inPath:" + ps.getPath());
				}
			}
		}

		MultipleOutputs.addNamedOutput(job, moIp, TextOutputFormat.class,
				Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, moZone, TextOutputFormat.class,
				Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, moUid, TextOutputFormat.class,
				Text.class, Text.class);

		Date startTime = new Date();
		job.waitForCompletion(true);
		Date end_time = new Date();
		System.out.println("The job took "
				+ (end_time.getTime() - startTime.getTime()) / 1000 + " seconds.");
		return 0;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		int res;
		try {
			res = ToolRunner.run(new Configuration(), new GClickOrderIP(), args);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

}
