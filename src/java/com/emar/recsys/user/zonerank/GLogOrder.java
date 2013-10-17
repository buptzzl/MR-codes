package com.emar.recsys.user.zonerank;

import java.io.IOException;
import java.text.ParseException;
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

import com.emar.recsys.user.log.LogParse;
import com.emar.recsys.user.util.UtilObj;
import com.emar.recsys.user.util.UtilStr;
import com.emar.recsys.user.util.mr.HdfsIO;
import com.emar.recsys.user.util.mr.PriorPair;
import com.emar.util.Ip2AreaUDF;

/**
 * 基于 Uid 链接 itemclass&IPinfo； 将无法链接的uid单独输出，为下一步处理
 * @after Iclassify2
 * @discard 基于订单的用户UID，归并出点击(表达了用户的兴趣)中的地域排行。
 * 
 * @author zhoulm
 * 
 */
public class GLogOrder extends Configured implements Tool {
	// 定义Map & Reduce 中通用的对象
	private static final String SEPA = LogParse.SEPA, SEPA_MR = LogParse.SEPA_MR,
			PLAT = LogParse.PLAT, EMAR = LogParse.EMAR, MAGIC = LogParse.MAGIC;

	// Reduce &MO setting info.
	public static final String moIp = "ip", IpDir = "ipRank/", moZone = "zone",
			ZoneDir = "zoneRank/", moUidIp = "uidip", moUidIpDir = "uid2IP/";

	public static class MapCombine extends
			Mapper<LongWritable, Text, PriorPair, Text> {
		private static final String PUser = "uUserRank";

		private Text oval = new Text();
		private PriorPair okey = new PriorPair();

		private static Ip2AreaUDF iparea;
		private LogParse logparse;
		private String PType;

		private static enum Counters {
			ErrInit, ErrMo, ErrParse, ErrMoRank, ErrIP, 
			MoRank, MoIP
		};

		public void setup(Context context) {
			iparea = Ip2AreaUDF.getInstance();
			// 日志解析初始化
			try {
				logparse = new LogParse();
			} catch (ParseException e) {
				context.getCounter(Counters.ErrInit).increment(1);
				e.printStackTrace();
				System.exit(e.getErrorOffset());
			}

			// Configuration conf = context.getConfiguration();
			PType = PUser;
		}

		public void map(LongWritable key, Text val, Context context) {

			String path = ((FileSplit) context.getInputSplit()).getPath().toString();
			if (path.indexOf(PType) >= 0) {
				String[] rankinfo = val.toString().split(SEPA_MR);
				okey.setFirst(rankinfo[0]); // campid or userid.
				okey.setFlag(false); // 升序时， Reduce端先到达
				oval.set(rankinfo[1]); // rank-info.
				try {
					context.write(okey, oval);
					context.getCounter(Counters.MoRank).increment(1);
				} catch (Exception e) {
					e.printStackTrace();
					context.getCounter(Counters.ErrMoRank).increment(1);
				}
			} else {
				String line = val.toString();
				try {
					this.logparse.parse(line, path);
				} catch (ParseException e1) {
				} finally {
					if (!this.logparse.base.status) {
						context.getCounter(Counters.ErrParse).increment(1);
						return;
					}
				}

				String[] ipinfo = iparea.evaluateMore(this.logparse.base.ip);
				if (ipinfo == null || ipinfo.length != 5) {
					context.getCounter(Counters.ErrIP).increment(1);
					return; //
				}
				String skey = this.logparse.buildUidKey();
				if (skey == null) {
					return;
				}
				okey.setFirst(skey); // uidkey
				okey.setFlag(true);
				List<String> ipzone = Arrays.asList(ipinfo);
				oval.set(this.logparse.base.ip + SEPA + ipzone.toString());
				try {
					context.write(okey, oval);
					context.getCounter(Counters.MoIP).increment(1);
				} catch (Exception e) {
					context.getCounter(Counters.ErrMo).increment(1);
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

	public static class ReduceCombine extends
			Reducer<PriorPair, Text, Text, Text> {

		private static enum Counters {
			ErrRo, ErrEmp, ErrMoIP, ErrRoIP, ErrRoZone, ErrMROIP, 
			MoRank, MoRankMul, MoIP, 
			RoIP, RoZone, RoUnrank
		};

		private MultipleOutputs<Text, Text> mos = null;
		private Text okey = new Text(), oval = new Text();

		public void setup(Context context) throws IOException, InterruptedException {
			mos = new MultipleOutputs(context);
			super.setup(context);
		}

		public void reduce(PriorPair key, Iterable<Text> values, Context context) {
			String rkey = key.getFirst().toString();
			String rank = null, tmp;
			String[] arrTmp;

			Set<String> ipset = new HashSet<String>();
			Set<String> zoneset = new HashSet<String>();

			for (Text t : values) {
				tmp = t.toString();

				if (tmp.startsWith("[")) {
					if (rank == null) {
						rank = tmp;

						oval.set(rank);
						context.getCounter(Counters.MoRank).increment(1);
					} else {
						context.getCounter(Counters.MoRankMul).increment(1);
					}
				} else {
					context.getCounter(Counters.MoIP).increment(1);
					
					// key对应的value中没有Rank信息 或 Rank信息没有最先到达, 直接写出IP信息，不进行ip-rank 操作
					if (rank == null) {
						context.getCounter(Counters.ErrEmp).increment(1);
						
						try {
							mos.write(moUidIp, key.getFirst(), t, moUidIpDir);
							context.getCounter(Counters.RoUnrank).increment(1);
						} catch (Exception e) {
							context.getCounter(Counters.ErrMROIP).increment(1);
						}
//						return;
					} else {
							arrTmp = tmp.split(SEPA);
							if (arrTmp.length < 2) { // Map-out 结果不正常
								context.getCounter(Counters.ErrMoIP).increment(1);
								continue;
							}
							ipset.add(arrTmp[0]);
							arrTmp = UtilStr.str2arr(arrTmp[1]); // 字符串转换
							for (String s : arrTmp) {
								zoneset.add(s);
							}
					}
					
				}
			}

			for (String s : ipset) {
				okey.set(s);
				try {
					mos.write(moIp, okey, oval, IpDir); // key=ip
					context.getCounter(Counters.RoIP).increment(1);
				} catch (Exception e) {
					e.printStackTrace();
					context.getCounter(Counters.ErrRoIP).increment(1);
				}
			}

			for (String s : zoneset) {
				okey.set(s);
				try {
					mos.write(moZone, okey, oval, ZoneDir); // key=zone
					context.getCounter(Counters.RoZone).increment(1);
				} catch (Exception e) {
					e.printStackTrace();
					context.getCounter(Counters.ErrRoZone).increment(1);
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
		String[] oargs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (oargs.length != 5) {
			System.out
					.println("Usage: <out> <in1> <data-range> <time-FMT-in-path> <inpath2-fmt>."
							+ "\nthe last 3 part imply the vary inpath.");
			System.exit(5);
		}

		Job job = new Job(conf, "[combine ip and rankinfo]");
		job.setJarByClass(GLogOrder.class);
		job.setMapperClass(MapCombine.class);
		job.setMapOutputKeyClass(PriorPair.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(ReduceCombine.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(32);

		job.setPartitionerClass(FPairPartition.class);
		job.setSortComparatorClass(SortComparator.class);
		// job.setCombinerClass(.class);
		job.setGroupingComparatorClass(KeyGrouping.class);

		FileOutputFormat.setOutputPath(job, new Path(args[0]));
		FileInputFormat.addInputPaths(job, args[1]);
		boolean setin = HdfsIO.setInput(oargs[2], oargs[3], oargs[4], job);
		if(!setin) {
			System.exit(1);
		}
		// FileInputFormat.addInputPath(job, new Path(args[2]));

		MultipleOutputs.addNamedOutput(job, moIp, TextOutputFormat.class,
				Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, moZone, TextOutputFormat.class,
				Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, moUidIp, TextOutputFormat.class,
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
			res = ToolRunner.run(new Configuration(), new GLogOrder(), args);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

}
