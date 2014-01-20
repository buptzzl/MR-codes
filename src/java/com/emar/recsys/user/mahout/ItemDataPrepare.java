package com.emar.recsys.user.mahout;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.emar.recsys.user.util.UtilObj;
import com.emar.recsys.user.util.mr.HdfsDAO;
import com.emar.recsys.user.util.mr.HdfsIO;

/**
 * 将HDFS 的用户数据转换为 uid,iid,pref 格式,用于Mahout训练。 id必须为Long。
 * @author zhoulm
 *
 */
public class ItemDataPrepare extends Configured implements Tool {
	private static final String SEPA = "\t", S_UID = "@@@", UID = "userHash_",
			P_HASH = "dataprepare.hashpath";
	private static final int LEN = 2, L_UID = 3;
	
	private static Logger log = Logger.getLogger(ItemDataPrepare.class);
	

	public static class MapDataParse extends Mapper<LongWritable, Text, Text, NullWritable> {
		private static enum counters {
			MO, N_Mo, N_Fewer2, N_Uid_Fewer3, N_Uid_Number, N_Iid_Number;
		}
		private String[] atom, atmp;
		private String hashPathPrefix;
		private String tmp;
		private Text okey;
		private NullWritable oval;
		private Map<Integer, String> userInfo;
		
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			hashPathPrefix = conf.get(P_HASH);
			assert hashPathPrefix != null && hashPathPrefix.length() != 0;
			okey = new Text();
			oval = NullWritable.get();
			userInfo = new HashMap<Integer, String>(1024, 0.95f);
		}
		
		public void map(LongWritable row, Text value, Context context) {
			atom = value.toString().split(SEPA);
			if (atom.length < LEN) {
				context.getCounter(counters.N_Fewer2).increment(1);
				log.error("data format error. input=" + value);
				return;
			}
			atmp = atom[0].split(S_UID);
			if(atmp.length < L_UID) {
				context.getCounter(counters.N_Uid_Number).increment(1);
				log.error("uid format error, Exp:X@@@123@@@Y, exact=" + atom[0]
						+ ",\tout=" + Arrays.asList(atmp));
				return;
			}
			
			try {
				int hcode = atmp[1].trim().hashCode();
				userInfo.put(hcode, atmp[1].trim());
				atmp[1] = hcode + "";
			} catch (Exception e) {
				context.getCounter(counters.N_Uid_Number).increment(1);
				log.error("uid isn't Number. exact=" + atmp[1]);
				return;
			}
			
			Map<String, Float> interests = UtilObj.Str2Map(atom[1], "[]", ", ", "=");
//			String uid = null;
//			Float freq = 0f;
			for (Map.Entry<String, Float> ei : interests.entrySet()) {
				try {
					Integer.parseInt(ei.getKey().trim());
				} catch (Exception e) {
					context.getCounter(counters.N_Iid_Number).increment(1);
					log.error("itemid isn't Number. exact=" + ei.getKey());
					continue;
				}
				okey.set(String.format("%s,%s,%.2f,",atmp[1].trim(), 
						ei.getKey().trim(), ei.getValue()));
//				oval.set(String.format("%.2f", ei.getValue()));
				try {
					context.write(okey, oval);
					context.getCounter(counters.MO).increment(1);
				} catch (Exception e) {
					context.getCounter(counters.N_Mo).increment(1);
				}
			}
			
		}
		
		public void cleanup(Context context) {
			try {
				HdfsIO.writeMap((Map)userInfo, 
						new Path(hashPathPrefix+"_"+context.getCounter(counters.MO).getValue()), 
						context.getConfiguration());
			} catch (IOException e) {
				log.error("write data error, path=" + hashPathPrefix 
						+ ", data=" + userInfo);
			}
		}
		
	}
	
//	public static class Reduce extends Reducer<Text, Text, Text, Text> {
//	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = new Configuration();
		String[] oargs = new GenericOptionsParser(conf, arg0).getRemainingArgs();
		if (oargs.length < 5) {
			System.out
					.println("Usage:<out> <data-range> <time-FMT-in-path> <path-fmt>  <hashmap path> ");
			return -1;
		}
		conf.set(P_HASH, oargs[4]);
		Job job = new Job(conf, "parse user data.");
		job.setJarByClass(ItemDataPrepare.class);
		job.setMapperClass(MapDataParse.class);
//		job.setReducerClass(null);
		job.setNumReduceTasks(0);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		boolean setin = HdfsIO.setInput(oargs[1], oargs[2], oargs[3], job);
		HdfsDAO hdao = new HdfsDAO(conf);
		hdao.rmr(oargs[0]);
		FileOutputFormat.setOutputPath(job, new Path(oargs[0]));

		Date startTime = new Date();
		int res = job.waitForCompletion(true) ? 0 : 1;
		Date end_time = new Date();
		System.out.println("[INFO] The job took "
				+ (end_time.getTime() - startTime.getTime()) / 1000
				+ " seconds.");
		
		return res;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		int res = 0;
		try {
			Configuration conf = new Configuration();
			res = ToolRunner.run(conf, new ItemDataPrepare(), args);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}
	
}
