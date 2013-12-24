package com.emar.util;

import java.io.IOException;
import java.text.ParseException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;

import com.emar.recsys.user.log.LogParse;

/**
 * 基于MR 的共现key频率统计: A-B, A&B, B-A.
 * 仅仅处理： history VS HDFS-RAW-LOG.
 * @author zhoulm
 *
 */
public class StatisticTwo extends Configured implements Tool {

	private static final String KEY = "classify";
	private static final int Value1 = 1, Value2 = 2;
	
	public static class MapStatic extends Mapper<LongWritable, Text, Text, VIntWritable> {
		private static final String S_KV = "\t", S_KEY = "@@@",
				S_HDFS = "\u0001";
//		private LogParse logparse;
		private int index2; // index1;
		
		private Text okey;
		private VIntWritable oval;
		private enum CNT { 
			ME_EMP1, ME_EMP2, ME_OUT1, ME_OUT2, 
		}
		
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
//			index1 = conf.getInt("mr.log1.index", 0);
			index2 = conf.getInt("mr.log2.index", -1);
			if (index2 < 0) {
				System.exit(index2);
			}
//			try {
//				logparse = new LogParse();
//			} catch (ParseException e) {
//				e.printStackTrace();
//				System.exit(e.getErrorOffset());
//			}
			okey = new Text();
			oval = new VIntWritable();
		}
		
		public void map(LongWritable key, Text value, Context context) {
			String line = value.toString();
			String path = ((FileSplit) context.getInputSplit()).getPath()
					.toString();
			String[] atom = null;
			
			if (path.indexOf(KEY) != -1) {
				// path-A: classify id.
				if (line.trim().length() != 0) {
					atom = line.trim().split(S_KV);
					if (atom.length > 1) {
						atom = atom[0].split(S_KEY);
						okey.set(atom[1]);
						oval.set(Value1);
						try {
							context.write(okey, oval);
						} catch (Exception e) {
							context.getCounter(CNT.ME_OUT1).increment(1);
						}
					}
				}
					context.getCounter(CNT.ME_EMP1).increment(1);
			} else {
				// path-A: HDFS-LOG. 
				if (line.trim().length() != 0) {
					atom = line.trim().split(S_HDFS);
					if (atom.length > index2) {
						okey.set(atom[index2]);
						oval.set(Value2);
						try {
							context.write(okey, oval);
						} catch (Exception e) {
							context.getCounter(CNT.ME_OUT2).increment(1);
						}
					}
				}
			}
		}
		
	}
	
	public static class ReduceStatic extends Reducer<Text, VIntWritable, Text, NullWritable> {
		private static String Part1 = "Part_1", Part2 = "Part_2", PartBoth="Part";  
		
		private MultipleOutputs<Text, NullWritable> mos;
		private String path;
		private enum CNT {
			R_N_Both, R_N_1, R_N_2,
			RE_B, RE_1, RE_2, 
		}
		
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			mos = new MultipleOutputs<Text, NullWritable>(context);
			path = conf.get("mr.outpath");
		}
		
		public void reduce(Text key, Iterable<VIntWritable> values, Context context) {
			Set<Integer> isboth = new HashSet<Integer>();
			for (VIntWritable vi : values) {
				isboth.add(vi.get());
			}
//			String ki = key.toString();
			if (isboth.size() == 2) {
				context.getCounter(CNT.R_N_Both).increment(1);
				try {
					mos.write(PartBoth, key, NullWritable.get(), path + "/" + PartBoth);
				} catch (Exception e) {
					context.getCounter(CNT.RE_B).increment(1);
				}
			} else if (isboth.contains(Value1)) {
				context.getCounter(CNT.R_N_1).increment(1);
				try {
					mos.write(Part1, key, NullWritable.get(), path + "/" + Part1);
				} catch (Exception e) {
					context.getCounter(CNT.RE_1).increment(1);
				}
			} else {
				context.getCounter(CNT.R_N_2).increment(1);
				try {
					mos.write(Part2, key, NullWritable.get(), path + "/" + Part2);
				} catch (Exception e) {
					context.getCounter(CNT.RE_2).increment(1);
				}
			}
		}
		
	}


	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
	
}
