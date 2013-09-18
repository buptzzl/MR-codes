package com.emar.recsys.user;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.emar.classify.GoodsMark;
// for Reducer.

public class ItemClassify extends Configured implements Tool {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		// public static class Map extends MapReduceBase implements Mapper<
		// LongWritable, Text, Text, Text> {

		private static enum Counters {
			Useless, UnMapout, UnClass
		};

		private static int LEN;
		private static int IDX_T;
		private static int IDX_UID;
		private static int[] IDX_LIST;
		private static String SEPA;

		private boolean caseLower, case2;
		private String inputFile;

		// private MultipleOutputs mos;

		Calendar c = Calendar.getInstance();
		SimpleDateFormat dateformat = new SimpleDateFormat("yyyyMMdd HH:mm:ss");

		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			LEN = conf.getInt("LEN", 10);
			String[] idxs = conf.getStrings("index_list"); // SEPA: ,

			IDX_LIST = new int[idxs.length];
			for (int i = 0; i < idxs.length; ++i) {
				IDX_LIST[i] = Integer.parseInt(idxs[i]);
			}
			SEPA = conf.get("SEPA", "\u0001");
			IDX_T = conf.getInt("index_time", 0);
			IDX_UID = conf.getInt("index_uid", 0);
		}

		public void map(LongWritable key, Text value, Context context) {
			// public void map(LongWritable arg0, Text arg1,
			// OutputCollector<Text, Text> arg2, Reporter arg3)
			// throws IOException {
			String[] line = value.toString().split(Map.SEPA);
			if (line == null || line.length < Map.LEN) {
				context.getCounter(Counters.Useless).increment(1);

				System.out.println("[Debug] Mapper() LEN=" + line.length);
			} else {
				String[] info_class = new String[] { "*", "*", "*", "*" };
				try { // ֱ��ʹ����ƷID
					Integer.parseInt(line[Map.IDX_LIST[0]]);
					info_class[2] = line[Map.IDX_LIST[0]];
				} catch (NumberFormatException e_n) {
					// ����ƷIDʱ������ʹ�� ��Ʒ����JAR
					for (int i : Map.IDX_LIST) {
						try {
							info_class = GoodsMark.getInstance()
									.ClassifyGoods(line[i]).split("\t");
							if (info_class.length > 2 && info_class[2] != "*") {
								break;
							}
						} catch (IOException e) {
							context.getCounter(Counters.UnClass).increment(1);
						}
					}
				}

				try { // ��д������ܵ����or��һ����ЧID
					context.write(
							new Text(line[Map.IDX_UID]),
							new Text(String.format("%s\u0001%s", info_class[2],
									line[Map.IDX_T])));
				} catch (Exception e) {
					context.getCounter(Counters.UnMapout).increment(1);
				}
			}
		}

		public void cleanup(Context context) {
			// TODO
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		// public static class Reduce extends MapReduceBase implements
		// Reducer<Text, Text,
		// Text, Text> {

		private static enum Counters {
			HighFreq, RoutCnt
		};

		private static int TOPK;

		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			TOPK = conf.getInt("topk", 10);
		}

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			HashMap<String, Integer> cnt_prod = new HashMap<String, Integer>(4);

			int tmp = 0;
			for (Text t : values) {
				// Ŀǰ��ʹ��ʱ����Ϣ
				String[] varr = t.toString().split("\u0001"); // values.next().toString().split(",");
				if (cnt_prod.containsKey(varr[0])) {
					cnt_prod.put(varr[0], cnt_prod.get(varr[0]) + 1);
				} else {
					cnt_prod.put(varr[0], 1);
				}

			}

			StringBuffer tmps = new StringBuffer();
			if (cnt_prod.size() < Reduce.TOPK) {
				tmps.append(cnt_prod.toString());
			} else {
				List<Entry<String, Integer>> sinfo = new ArrayList<Entry<String, Integer>>(
						cnt_prod.entrySet());
				Collections.sort(sinfo,
						new Comparator<Entry<String, Integer>>() {
							public int compare(Entry<String, Integer> e1,
									Entry<String, Integer> e2) {
								return e1.getValue() - e2.getValue();
							}
						});
				tmps.append("{");
				for (int i = 0; i < Reduce.TOPK; ++i) {
					tmps.append(String.format("%s=%d", sinfo.get(i).getKey(),
							sinfo.get(i).getValue()));
				}
				tmps.append("}");
				context.getCounter(Counters.HighFreq).increment(1);
				System.out.println("[TEST] Reducer() info:" + sinfo + "\nmap :"
						+ cnt_prod);
			}

			// System.out.println("[TEST] Reducer() out :" + tmps);
			context.getCounter(Counters.RoutCnt).increment(1);
			context.write(key, new Text(tmps.toString()));

		}

		public void cleanup(Context context) {
		}
	}

	public static class PartitionerClass extends Partitioner<Text, IntWritable> {
		public int getPartition(Text key, IntWritable value, int numPartitions) {
			if (numPartitions >= 2)// Reduce �����ж� loglevel ���� logmodule
									// ��ͳ�ƣ����䵽��ͬ�� Reduce
				if (key.toString().startsWith("logLevel::"))
					return 0;
				else if (key.toString().startsWith("moduleName::"))
					return 1;
				else
					return 0;
			else
				return 0;
		}
	}

	// REF:
	// http://hadoop.apache.org/docs/current/api/org/apache/hadoop/mapreduce/lib/output/MultipleOutputs.html
	public int run(String[] args) throws IOException, InterruptedException,
			ClassNotFoundException {
		Configuration conf = new Configuration();
		conf.set("LEN", args[2]);
		String[] idxs = args[3].split("\t");
		conf.set("index_list", args[3]); // idxs);
		// conf.set("index_arr", idxs);
		conf.set("SEPA", args[4]);
		conf.set("index_time", args[5]);
		conf.set("index_uid", args[6]);
		conf.set("topk", args[7]);

		Job job = new Job(conf); // Job(super.getConf()); //
		job.setJarByClass(ItemClassify.class);
		job.setJobName("user's item classify log1");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		// job.setPartitionerClass(PartitionerClass.class);
		job.setNumReduceTasks(8); // ǿ����Ϊ2��
									// �Զ���Partitioner�Ŀ�ѡReduce-worker.
		// HashPartitioner is the default Partitioner.

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		// FileStatus[] input_tot = (FileStatus[])
		// FileInputFormat.listStatus(job).toArray();
		Path[] input_tot = FileInputFormat.getInputPaths(job);
		for (Path p : input_tot) {
			FileSystem fsystem = p.getFileSystem(job.getConfiguration());
			FileStatus[] input_fs = fsystem.globStatus(p);
			for (FileStatus ps : input_fs) {
				System.out.println("[INFO] inPath:" + ps.getPath());
			}
		}
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		Date startTime = new Date();
		int res = job.waitForCompletion(true) ? 0 : 1;
		Date end_time = new Date();
		System.out.println("[INFO] The job took "
				+ (end_time.getTime() - startTime.getTime()) / 1000
				+ " seconds.");
		return res;
	}

	/**
	 * 
	 * @param args
	 *            input,output,LEN,key-list(order by importance)
	 */
	public static void main(String[] args) {
		String arg_test = "";
		for (String s : args) {
			arg_test = arg_test + "\t" + s;
		}
		System.out.printf("[INFO] ItemClassify::args\n%s\n", arg_test);

		int res;
		try {
			res = ToolRunner.run(new Configuration(), new ItemClassify(), args);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}