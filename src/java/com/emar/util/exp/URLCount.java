/**
 * @func code-store.
 * 
 */
package com.emar.util.exp;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//import com.emar.util.LogRecordUtil;  // ҵ���߼� 

public class URLCount {

	enum RecordNum {
		EmarBoxPV11, EmarBoxPVALL, YigaoPV11, YigaoPVALL
	}

	static class UrlCountMapper extends
			Mapper<LongWritable, Text, UrlPair, IntWritable> {

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			// ��ȡ�����Ƭ���ļ�·��
			String pathStr = ((FileSplit) context.getInputSplit()).getPath()
					.toString();
			// List<String> fields = LogRecordUtil.parseLogLine(line);
			List<String> fields = null; // @modify
			String urlLine = null;
			// ��ȡi_emarbox_browse�ļ�
			if (pathStr.indexOf("i_emarbox_browse_") >= 0) {
				context.getCounter(RecordNum.EmarBoxPVALL).increment(1);
				if (fields.size() >= 4) {
					context.getCounter(RecordNum.EmarBoxPV11).increment(1);
					urlLine = fields.get(3);
					if (urlLine != null && urlLine.length() >= 0) {
						context.write(new UrlPair(urlLine, 1), new IntWritable(
								1));
					}
				}
			}

			// ��ȡ���𷢶����ļ��������ļ����ڸ�ƽ̨����
			if (pathStr.indexOf("i_yigao_pv_") >= 0) {
				context.getCounter(RecordNum.YigaoPVALL).increment(1);
				if (fields.size() >= 5) {
					context.getCounter(RecordNum.YigaoPV11).increment(1);
					urlLine = fields.get(4);
					if (urlLine != null && urlLine.length() >= 0) {
						context.write(new UrlPair(urlLine, 1), new IntWritable(
								1));
					}
				}
			}

		}

	}

	static class UrlCountReducer extends
			Reducer<UrlPair, IntWritable, UrlPair, NullWritable> {

		public void reduce(UrlPair key, Iterable<IntWritable> values,
				Context context) throws IOException {
			int result = 0;
			for (IntWritable val : values) {
				result += val.get();
			}
			key.setUrlNum(new IntWritable(result));
			try {
				if (result >= 30) {
					context.write(key, NullWritable.get());
				}

			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	static class UrlKeyComparator extends WritableComparator {

		protected UrlKeyComparator() {
			// TODO Auto-generated constructor stub
			super(UrlPair.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			UrlPair u1 = (UrlPair) a;
			UrlPair u2 = (UrlPair) b;

			int cmp = u1.getUrlStr().compareTo(u2.getUrlStr());
			if (cmp != 0) {
				return cmp;
			}
			cmp = u1.getUrlNum().compareTo(u2.getUrlNum());
			return cmp;
		}

	}

	static class UrlGroupComparator extends WritableComparator {

		protected UrlGroupComparator() {
			super(UrlPair.class, true);
			// TODO Auto-generated constructor stub
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			UrlPair u1 = (UrlPair) a;
			UrlPair u2 = (UrlPair) b;

			int cmp = u1.getUrlStr().compareTo(u2.getUrlStr());
			return cmp;
		}

	}
	// 设置哪个 Reducer 处理 
	static class UrlPartition extends Partitioner<UrlPair, IntWritable> {

		@Override
		public int getPartition(UrlPair key, IntWritable value,
				int numPartitions) {
			// TODO Auto-generated method stub
			return Math.abs(key.getUrlStr().hashCode()) % numPartitions;
		}

	}

	static void addInputFile(FileSystem fs, Job job, String dateStr)
			throws IOException {
		String datePath = dateStr;
		if (dateStr == null || dateStr.length() != 8) {
			datePath = "*";
		}
		FileStatus[] fileStatus = fs.globStatus(new Path(
				"/data/stg/s_browse_log/" + datePath + "/3/*.dat"));
		for (FileStatus f : fileStatus) {
			CombineFileInputFormat.addInputPath(job, f.getPath());
			System.out.println(f.getPath().toString());
		}
		System.out.println("*************************");
		fileStatus = null;

		fileStatus = fs.globStatus(new Path("/data/stg/s_ad_pv_log/" + datePath
				+ "/2/*.dat"));
		for (FileStatus f : fileStatus) {
			CombineFileInputFormat.addInputPath(job, f.getPath());
			System.out.println(f.getPath().toString());
		}
		System.out.println("*************************");

	}

	public static void main(String[] args) {
		Configuration conf = new Configuration();
		Job job = null;
		System.out.println(args.length);
		System.out.println(args[0]);
		try {
			job = new Job(conf, "URL-COUNT");
			FileSystem fs = FileSystem.get(conf);

			if (args.length == 0) {
				addInputFile(fs, job, null);
				FileOutputFormat.setOutputPath(job, new Path(
						"/output/liuwei/URLCount/all"));
			} else if (args.length == 1) {
				addInputFile(fs, job, args[0]);
				FileOutputFormat.setOutputPath(job, new Path(
						"/output/liuwei/URLCount/" + args[0]));

			}

			else {
				System.exit(-1);
			}

			job.setJarByClass(URLCount.class);
			job.setMapperClass(UrlCountMapper.class);
			job.setReducerClass(UrlCountReducer.class);

			job.setMapOutputKeyClass(UrlPair.class);
			job.setMapOutputValueClass(IntWritable.class);
			// controls which of the m reduce tasks the intermediate key (and hence the record) is sent for reduction.
			// 在 Mapper 端执行； 同 Combiner
			job.setPartitionerClass(UrlPartition.class);
			//controls how the keys are sorted before they are passed to the Reducer
			// 对分区内的Key排序； 在 Mapper & Reducer 中使用
			job.setSortComparatorClass(UrlKeyComparator.class);
			// controls which keys are grouped together for a single reducer
			// 分组，构造一个key对应的value迭代器  Reducer
			job.setGroupingComparatorClass(UrlGroupComparator.class);
			job.setOutputKeyClass(UrlPair.class);
			job.setOutputValueClass(NullWritable.class);

			// job.setCombinerClass(UrlCountReducer.class);
			job.setNumReduceTasks(26);

			System.exit(job.waitForCompletion(true) ? 0 : -1);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
