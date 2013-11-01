package com.emar.recsys.user.util.mr;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

/**
 * 处理多个小文件做Map 的输入.
 * @TODO 换行符没有保留下来。
 * @author zhoulm
 *
 */
public class CombineFilesInputFormat extends
		CombineFileInputFormat<LongWritable, BytesWritable> {

	@Override
	public RecordReader<LongWritable, BytesWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException {

		CombineFileSplit combineFileSplit = (CombineFileSplit) split;
		// 使用自定义的 CombineFilesRecordReader
		CombineFileRecordReader<LongWritable, BytesWritable> recordReader = 
				new CombineFileRecordReader<LongWritable, BytesWritable>(
				combineFileSplit, context, CombineFilesRecordReader.class);
		try {
			recordReader.initialize(combineFileSplit, context);
		} catch (InterruptedException e) {
			new RuntimeException(
					"Error to initialize CombineSmallfileRecordReader.");
		}
		return recordReader;
	}

}