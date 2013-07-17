package com.emar.util.exp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * MR 多输入功能：不同的KEY自定义类型; 以及泛Writable类型 GenericWritable 的使用。
 * @ref http://www.lichun.cc/blog/2012/05/hadoop-multipleinputs-usage/
 * @author zhoulm
 *
 */
public class ExpMultipleInputs {
	
	/*
	public static class MyGenericWritable extends GenericWritable {

	    private static Class<? extends Writable>[] CLASSES = null;

	    static {
	        CLASSES = (Class<? extends Writable>[]) new Class[] {
	            FirstClass.class,
	            SecondClass.class
	             //add as many different class as you want
	        };
	    }
	    //this empty initialize is required by Hadoop
	    public MyGenericWritable() {
	    }

	    public MyGenericWritable(Writable instance) {
	        set(instance);
	    }

	    @Override
	    protected Class<? extends Writable>[] getTypes() {
	        return CLASSES;
	    }

	    @Override
	    public String toString() {
	        return "MyGenericWritable [getTypes()=" + Arrays.toString(getTypes()) + "]";
	    }
	}
	*/

	public static class FirstMap extends Mapper<Text, FirstClass, Text, Text> {
//	public static class FirstMap extends Mapper<Text, FirstClass, Text, MyGenericWritable> {
		public void map(Text key, FirstClass value, Context context)
				throws IOException, InterruptedException {
			System.out.println("FirstMap:" + key.toString() + " "
					+ value.toString());
			context.write(key, new Text(value.toString()));
//			context.write(key, new MyGenericWritable(value));  //wrap 
		}
	}
	
	public static class SecondMap extends Mapper<Text, SecondClass, Text, Text> {
//	public static class SecondMap extends Mapper<Text, SecondClass, Text, MyGenericWritable> {
		public void map(Text key, SecondClass value, Context context)
				throws IOException, InterruptedException {
			System.out.println("SecondMap:" + key.toString() + " "
					+ value.toString());
			context.write(key, new Text(value.toString()));
		}
	}

//	public class Reduce extends Reducer<Text, MyGenericWritable, Text, Text> {
	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				System.out.println("Reduce:" + key.toString() + " "
						+ value.toString());
				context.write(key, value);
				// @for MyGenericWritable
//				Writable rawValue = value.get();   //unwrap
//				if(rawValue instanceof FirstClass){
//					FirstClass firstClass = (FirstClass)rawValue;
		        }
			}
		}
	}

	/**
first mapclass's input example:
1	Chun
2	Tina
	 */
	public static class FirstClass implements Writable {
		private String value;

		public FirstClass() {
			this.value = "TEST";
		}

		public FirstClass(String val) {
			this.value = val;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			if (null == in) {
				throw new IllegalArgumentException("in cannot be null");
			}
			String value = in.readUTF();

			this.value = value.trim();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			if (null == out) {
				throw new IllegalArgumentException("out cannot be null");
			}
			out.writeUTF(this.value);
		}

		@Override
		public String toString() {
			return "FirstClass\t" + value;
		}

	}

	/**
second mapclass's input example:
1	Time	87
2	Date	19
	 */
	public static class SecondClass implements Writable {

		private String value;
		private int additional;

		public SecondClass() {
			this.value = "TEST";
			this.additional = 0;
		}

		public SecondClass(String val, int addi) {
			this.value = val;
			this.additional = addi;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			if (null == in) {
				throw new IllegalArgumentException("in cannot be null");
			}
			String value = in.readUTF();
			int addi = in.readInt();

			this.value = value.trim();
			this.additional = addi;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			if (null == out) {
				throw new IllegalArgumentException("out cannot be null");
			}
			out.writeUTF(this.value);
			out.writeInt(this.additional);
		}

		@Override
		public String toString() {
			return "SecondClass\t" + value + "\t" + additional;
		}
	}

	public static class FirstClassReader extends RecordReader<Text, FirstClass> {
		private LineRecordReader lineRecordReader = null;
		private Text key = null;
		private FirstClass valueFirstClass = null;

		@Override
		public void close() throws IOException {
			if (null != lineRecordReader) {
				lineRecordReader.close();
				lineRecordReader = null;
			}
			key = null;
			valueFirstClass = null;
		}

		@Override
		public Text getCurrentKey() throws IOException, InterruptedException {
			return key;
		}

		@Override
		public FirstClass getCurrentValue() throws IOException,
				InterruptedException {
			return valueFirstClass;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return lineRecordReader.getProgress();
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			close();

			lineRecordReader = new LineRecordReader();
			lineRecordReader.initialize(split, context);
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (!lineRecordReader.nextKeyValue()) {
				key = null;
				valueFirstClass = null;
				return false;
			}

			// otherwise, take the line and parse it
			Text line = lineRecordReader.getCurrentValue();
			String str = line.toString();
			System.out.println("FirstClass:" + str);
			String[] arr = str.split("\t", -1);

			key = new Text(arr[0].trim());
			valueFirstClass = new FirstClass(arr[1].trim());

			return true;
		}

	}

	public static class SecondClassReader extends
			RecordReader<Text, SecondClass> {

		private LineRecordReader lineRecordReader = null;
		private Text key = null;
		private SecondClass valueSecondClass = null;

		@Override
		public void close() throws IOException {
			if (null != lineRecordReader) {
				lineRecordReader.close();
				lineRecordReader = null;
			}
			key = null;
			valueSecondClass = null;
		}

		@Override
		public Text getCurrentKey() throws IOException, InterruptedException {
			return key;
		}

		@Override
		public SecondClass getCurrentValue() throws IOException,
				InterruptedException {
			return valueSecondClass;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return lineRecordReader.getProgress();
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			close();

			lineRecordReader = new LineRecordReader();
			lineRecordReader.initialize(split, context);
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (!lineRecordReader.nextKeyValue()) {
				key = null;
				valueSecondClass = null;
				return false;
			}

			// otherwise, take the line and parse it
			Text line = lineRecordReader.getCurrentValue();

			String str = line.toString();

			System.out.println("SecondClass:" + str);
			String[] arr = str.split("\t", -1);
			int addi = Integer.parseInt(arr[2]);

			key = new Text(arr[0].trim());
			valueSecondClass = new SecondClass(arr[1].trim(), addi);

			return true;
		}

	}

	public static class FirstInputFormat extends
			FileInputFormat<Text, FirstClass> {
		@Override
		public RecordReader<Text, FirstClass> createRecordReader(
				InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			return new FirstClassReader();
		}
	}

	public static class SecondInputFormat extends
			FileInputFormat<Text, SecondClass> {
		@Override
		public RecordReader<Text, SecondClass> createRecordReader(
				InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			return new SecondClassReader();
		}
	}

	/**
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 * @param args
	 * @throws
	 */
	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		Path firstPath = new Path(args[0]);
		Path sencondPath = new Path(args[1]);
		Path outputPath = new Path(args[2]);

		Configuration conf = new Configuration();

		Job job = new Job(conf);
		job.setJarByClass(ExpMultipleInputs.class);
		job.setJobName("MultipleInputs Test");

		job.setReducerClass(MyReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		System.out.println("start");

		MultipleInputs.addInputPath(job, firstPath, FirstInputFormat.class,
				FirstMap.class);
		MultipleInputs.addInputPath(job, sencondPath, SecondInputFormat.class,
				SecondMap.class);

		FileOutputFormat.setOutputPath(job, outputPath);

		job.waitForCompletion(true);

	}

}
