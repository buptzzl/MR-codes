package com.emar.recsys.user.action;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;
import java.util.TreeSet;

import javax.sound.midi.SysexMessage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.JSONArray;
import org.json.JSONObject;

import com.emar.recsys.user.log.LogParse;
import com.emar.recsys.user.util.mr.HdfsIO;
import com.emar.recsys.user.util.mr.CounterArray.EArray;

/**
 * 归并用户的行为。
 * 
 * @author zhoulm
 * 
 */
public class MergeAction extends Configured implements Tool {
	// 定义Map & Reduce 中通用的对象
	private static final String SEPA = LogParse.SEPA,
			SEPA_MR = LogParse.SEPA_MR, PLAT = LogParse.PLAT,
			EMAR = LogParse.EMAR, MAGIC = LogParse.MAGIC, 
			nActMin = "minActionTimes", nEnumSize = "nEnumSize";
	// 有效KEY 的集合。 仅仅在值非null与空串时插入JSON
	private static final String[] ATOM_STR = new String[] {
			"time", "ip", "type", "plat", "prod_price", "domain",			
		},  
		PRIMARY_STR = { //
			"prod_name", "page_url", "refer_url", "orig_media", 
		},
		WordsPage = new String[] { // 页面信息
			"title", "desc", "keywords"
		};
	private static String j_WP = "pagewords";
	
	public static boolean debug = true;

	public static class MapAction extends
			Mapper<LongWritable, Text, Text, Text> {
		private LogParse logparse;
		private Text okey, oval;
		private JSONArray j_arr = new JSONArray();
		private JSONObject j_obj;

		private enum CNT {
			ErrMo, ErrParse, ErrParUid, ErrParTime, ErrParPrimay, 
		};
		
		public void setup(Context  context) {
			try {
				logparse = new LogParse();
			} catch (ParseException e) {
				e.printStackTrace();
				System.exit(e.getErrorOffset());
			}
			okey = new Text();
			oval = new Text();
		}

		public void map(LongWritable key, Text val, Context context) {
			// String result = ClassifyGoods("葡萄酒100ml");
			String line = val.toString();
			String path = ((FileSplit) context.getInputSplit()).getPath()
					.toString();
				
			try {
				// this.logparse.base.isdug = true;
				this.logparse.parse(line, path);
			} catch (ParseException e1) {
				this.logparse.base.status = false;
			}
			if (!this.logparse.base.status) {
				context.getCounter(CNT.ErrParse).increment(1);
				if (debug) 
					System.out.println("[ERR] MAction::map() in parse status." 
							+ "path=" + path + "\nline=" + line + "\nparse=" + logparse);
				return;
			}

			Object tmp;
			String keyLog = this.logparse.buildUidKey();
			if (keyLog == null || keyLog.trim().length() == 0) {
				context.getCounter(CNT.ErrParUid).increment(1);
				if (debug)
					System.out.println("[ERR] MAction::map() in get UID. "
							+ "path=" + path + "\nline=" + line + "\nparse=" + logparse);
				return;
			}
			okey.set(keyLog);
			
			tmp = this.logparse.getTime();//固定的Reduce中排序的依据 
			if (tmp == null || tmp.toString().trim().length() == 0) {
				context.getCounter(CNT.ErrParTime).increment(1);
				if (debug)
					System.out.println("[ERR] MAction::map() in log parse time."
							+ "path=" + path + "\nline=" + line + "\nparse=" + logparse);
				return;
			}
			
			j_obj = new JSONObject();
			StringBuffer keywords = new StringBuffer();
			for (int i = 0; i < WordsPage.length; ++i) {
				tmp = this.logparse.getField(WordsPage[i]);
				if (tmp != null && tmp.toString().trim().length() != 0) 
					keywords.append(tmp);
				keywords.append(MAGIC);// 分隔符始终存在
			}
			for (int i = 0; i < PRIMARY_STR.length; ++i) {
				tmp = this.logparse.getField(PRIMARY_STR[i]);
				if (tmp != null && tmp.toString().trim().length() != 0) 
					j_obj.put(PRIMARY_STR[i], tmp);
			}
			if (MAGIC.length() * WordsPage.length != keywords.length()) {
				j_obj.put(j_WP, keywords.toString());
			} else if (j_obj.length() == 0) {
				context.getCounter(CNT.ErrParPrimay).increment(1);
				return;
			}
			
			for (int i = 0; i < ATOM_STR.length; ++i) {
				tmp = this.logparse.getField(ATOM_STR[i]);
				if (tmp != null && tmp.toString().trim().length() != 0) 
					j_obj.put(ATOM_STR[i], tmp);
			}
			
			oval.set(tmp + SEPA + j_obj);
			
			try {
				context.write(okey, oval);
			} catch (Exception e) {
				context.getCounter(CNT.ErrMo).increment(1);
			}
			this.logparse.reset();
		}

	}

	public static class ReduceAction extends Reducer<Text, Text, Text, Text> {
		private static int N_ACT_MIN = 3, N_CNT_MAX = 50;
		private enum CNT {
			RoFewerK, 
			ErrRo 
//			N1, N3, N5, N10, N20, N30
		}
		private EArray enumArr;
		
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			N_ACT_MIN = conf.getInt(nActMin, N_ACT_MIN);
			enumArr.setMax(conf.getInt(nEnumSize, N_CNT_MAX));
			if (debug) 
				System.out.println("MAction::Reduce setup() N_ACT_MIN=" + N_ACT_MIN);
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context) {
			int n = 0;
			
			String tmp;
			TreeSet<String> act_sort = new TreeSet<String>();
			for (Text ti : values) {
//				JSONObject j_obj = new JSONObject(ti.toString());
				n ++;
				act_sort.add(ti.toString());  // 去重
			}
			
			JSONArray j_arr = new JSONArray();
			for (String si : act_sort) {
				si = si.split(SEPA)[1];
				j_arr.put(si);
			}
				
			context.getCounter(EArray.getElement(n)).increment(1);
			try {
				if (N_ACT_MIN < act_sort.size()) 
					context.write(key, new Text(j_arr.toString()));
				else 
					context.getCounter(CNT.RoFewerK).increment(1);
			} catch (Exception e) {
				context.getCounter(CNT.ErrRo).increment(1);
			}
		}

	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] oargs = new GenericOptionsParser(conf,args).getRemainingArgs();
		if (oargs.length < 5) {
			System.out.println("Usage:  <out> <dataHour-Range> " +
					"<dFMT> <inFMT> <UserMinFreq_NCounter_NumRed> " +
					"[<dFMT2> <inFMT2> ...] ");
			System.exit(4);
		}
		int NRed = 4;
		if (oargs[4].indexOf('_') != -1) {
			String[] tmp = oargs[4].split("_");
			conf.setInt(nActMin, Integer.parseInt(tmp[0]));
			if (1 < tmp.length)
				conf.setInt(nEnumSize, Integer.parseInt(tmp[1]));
			if (2 < tmp.length)
				NRed = Integer.parseInt(tmp[2]);
			System.out.println("[info] UserMinFreq="+conf.get(nActMin)+
					"EnumSize="+conf.get(nEnumSize)+", NumRed"+NRed);
		}

		Job job = new Job(conf, "[merge users action]");
		job.setJarByClass(MergeAction.class);
		job.setMapperClass(MapAction.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(ReduceAction.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(NRed); // (16);
		
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[0]));
		// FileInputFormat.addInputPaths(job, args[1]);
		if (debug)
			System.out.println(String.format("[Info] inpath-param:\t%s, %s, %s", 
					oargs[1], oargs[2], oargs[3]));
		boolean setin = HdfsIO.setInput(oargs[1], oargs[2], oargs[3], job);
		if (5 < oargs.length && (oargs.length - 5) % 2 == 0) {// 添加输入路径
			for (int i = 5; i < oargs.length; i+=2) {
				if (debug)
					System.out.println(String.format("[Info] inpath-param:\t%s, %s, %s", 
							oargs[1], oargs[i], oargs[i+1]));
				setin = setin && HdfsIO.setInput(oargs[1], oargs[i], oargs[i+1], job);
			}
		}

		// MultipleOutputs.addNamedOutput(job, first, TextOutputFormat.class,
		// Text.class, Text.class);

		Date startTime = new Date();
		job.waitForCompletion(true);
		Date end_time = new Date();
		System.out.println("The job took "
				+ (end_time.getTime() - startTime.getTime()) / 1000
				+ " seconds.");
		return 0;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		int res;
		try {
			res = ToolRunner.run(new Configuration(), new MergeAction(), args);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

}
