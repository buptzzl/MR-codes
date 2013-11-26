package com.emar.recsys.user.action;

import java.io.IOException;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.antlr.grammar.v3.ANTLRv3Parser.throwsSpec_return;
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
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.emar.recsys.user.log.LogParse;
import com.emar.recsys.user.services.ItemCacheRedis;
import com.emar.recsys.user.services.ItemClassifyRedis;
import com.emar.recsys.user.util.itemclassify.ItemSegmentClass;
import com.emar.recsys.user.util.mr.HdfsDAO;
import com.emar.recsys.user.util.mr.HdfsIO;
import com.emar.recsys.user.util.mr.CounterArray.EArray;
import com.emar.util.ConfigureTool;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.collect.TreeMultiset;

/**
 * 归并用户的行为； 并将商品名写 HDFS。
 * 
 * @author zhoulm
 * 
 */
public class MergeActionCache extends Configured implements Tool {
	// 定义Map & Reduce 中通用的对象
	private static Logger log = Logger.getLogger(MergeActionCache.class);

	private final static int IdxClass = 2;
	private static final String SEPA = LogParse.SEPA, UnuseClass = "*",
			EMAR = LogParse.EMAR, MAGIC = LogParse.MAGIC,
			nActMin = "minActionTimes", nEnumSize = "nEnumSize";
	// 有效KEY 的集合。 仅仅在值非null与空串时插入JSON
	private static final String[] ATOM_STR = new String[] { "time", "ip",
			"type", "plat", "prod_price", "domain", },
			PRIMARY_STR = new String[] { //
			"prod_name", "prod_type_name", }, PRIMARY_URL = new String[] {
					"page_url", "refer_url", "landing_url", "orig_media", },
			WordsPage = new String[] { // 页面信息
			"title", "desc", "keywords" };
	private static final String j_WP = "pagewords", CLASSID = "cid";
	private static final String mosItem = "classify", mosAction = "merge";

	public static boolean debug = true;

	public static class MapAction extends
			Mapper<LongWritable, Text, Text, Text> {
		private LogParse logparse;
		private Text okey, oval;
		private MultipleOutputs<Text, Text> mos;
		private String classId;

		private ItemCacheRedis itemCache;
		private ItemSegmentClass itemClassify;
		private JSONArray j_arr = new JSONArray();
		private JSONObject j_obj;

		private enum CNT {
			ErrMo, ErrParse, ErrParUid, ErrParTime, ErrParPrimay,
		};

		public void setup(Context context) {
			try {
				logparse = new LogParse();
			} catch (ParseException e) {
				e.printStackTrace();
				System.exit(e.getErrorOffset());
			}
			okey = new Text();
			oval = new Text();
			mos = new MultipleOutputs<Text, Text>(context);

			try {
				itemClassify = new ItemSegmentClass(context);
			} catch (Exception e) {
				log.error("initial ItemSegmentClass failed:" + e.getMessage());
				throw new RuntimeException(e.getMessage());
			}
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
				log.error("parse log failed. hdfs-path=" + path + "\n\tline="
						+ line + "\n\tparse=" + logparse);
				return;
			}

			Object tmp;
			String keyLog = this.logparse.buildUidKey();
			if (keyLog == null || keyLog.trim().length() == 0) {
				context.getCounter(CNT.ErrParUid).increment(1);
				log.error("parse build key failed. keyLog=" + keyLog
						+ "\tparse=" + logparse + "\tline=" + line);
				return;
			}
			okey.set(keyLog);

			tmp = this.logparse.getTime();// 固定的Reduce中排序的依据
			if (tmp == null || tmp.toString().trim().length() == 0) {
				context.getCounter(CNT.ErrParTime).increment(1);
				log.error("parse time failed. time=" + logparse.getTime()
						+ "\tparse=" + logparse);
				return;
			}

			j_obj = new JSONObject();
			String[] pclass = null;
			// 商品分类 优先级最高
			for (int i = 0; i < PRIMARY_STR.length; ++i) {
				tmp = this.logparse.getField(PRIMARY_STR[i]);
				if (tmp != null && tmp.toString().trim().length() != 0) {
					j_obj.put(PRIMARY_STR[i], tmp);

					if (pclass == null) {
						pclass = itemClassify.ClassifyGoods((String) tmp)
								.split("\t");
						if (pclass.length != 5
								|| pclass[IdxClass].equals(UnuseClass)) {
							pclass = null;
							log.debug("classify empty. input=" + tmp);
						}
					}
				}
			}
			// 商品分类优先级第2
			StringBuffer keywords = new StringBuffer();
			for (int i = 0; i < WordsPage.length; ++i) {
				tmp = this.logparse.getField(WordsPage[i]);
				if (tmp != null && tmp.toString().trim().length() != 0)
					keywords.append(tmp);
				keywords.append(MAGIC);// 分隔符始终存在
			}
			tmp = keywords.toString();
			if (MAGIC.length() * WordsPage.length != keywords.length()) {
				j_obj.put(j_WP, tmp);

				if (pclass == null) {
					pclass = itemClassify.ClassifyGoods((String) tmp).split(
							"\t");
					if (pclass.length != 5
							|| pclass[IdxClass].equals(UnuseClass)) {
						pclass = null;
						log.debug("classify empty. input=" + tmp);
					}
				}
			}
			// 商品分类优先级第3
			for (int i = 0; i < PRIMARY_URL.length; ++i) {
				tmp = this.logparse.getField(PRIMARY_URL[i]);
				if (tmp != null && tmp.toString().trim().length() != 0) {
					j_obj.put(PRIMARY_URL[i], tmp);

					if (pclass == null) {
						pclass = itemClassify.ClassifyGoods((String) tmp)
								.split("\t");
						if (pclass.length != 5
								|| pclass[IdxClass].equals(UnuseClass)) {
							pclass = null;
							log.debug("classify empty. input=" + tmp);
						}
					}
				}
			}
			if (pclass != null) {
				j_obj.put(CLASSID, pclass[IdxClass]);
			}
			if (j_obj.length() == 0) {
				context.getCounter(CNT.ErrParPrimay).increment(1);
				log.warn("parse primary data is empty.");
				return;
			}
			// TODO 12上的现有服务不支持并发，暂时不能使用多个Mapper同时请求。
			// final int order = -1, waitMS = 100000;
			// try {
			// itemCache = new ItemCacheRedis((String)tmp);
			// } catch (NullPointerException e) {
			// itemClassify.push((String)tmp, order);
			// classId = itemClassify.get(waitMS);
			// }

			for (int i = 0; i < ATOM_STR.length; ++i) { // 原子信息，且在Reduce中用于排序
				tmp = this.logparse.getField(ATOM_STR[i]);
				if (tmp != null && tmp.toString().trim().length() != 0)
					j_obj.put(ATOM_STR[i], tmp);
			}

			oval.set(tmp + SEPA + j_obj);
			try {
				context.write(okey, oval);
				// TODO
				// mos.write(namedOutput, key, value)

			} catch (Exception e) {
				context.getCounter(CNT.ErrMo).increment(1);
				log.error("map() write out failed. " + e.getMessage());
			}
		}

		public void cleanup(Context context) {
			if (mos != null) {
				try {
					mos.close();
					super.cleanup(context);
				} catch (Exception e) {
					log.error("map() cleanup failed. " + e.getMessage());
					throw new RuntimeException(e);
				}
			}
		}
	}

	public static class ReduceAction extends Reducer<Text, Text, Text, Text> {
		private static int N_ACT_MIN = 3, N_CNT_MAX = 50;
		private MultipleOutputs<Text, Text> mos;
		private String pathItem, pathAction;

		private enum CNT {
			RoFewerK, ErrRo, MRo, MRoBef
			// N1, N3, N5, N10, N20, N30
		}

		private EArray enumArr;

		public void setup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			N_ACT_MIN = conf.getInt(nActMin, N_ACT_MIN);
			enumArr.setMax(conf.getInt(nEnumSize, N_CNT_MAX));
			mos = new MultipleOutputs(context);
			pathItem = conf.get("mr.mos.item.path");
			pathAction = conf.get("mr.mos.action.path");
			super.setup(context);

			log.info("reduce::setup N_ACT_MIN=" + N_ACT_MIN + "\tmosItem="
					+ mosItem + "\tmosAction=" + mosAction + "\tpathItem="
					+ pathItem + "\tpathAction=" + pathAction);
		}

		public void reduce(Text key, Iterable<Text> values, Context context) {
			int n = 0;

			String tmp;
			TreeSet<String> act_sort = new TreeSet<String>();
			for (Text ti : values) {
				// JSONObject j_obj = new JSONObject(ti.toString());
				n++;
				act_sort.add(ti.toString()); // 去重
			}

			TreeMultiset<String> cid2freq = TreeMultiset.create();
			JSONObject j_obj;
			JSONArray j_arr = new JSONArray();
			for (String si : act_sort) {
				si = si.split(SEPA)[1];
				j_obj = new JSONObject(si);
				if (j_obj.has(CLASSID))
					cid2freq.add(j_obj.getString(CLASSID));
				j_arr.put(si);
			}
			TreeMap<String, Integer> cidMap = new TreeMap<String, Integer>();
			for (Multiset.Entry<String> ei : cid2freq.entrySet()) {
				cidMap.put(ei.getElement(), ei.getCount());
			}

			context.getCounter(EArray.getElement(n / 10)).increment(1);
			try {
				if (N_ACT_MIN < act_sort.size()) {
					// context.getCounter(CNT.MRoBef).increment(1);
					mos.write(mosAction, key, new Text(j_arr.toString()),
							pathAction);
					if (cidMap.size() != 0)
						mos.write(mosItem, key,
							new Text(new ArrayList<Entry<String, Integer>>(
									cidMap.entrySet()).toString()), pathItem);
					// context.getCounter(CNT.MRo).increment(1);
				} else {
					context.getCounter(CNT.RoFewerK).increment(1);
				}
				log.info("reduce:: write out unique-size=" + act_sort.size());
			} catch (Exception e) {
				context.getCounter(CNT.ErrRo).increment(1);
				log.error("reduce::multiwriteout failed. " + e.getMessage()
						+ "\tmosActioin=" + mosAction + "\tmosItem=" + mosItem);
				// return;
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

		ItemSegmentClass.addClassCacheFile(conf);
		String[] oargs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (oargs.length < 5) {
			System.out.println("Usage:  <out> <dataHour-Range> "
					+ "<dFMT> <inFMT> <UserMinFreq_NCounter_NumRed> "
					+ "[<dFMT2> <inFMT2> ...] ");
			System.exit(4);
		}
		conf.addResource("hadoop.conf");
		// 构造与输出目录平行的结果目录
		String path = args[0].substring(0, args[0].lastIndexOf("/"));
		conf.set("mr.mos.item.path", path+"/" + mosItem + "/" + args[1] + "/");
		conf.set("mr.mos.action.path", path+"/" + mosAction + "/" + args[1] + "/");
		
		int NRed = 4;
		if (oargs[4].indexOf('_') != -1) {
			String[] tmp = oargs[4].split("_");
			conf.setInt(nActMin, Integer.parseInt(tmp[0]));
			if (1 < tmp.length)
				conf.setInt(nEnumSize, Integer.parseInt(tmp[1]));
			if (2 < tmp.length)
				NRed = Integer.parseInt(tmp[2]);
			log.info("setup nActMin=" + conf.get(nActMin) + "\tnEnumSize="
					+ conf.get(nEnumSize) + "\tNRed=" + NRed);
		}

		Job job = new Job(conf, "[merge users action]");
		job.setJarByClass(MergeActionCache.class);
		job.setMapperClass(MapAction.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(ReduceAction.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(NRed); // (16);

		// FileInputFormat.addInputPaths(job, args[1]);
		log.info("input-path: " + oargs[1] + "\t" + oargs[2] + "\t" + oargs[3]);
		boolean setin = HdfsIO.setInput(oargs[1], oargs[2], oargs[3], job);
		if (5 < oargs.length && (oargs.length - 5) % 2 == 0) {// 添加输入路径
			for (int i = 5; i < oargs.length; i += 2) {
				log.info("input-path: " + oargs[1] + "\t" + oargs[i] + "\t"
						+ oargs[i + 1]);
				setin = setin
						&& HdfsIO.setInput(oargs[1], oargs[i], oargs[i + 1],
								job);
			}
		}
		
		HdfsIO.removeDir(oargs[0], job);
		HdfsIO.removeDir(conf.get("mr.mos.item.path"), job);
		HdfsIO.removeDir(conf.get("mr.mos.action.path"), job);
		FileOutputFormat.setOutputPath(job, new Path(args[0]));

		log.info("run::enviroment setup, mosItem=" + mosItem + ", mosAction="
				+ mosAction);
		if (mosItem == null || mosAction == null) {
			System.exit(-1);
		}
		MultipleOutputs.addNamedOutput(job, mosItem, TextOutputFormat.class,
				Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, mosAction, TextOutputFormat.class,
				Text.class, Text.class);

		Date startTime = new Date();
		job.waitForCompletion(true);
		Date end_time = new Date();
		System.out.println("The job took "
				+ (end_time.getTime() - startTime.getTime()) / 1000
				+ " seconds.");
		log.info("job time consume: "
				+ (end_time.getTime() - startTime.getTime()) / 1000);
		return 0;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		int res;
		try {
			res = ToolRunner.run(new Configuration(), new MergeActionCache(),
					args);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

}
