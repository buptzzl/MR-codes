package com.emar.recsys.user.util.itemclassify;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashSet;
import java.util.StringTokenizer;
import java.util.Vector;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.emar.recsys.user.log.LogParse;
import com.emar.recsys.user.services.ItemCacheRedis;

public class IclassifyMap2 extends Mapper<LongWritable, Text, Text, Text> {

	// 定义Map & Reduce 中通用的对象
	private static final String SEPA = LogParse.SEPA,
			MRSEPA = LogParse.SEPA_MR, PLAT = LogParse.PLAT,
			EMAR = LogParse.EMAR, MAGIC = LogParse.MAGIC;
	private static final String MRKC = "C_", UnuseClass = "*";
	// 分类结果下标
	private static final int IdxClass = 2, IdxClassInfo = 1;
	// 出现无效值时，指定的默认值
	private static final String VDefPCnt = "1", VDefPPrice = "0.0",
			VDefPTime = "20130101000000", VDefHost = "emar.com";

	private static Calendar c = Calendar.getInstance();
	private static SimpleDateFormat datefmt = new SimpleDateFormat(
			"yyyyMMddHHmmss");
	private ItemSegmentClass itemClassify = null;
	private LogParse logparse;
	private Text okey = new Text(), oval = new Text();

	// private MultipleOutputs<Text, Text> mos;

	private static enum Counters {
		MoUser, MoCamp, Err, ErrLog, ErrMO, ErrInit, ErrClassify, ErrParse, ErrPName
	};

	public void setup(Context context) {
		// 日志解析初始化
		try {
			logparse = new LogParse();
		} catch (ParseException e) {
			context.getCounter(Counters.ErrInit).increment(1);
			e.printStackTrace();
			System.exit(e.getErrorOffset());
		}
		// mos = new MultipleOutputs(context);
		try {
			itemClassify = new ItemSegmentClass(context);
		} catch (IOException e) {
			throw new RuntimeException(e.getMessage());
		} catch (URISyntaxException e) {
			throw new RuntimeException(e.getMessage());
		}
	}
	
	/**
	 * key:
	 */
	public void map(LongWritable key, Text val, Context context) {
		// String result = ClassifyGoods("葡萄酒100ml");
		String line = val.toString();
		String path = ((FileSplit) context.getInputSplit()).getPath()
				.toString();
		try {
			// this.logparse.base.isdug = true;
			this.logparse.parse(line, path);
		} catch (ParseException e1) {
			System.out.println("[ERROR] IclassifyMap2::map() " + this.logparse);
		}
		if (!this.logparse.base.status) {
			context.getCounter(Counters.ErrParse).increment(1);
			return;
		}
		if ((this.logparse.base.prod_name == null)
				&& (this.logparse.base.prod_type_name == null)) {
			context.getCounter(Counters.ErrPName).increment(1);
			System.out.println("[Err] IclassifyMap2::map() path=" + path
					+ "\nparse-res=" + this.logparse + "\n"
					+ this.logparse.base.isdebug + "\nindata=" + line);
			return;
		}
		this.logparse.base.prod_name.replaceAll(", ", ","); // 便于后续存储到数组中 使用,
															// 做分割符
		this.logparse.base.prod_type_name.replaceAll(", ", ",");
		String[] pclass;
		pclass = itemClassify.ClassifyGoods(this.logparse.base.prod_name)
				.split("\t");
		if (pclass.length != 5 || pclass[IdxClass].equals(UnuseClass)) {
			pclass = itemClassify.ClassifyGoods(
					this.logparse.base.prod_type_name).split("\t");
		}
		if (pclass.length != 5 || pclass[IdxClass].equals(UnuseClass)) {
			context.getCounter(Counters.ErrClassify).increment(1);
			try {
				context.write(new Text(""), val);
				// 不能有效写结果， Map的输出为空
				// mos.write("badclassify", new
				// Text(this.logparse.logpath.toString()),
				// val, "badclassify/");
			} catch (Exception e) {
			}
			return;
		}

		try {
			oval.set(logparse.base.time + SEPA + pclass[IdxClass] + SEPA
					+ pclass[IdxClassInfo] + SEPA + logparse.base.prod_name
					+ SEPA + logparse.base.prod_price + SEPA
					+ logparse.base.domain);
			String os = this.logparse.buildUidKey();
			if (os != null) {
				okey.set(os);
				context.write(okey, oval); // key=puid
				context.getCounter(Counters.MoUser).increment(1);
			}

			String fmt = MRKC + "%s" + MAGIC + logparse.logpath.plat;
			if (this.logparse.base.camp_ids == null) {
				okey.set(String.format(fmt, this.logparse.base.camp_id));
				context.write(okey, oval);
				context.getCounter(Counters.MoCamp).increment(1);
			} else {
				String[] cs = logparse.base.camp_ids.split(LogParse.SEPA_CAMPS);
				for (String s : cs) {
					if (s.length() > 1) {
						okey.set(String.format(fmt, s));
						context.write(okey, oval);
						context.getCounter(Counters.MoCamp).increment(1);
					}
				}
			}
		} catch (Exception e) {
			context.getCounter(Counters.ErrMO).increment(1);
		}

	}


}
