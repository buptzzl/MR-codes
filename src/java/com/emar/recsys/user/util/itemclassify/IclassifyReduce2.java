package com.emar.recsys.user.util.itemclassify;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.emar.recsys.user.log.LogParse;
import com.emar.recsys.user.util.UtilObj;

/**
 * @author zhoulm
 * 
 */
public class IclassifyReduce2 extends Reducer<Text, Text, Text, Text> {

	// 定义Map & Reduce 中通用的对象
//定义Map & Reduce 中通用的对象
	private static final String SEPA = LogParse.SEPA, MRSEPA = LogParse.SEPA_MR,
			PLAT = LogParse.PLAT, EMAR = LogParse.EMAR, MAGIC =LogParse.MAGIC;
	private static final String MRKC = "C_";
			
	private static final int MIdxTime = 0, MIdxClass = 1, MIdxCInfo = 2, MIdxName = 3,
			MIdxPri = 4, MIdxDomain = 5;

	private static enum Counters {
		ErrRo, RoUser, RoCamp
	};

	private MultipleOutputs<Text, Text> mos = null;
	private Text okey = new Text(), oval = new Text();

	public void setup(Context context) throws IOException, InterruptedException {
		mos = new MultipleOutputs(context);
		super.setup(context);
	}

	public void reduce(Text key, Iterable<Text> values, Context context) {

		String rkey = key.toString(), skey;
		boolean isUser = true;
		
		if(rkey.length() == 0) {
			// 分类失败的badcase 
			for(Text v: values) {
				try {
					mos.write("badclassify", key, v, "badclassify/");  //不能使用Mout
				} catch (Exception e) {
				}
			}
			return;
		}

		if (rkey.startsWith(MRKC)) {
			// 处理基于 campid 的数据
			skey = rkey.substring(2);
			isUser = false;
		} else {
			// 处理基于 platuser 的数据
			skey = rkey;
			isUser = true;
		}
		ArrayList<String> info = new ArrayList<String>();
		HashMap<String, Float> crank = new HashMap<String, Float>();
		for (Text v : values) {
			String sval = v.toString();
			info.add(sval); // 直接写出原始分类数据
			String[] atom = sval.split(SEPA);

			crank.put(
					atom[MIdxClass],
					(float) (crank.containsKey(atom[MIdxClass]) ? crank
							.get(atom[MIdxClass]) + 1 : 1)); // 不衰减
			// crank.get(atom[MIdxClass])+1.0/icnt: 1.0/icnt));
		}
		Collections.sort(info); // 主要按时间排序
		List<Entry<String, Float>> lrank = UtilObj.entrySortFloat(crank, true);
		
		String orank, oinfo, ordir, orinfo;
		if (isUser) {
			orank = "userClassrank"; // 多输出的类目类型1
			oinfo = "userInfo";
			ordir = "uUserRank/";
			orinfo = "userInfo/";
		} else {
			orank = "campClassrank";
			oinfo = "campInfo";
			ordir = "uCampRank/";
			orinfo = "campInfo/";
		}
		okey.set(skey);
		try {
			oval.set(lrank.toString());
			// mos.write(orank, okey, oval); // [a=1, b=2]
			mos.write(orank, okey, oval, ordir);
			context.getCounter(Counters.RoUser).increment(1);
			for (String s : info) {
				oval.set(s);
				mos.write(oinfo, okey, oval, orinfo);
				context.getCounter(Counters.RoCamp).increment(1);
			}
		} catch (Exception e) {
			context.getCounter(Counters.ErrRo).increment(1);
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