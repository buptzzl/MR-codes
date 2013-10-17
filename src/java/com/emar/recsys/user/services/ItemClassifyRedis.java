package com.emar.recsys.user.services;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.json.JSONArray;
import org.json.JSONObject;

import com.emar.recsys.user.demo.IKeywords;
import com.emar.recsys.user.util.UtilJson;
import com.ibm.icu.util.Calendar;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

/**
 * 商品名分词+分类。 基于 Redis服务端提供的消息队列机制，采用端口号的方式区别队列。
 * 
 * @see 取队列中元素时须先清空。 突出的顺序不保证。
 * @author Administrator
 * 
 */
public class ItemClassifyRedis {
	private static String host;
	private static int port;
	private static int timeout;

	private static JedisPool jedisPool;
	private static JedisPoolConfig poolConfig;
	private static ItemClassifyRedis instance;

	private static boolean debug = true;
	private Jedis jedis;

	static {
		Configuration pro = new Configuration();
		// ConfigProperties pro = AdConfiguration.readisConf ;
		timeout = pro.getInt("timeout", 20000);
		poolConfig = new JedisPoolConfig();
		poolConfig.setMaxActive(pro.getInt("maxActive", 20));
		poolConfig.setMaxIdle(pro.getInt("maxIdle", 10));
		poolConfig.setMaxWait(pro.getLong("maxWait", 10000));
		poolConfig.setMinEvictableIdleTimeMillis(pro.getLong(
				"minEvictableIdleTimeMillis", 100000));
		// poolConfig.setMinIdle(pro.getInt("minIdle", 5)) ;
		poolConfig.setTimeBetweenEvictionRunsMillis(pro.getLong(
				"timeBetweenEvictionRunsMillis", 600000));
		// host = pro.getString("host", "114.111.164.12") ;
		host = pro.get("host", "114.111.164.12");
		port = pro.getInt("port", 6379);

		instance = new ItemClassifyRedis();
	}

	private ItemClassifyRedis() {
		jedisPool = new JedisPool(poolConfig, host, port, timeout);
	}

	public static ItemClassifyRedis getInstance() {
		return ItemClassifyRedis.instance;
	}

	public void destroy() {
		getJedisPool().destroy();
	}

	public void set(final String key, final String value) {
		jedis = jedisPool.getResource();

		try {
			jedis.set(key, value);
			jedisPool.returnBrokenResource(jedis);
		} catch (Exception e) {
			// jedisPool.returnBrokenResource(jedis);
			destroy();
			throw new RuntimeException(e);
		}
		jedis = null;
	}

	public String get(final String key) {
		jedis = jedisPool.getResource();
		try {
			String result = jedis.get(key);
			jedisPool.returnBrokenResource(jedis);
			return result;

		} catch (Exception e) {
			jedisPool.returnBrokenResource(jedis);
			throw new RuntimeException(e);
		}
	}

	/** 删除若干指明名字 的队列 */
	public void del(String... key) {
		jedis = jedisPool.getResource();
		try {
			jedis.del(key);
			jedisPool.returnResource(jedis);
		} catch (Exception e) {
			jedisPool.returnBrokenResource(jedis);
			throw new RuntimeException(e);
		}
	}

	/** 返回全部的队列名 */
	public Set<String> keys(String pattern) {
		Jedis jedis = jedisPool.getResource();
		try {
			Set<String> result = jedis.keys(pattern);
			jedisPool.returnResource(jedis);
			return result;

		} catch (Exception e) {
			jedisPool.returnBrokenResource(jedis);
			throw new RuntimeException(e);
		}
	}

	/**
	 * URL\u0001主题[kernel]\u0001面包屑\u0001显性属性块\u0001\描述\u0001评论
	 * 
	 * @param key
	 * @param data
	 */
	public void setData2Queue(String key, String data) {
		Jedis jedis = jedisPool.getResource();
		Long res = null;
		try {
			res = jedis.rpush(key, data);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			jedisPool.returnResource(jedis);
		}
	}

	/**
	 * 
	 * @param key
	 * @return
	 */
	public String getDataFromQueue(String key) {
		Jedis jedis = jedisPool.getResource();

		String result = "";
		try {
			result = jedis.lpop(key);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			jedisPool.returnResource(jedis);
		}

		return result;
	}

	private JedisPool getJedisPool() {
		return jedisPool;
	}

	/**
	 * 输入标准格式： URL\u0001主题\u0001面包屑\u0001显性属性块\u0001\描述\u0001评论
	 * 
	 * @param atom
	 *            包含 主题， 或完整的格式
	 * @return
	 */
	public boolean push(String atom, int index) {
		if (atom == null)
			return false;

		final String keyRedisIn = "HTPPQUEUE", sepa = "\u0001";
		final Pattern regNoline = Pattern.compile("\\r\\n");
		final BASE64Encoder b64Encoder = new BASE64Encoder();
		final ItemClassifyRedis source = ItemClassifyRedis.getInstance();

		String[] items = atom.split(sepa);
		if (items.length < 6) {
			atom = b64Encoder.encode(atom.getBytes());
			atom = String.format("\u0001%s\u0001\u0001\u0001\u0001", atom);
		} else {
			atom = "";
			atom = String.format("%s\u0001%s\u0001%s\u0001%s\u0001%s\u0001%s",
					b64Encoder.encode(items[0].getBytes()),
					b64Encoder.encode(items[1].getBytes()),
					b64Encoder.encode(items[2].getBytes()),
					b64Encoder.encode(items[3].getBytes()),
					b64Encoder.encode(items[4].getBytes()),
					b64Encoder.encode(items[5].getBytes()));
		}
		atom = regNoline.matcher(atom).replaceAll("");// 删除 空白符
		atom += b64Encoder.encode(("_" + index).getBytes());// 添加ID到末尾字段
		if (debug)
			System.out.println("[Info] B64encode input=" + atom);

		source.setData2Queue(keyRedisIn, atom);
		return true;
	}

	/**
	 * 从Redis队列取1个结果. 输出格式： 分类字符串\1index\1ClassID\1ClassName
	 * 
	 * @throws InterruptedException
	 * 
	 */
	public String get(int timeout) throws InterruptedException {
		final String keyRedisOut = "QUEUEHTPP", sepa = "\u0001";
		final BASE64Decoder b64Decoder = new BASE64Decoder();
		final ItemClassifyRedis source = ItemClassifyRedis.getInstance();
		int timeStep = timeout / 100;// 最大请求次数
		timeStep = timeStep <= 0 ? 100 : timeStep;

		String result = null;
		int timer = 0;
		result = source.getDataFromQueue(keyRedisOut);
		while (result == null && timer < timeout) {
			timer = timer + timeout;
			Thread.sleep(timer); // 等待
			result = source.getDataFromQueue(keyRedisOut);
		}

		try {
			result = new String(b64Decoder.decodeBuffer(result));
		} catch (IOException e) {
			result = null;
		} catch (NullPointerException e) {
		}
		return result;
	}

	/**
	 * 对一个文件调用 Redis分类接口.
	 * 
	 * @throws IOException
	 */
	public static boolean process(String path, String key, String opath)
			throws IOException {
		final String keyRedisOut = "QUEUEHTPP", S_MR = "\t", NoResult = "NIL", 
				keyArr = IKeywords.RawLog, sVersion = "A_M_1"; 
		final String[] keys = new String[] { "prod_name", "pagewords" };
		final int order = -1, wMS = 100000;
		ItemClassifyRedis source = ItemClassifyRedis.getInstance();
		source.getJedisPool().getResource().del(keyRedisOut);
		int NBad = 0, Nnil = 0;

		String data, keyWords, id;
		JSONArray jArr;
		JSONObject jObj, jAtom;
		String[] arr;
		Map<String, Integer> Cid = new HashMap<String, Integer>();
		
		BufferedWriter wbuf = new BufferedWriter(new FileWriter(opath));
		BufferedReader rbuf = new BufferedReader(new FileReader(path));
		for (data = rbuf.readLine(); data != null; data = rbuf.readLine()) {
			Cid.clear();
			data = rbuf.readLine();
			jObj = UtilJson.parseJson(data, S_MR, keyArr, 1, 0);
			if (jObj == null)
				continue;
			jArr = jObj.getJSONArray(keyArr);
			for (int i = 0; i < jArr.length(); ++i) {
				
				jAtom = new JSONObject(jArr.get(i).toString());
				
//				System.out.println("[info] " + jArr.get(i) + "\n" + jAtom);
				for (int j = 0; j < keys.length; ++j) {
					if (jAtom.has(keys[j])) {
						source.push(jAtom.getString(keys[j]), order);
						try {
							id = source.get(wMS);
							if (id.indexOf(NoResult) != -1)
								Cid.put(id, Cid.containsKey(id) ? Cid.get(id)+1 : 1);
							else
								++Nnil;
						} catch (InterruptedException e) {
							e.printStackTrace();
							++NBad;
						}
						id = null;
					}
				}
			}
			wbuf.write(String.format("%s\t%s\t%s", jObj.get(IKeywords.KUid), 
					Cid.toString(), sVersion));
			wbuf.newLine();
		}
		rbuf.close();
		wbuf.close();
		
		if (debug) 
			System.out.println("[Info] process::Cnt_Bad=" + NBad
					+ "\tUnclassify=" + NoResult);
		return true;
	}
	
	public static void main(String[] args) {
		if (args.length < 0) { // 2) {
			System.out.println("Usage: <input> <output> [sepa-str] ");
			System.exit(2);
		}
		args = new String [] {
				"D:/Data/MR-codes/data/test/act_merge.100", 
				"D:/Data/MR-codes/data/test/act_merge.redis"
				};
		String sepa = args.length < 3 ? "\t" : args[2];  
		try {
			ItemClassifyRedis.getInstance().process(args[0], sepa, args[1]);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * URL\u0001主题\u0001面包屑\u0001显性属性块\u0001\描述\u0001评论
	 * 
	 * @param args
	 * @throws FileNotFoundException
	 */
	public static void main_test(String[] args) throws IOException {

		final String keyRedisIn = "HTPPQUEUE", keyRedisOut = "QUEUEHTPP";
		final Pattern regNoline = Pattern.compile("\\r\\n");
		// final String keyRedisIn = "EAW", keyRedisOut = "EQA";

		// String name = "稻草人新款欧美经典时尚金锁真皮单肩斜跨男士挎包MMD50034M-03 啡色(huadong)" ;
		String name = "天谱乐食Tender Plus 澳洲120天谷饲西冷牛排 250g-天谱乐食 Tender Plus牛羊肉 【品牌 介绍 价格 图片 评论】 - 顺丰优选sfbest.com@@@顺丰优选(sfbest.com)提供澳大利亚天谱乐食 Tender Plus牛羊肉，天谱乐食Tender Plus 澳洲120天谷饲西冷牛排 250g价格最低，包括天谱乐食Tender Plus 澳洲120天谷饲西冷牛排 250g食用指南、健康知识以及天谱乐食Tender Plus 澳洲120天谷饲西冷牛排 250g图片、参数、评论、晒单、饮食文化等信息。选购天谱乐食 Tender Plus牛羊肉上顺丰优选，优选美食,顺丰到家！@@@天谱乐食Tender Plus 澳洲120天谷饲西冷牛排 250g,天谱乐食 Tender Plus,生鲜食品,精品肉类,牛羊肉,顺丰优选,进口食品@@@";
		// String value =
		// "www.baidu.com\u0001妹妹好漂亮\u0001操\u0001\u0001\u0001www.baidu.com";

		BASE64Decoder b64Decoder = new BASE64Decoder();
		BASE64Encoder b64Encoder = new BASE64Encoder();

		// System.out.println(UUIDUtil.getBASE64(name));
		name = b64Encoder.encode(name.getBytes());
		System.out.println(Calendar.getInstance().getTime());
		// + "[info] name-encode: " + name);
		name = regNoline.matcher(name).replaceAll("");

		ItemClassifyRedis source = ItemClassifyRedis.getInstance();
		String data = null;

		final String pname = "D:/Data/MR-codes/data/test/order_badclassify_1010_name.dat";
		final int N = 100;
		int nget = 0;
		source.getJedisPool().getResource().del(keyRedisOut);
		BufferedReader rbuf = new BufferedReader(new FileReader(pname));
		// for(data = rbuf.readLine(); data != null; data = rbuf.readLine()) {
		for (int i = 0; i < N; i++) {
			data = rbuf.readLine();
//			source.push(data, i);
			source.push("", i);
			String result = null;
			try {
				result = source.get(100000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			System.out.println("[info] input=" + data + "\noutput=" + result);

		}
		// source.setData2Queue(keyRedisIn, data);
		rbuf.close();

		while (nget < N) {

			String result = null;
			result = source.getDataFromQueue(keyRedisOut);

			if (result != null) {
				// result = UUIDUtil.getDataFromBase64(result) ;
				try {
					result = new String(b64Decoder.decodeBuffer(result));
					String[] results = result.split("\u0001");
					System.err.println("[info] result:" + result);
					nget++;
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

		}
		//
	}

}
