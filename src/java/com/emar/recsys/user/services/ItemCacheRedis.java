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

import org.antlr.grammar.v3.ANTLRv3Parser.throwsSpec_return;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.emar.recsys.user.demo.IKeywords;
import com.emar.recsys.user.util.UtilJson;
import com.emar.util.ConfigureTool;
import com.sun.jersey.api.NotFoundException;
import com.sun.research.ws.wadl.Param;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

/**
 * 商品名分词结果的Redis cache. 不做业务逻辑 与 数据有效性检验。 每次对key进行的操作都将其过去时间延期
 * 
 * @author zhoulm
 * @TODO 1.1 将商品名作为hash的字段， key 采用str的HASH编码。 2.1 引入事件机制。 2.2 重构代码， 抽象出基本框架。 2.3
 *       增加数据库选择功能。
 */
public class ItemCacheRedis {
	private static String host;
	private static int port, timeout, ttlKey, maxAct, maxIdle;
	private static long maxWait, minEviIdlTimMis, timeBetEviRunMis;

	private static final String CONF_PATH = "redis.conf";
	private static final String sepa = "\u0001", DHOST = "114.111.164.12",
			hkeyCount = "cnt", hkeyOldestTime = "t", skeyClass = "cidx",
			skeyPlat = "plat", skeyUser = "user";
	private static final int DPORT = 3003, DTIME_OUT = 20000,
			/** expires of key. TOOD: key^user仅仅保存4Week即可 手动删除之 */
			DTTL_KEY = 3600 * 24 * 90;

	private static JedisPool jedisPool = null;
	private static JedisPoolConfig poolConfig = null;
	/** 是否调试， 是否存储 平台信息到Redis */
	private static boolean debug, dumpUser, DBOOL = false;
	private static Logger logger = Logger.getLogger(ItemCacheRedis.class);

	private Jedis jedis;
	public ItemClassCache itemCache;

	static {
		ConfigureTool pro = new ConfigureTool();
		pro.addResource(CONF_PATH);

		maxAct = pro.getInt("classCache.maxActive", 100);
		maxIdle = pro.getInt("classCache.maxIdle", 100);
		maxWait = pro.getLong("classCache.maxWait", 10000);
		minEviIdlTimMis = pro.getLong("classCache.minEvictableIdleTimeMillis",
				100000);
		timeBetEviRunMis = pro.getLong(
				"classCache.timeBetweenEvictionRunsMillis", 600000);
		poolConfig = new JedisPoolConfig();
		// 控制一个pool可分配多少个jedis实例, -1指无限制
		poolConfig.setMaxActive(maxAct);
		// 1个pool最多有多少个状态为idle(空闲的)的jedis实例。
		poolConfig.setMaxIdle(maxIdle);
		// 当borrow(引入)一个jedis实例时，最大的等待时间，超时则抛异常
		poolConfig.setMaxWait(maxWait);
		// 当borrow(引入)一个jedis实例时，进行有效监测 无效则从连接池中移除并继续申请
		poolConfig.setTestOnBorrow(true);
		// 连接空闲的最小时间 达到则可将该连接释放 -1指不移除
		poolConfig.setMinEvictableIdleTimeMillis(minEviIdlTimMis);
		// “空闲链接”检测线程的监测周期
		poolConfig.setTimeBetweenEvictionRunsMillis(timeBetEviRunMis);

		host = pro.get("classCache.host", DHOST);
		port = pro.getInt("classCache.port", DPORT);
		timeout = pro.getInt("classCache.timeout", DTIME_OUT);
		jedisPool = new JedisPool(poolConfig, host, port, timeout);

		ttlKey = pro.getInt("classCache.ttl_key", DTTL_KEY);
		debug = pro.getBoolean("classCache.debug", DBOOL);
		dumpUser = pro.getBoolean("classCache.dump_user", DBOOL);

		logger.info(String.format("envirment set configure&make connection:"
				+ "\nhost=%s,port=%d,timeout=%d,ttl_key=%d,debug=%b,dump_user=%b,"
				+ "maxActive=%d,maxIdle=%d,maxWait=%d,minEviIdlTimMis=%d,"
				+ "timeBetEviRunMis=%d", host, port, timeout, ttlKey, debug,
				dumpUser, maxAct, maxIdle, maxWait, minEviIdlTimMis,
				timeBetEviRunMis));
	}

	/** 返还资源到连接池 */
	public static void freeResource(Jedis jedis) {
		if (jedis != null)
			jedisPool.returnResource(jedis);
	}

	/** Redis中 分类Cache的操作接口。 TODO： 将架构{Redis通用操作包含业务操作}反转之。 */
	private class ItemClassCache {
		public String item;
		public Set<String> classId;
		public String time;
		public Integer count;
		/** 对商品有行为的平台频率信息 */
		// public Map<String, Integer> platInfo;
		public Map<String, Integer> userInfo;

		/** 内部使用。 */
		private ItemClassCache() {
		}

		/** 创建商品对象 + 查 Redis */
		public ItemClassCache(String item) throws NullPointerException {
			this.init(item);

			boolean bool = this.getCache();
		}

		/** 更新单个用户的信息 + 写Redis. */
		public ItemClassCache(String item, String cid, String t, String user,
				String plat) throws NullPointerException {
			this.init(item);

			this.classId.add(cid);
			this.time = t;
			count = 1; //
			if (dumpUser)
				// this.platInfo.put(plat, 1);
				this.userInfo.put(user, 1);

			this.updateCache();
		}

		private void init(String ite) throws NullPointerException {
			if (ite == null || ite.trim().length() == 0)
				throw new NullPointerException("item name is null.");
			final int SIZE = 4;
			item = ite;
			classId = new HashSet<String>(SIZE, 1.0f);
			time = ""; // 最小字符串
			count = 0;
			if (dumpUser)
				// platInfo = new HashMap<String, Integer>(SIZE, 1.0f);
				userInfo = new HashMap<String, Integer>(SIZE << SIZE, 1.0f);
		}

		private boolean getCache() throws NotFoundException {
			// 查Redis 读出商品类目的信息
			Map<String, String> base = getItem(this.item);
			if (base.size() == 0)
				throw new NotFoundException("Redis don't find item:"
						+ this.item);

			time = base.get(hkeyOldestTime);
			count = Integer.parseInt(base.get(hkeyCount).trim());

			classId = getItemClass(this.item);
			if (dumpUser)
				// platInfo = getItemPlats(this.item);
				userInfo = getItemUsers(this.item);
			return true;
		}

		private boolean updateCache() {
			ItemClassCache old;
			try {
				old = new ItemClassCache(this.item);
			} catch (NullPointerException e) {
				old = new ItemClassCache();
				old.item = this.item;
				old.init(this.item);
			}
			// 合并 历史实例
			this.count += old.count;
			if (this.time.compareTo(old.time) < 0)
				this.time = old.time;
			this.classId.addAll(old.classId);
			if (dumpUser)
				// for (Map.Entry<String, Integer> ei : old.platInfo.entrySet())
				// {
				// if (this.platInfo.containsKey(ei.getKey()))
				// this.platInfo.put(ei.getKey(),
				// this.platInfo.get(ei.getKey()) + ei.getValue());
				// else
				// this.platInfo.put(ei.getKey(), ei.getValue());
				// }
				for (Map.Entry<String, Integer> ei : old.userInfo.entrySet()) {
					if (this.userInfo.containsKey(ei.getKey()))
						this.userInfo.put(ei.getKey(),
								this.userInfo.get(ei.getKey()) + ei.getValue());
					else
						this.userInfo.put(ei.getKey(), ei.getValue());
				}

			// 插入 Redis
			setHash(this.item, hkeyCount, this.count + "");
			setHash(this.item, hkeyOldestTime, this.time);
			setSet(this.item, skeyClass, this.classId);
			if (dumpUser)
				// setItemPlat(this.item, this.platInfo);
				setItemUser(this.item, this.userInfo);
			return true;
		}

		public String toString() {
			return String.format("%s::[lastTime=%s, frequence=%d, item=%s, "
					+ "classifyid=%s, user=%s, plat=%s]", this.getClass()
					.getName(), this.time, this.count, this.item, this.classId
					.toArray(), this.userInfo.toString());
			// , this.platInfo.toString());
		}
	}

	/**
	 * 取 Redis的 Item分类 hash结果。
	 * 
	 * @return
	 */
	public Map<String, String> getItem(String key) {
		Map<String, String> res = new HashMap<String, String>();
		if (key == null || key.trim().length() == 0)
			return res;

		key = RedisKey.keyItem(key);

		jedis = jedisPool.getResource();
		res = jedis.hgetAll(key);
		jedis.expire(key, ttlKey);
		jedisPool.returnResource(jedis);

		logger.info("getItem() key=" + key + "\tres=" + res);
		return res;
	}

	/** 插入基本信息到Redis */
	public boolean setHash(String key, String field, String value) {
		key = RedisKey.keyItem(key);
		if (key == null)
			return false;

		jedis = jedisPool.getResource();
		Long status = jedis.hset(key, field, value);
		jedis.expire(key, ttlKey);
		jedisPool.returnResource(jedis);

		logger.info("setHash() key=" + key + "\tfield=" + field + "\tvalue="
				+ value);
		return true;
	}

	/** 插入基本信息的 Redis. key不做变换。 */
	public boolean setRawHash(String key, String field, String value) {
		if (key == null)
			return false;

		jedis = jedisPool.getResource();
		jedis.hset(key, field, value);
		jedis.expire(key, ttlKey);
		jedisPool.returnResource(jedis);

		logger.info("setRawHash key=" + key + "\tfield=" + field + "\tvalue="
				+ value);
		return true;
	}

	public boolean setSet(String key, String field, Set<String> value) {
		key = RedisKey.keyItemInfo(key, field);
		if (key == null)
			return false;

		jedis = jedisPool.getResource();
		jedis.sadd(key, value.toArray(new String[value.size()]));
		jedis.expire(key, ttlKey);
		jedisPool.returnResource(jedis);

		logger.info("setSet key=" + key + "\tfield=" + field + "\tvalue="
				+ value);
		return true;
	}

	/** 取Redis的 Item分类的 HASH子域(拼接的key)属性。 */
	public Map<String, String> getItemHash(String key, String field) {
		Map<String, String> res = new HashMap<String, String>();
		key = RedisKey.keyItemInfo(key, field);
		if (key == null)
			return res;

		jedis = jedisPool.getResource();
		res = jedis.hgetAll(key);
		jedis.expire(key, ttlKey);
		jedisPool.returnResource(jedis);

		logger.info("getItemHash key=" + key + "\tfield=" + field + "\tvalue="
				+ res);
		return res;
	}

	/** 取Redis的 Item分类的 SET子域属性。 */
	public Set<String> getItemSet(String key, String field) {
		Set<String> res = new HashSet<String>();
		key = RedisKey.keyItemInfo(key, field);
		if (key == null)
			return res;

		jedis = jedisPool.getResource();
		res = jedis.smembers(key);
		jedis.expire(key, ttlKey);
		jedisPool.returnResource(jedis);

		logger.info("setItemSet key=" + key + "\tfield=" + field + "\tvalue="
				+ res);
		return res;
	}

	private Map<String, Integer> str2Int(Map<String, String> info) {
		Map<String, Integer> res = new HashMap<String, Integer>();
		if (info == null || info.size() == 0)
			return res;

		Integer ai = null;
		for (Map.Entry<String, String> ei : info.entrySet()) {
			try {
				ai = Integer.parseInt(ei.getValue());
				res.put(ei.getKey(), ai);
			} catch (NumberFormatException e) {
				logger.error("unnumber data:" + ei);
			}
		}

		logger.info("str2Int input=" + info + "\tres=" + res);
		return res;
	}

	private Map<String, String> int2Str(Map<String, Integer> info) {
		Map<String, String> res = new HashMap<String, String>();
		if (info == null || info.size() == 0)
			return res;

		for (Map.Entry<String, Integer> ei : info.entrySet())
			res.put(ei.getKey(), ei.getValue() + "");

		logger.info("int2Str input=" + info + "\tres=" + res);
		return res;
	}

	/** 不存在则返回null @param key 商品字符串 */
	public String getItemClassid(String key) {
		Map<String, String> tMap = this.getItem(key);
		String res = tMap.get(skeyClass);

		return res;
	}

	/** 不存在时返回-1. */
	public Integer getItemCount(String key) {
		Map<String, String> tMap = this.getItem(key);
		Integer res = -1;
		if (tMap.containsKey(hkeyCount))
			res = Integer.parseInt(tMap.get(hkeyCount));
		return res;
	}

	public String getItemLastTime(String key) {
		Map<String, String> tMap = this.getItem(key);
		String res = tMap.get(hkeyOldestTime);
		return res;
	}

	/** 返回当前商品的用户信息。 */
	public Map<String, Integer> getItemUsers(String key) {
		Map<String, String> tMap = this.getItemHash(key, skeyUser);
		Map<String, Integer> iUser = str2Int(tMap);
		return iUser;
	}

	public boolean setItemUser(String key, Map<String, Integer> userInfo) {
		key = RedisKey.keyItemInfo(key, skeyUser);
		if (key == null)
			return false;

		jedis = jedisPool.getResource();
		jedis.hmset(key, int2Str(userInfo));
		jedis.expire(key, ttlKey);
		jedisPool.returnResource(jedis);
		return true;
	}

	/** @deprecated 返回当前商品的平台信息 */
	public Map<String, Integer> getItemPlat(String key) {
		Map<String, String> tMap = this.getItemHash(key, skeyPlat);
		Map<String, Integer> iPlat = str2Int(tMap);
		return iPlat;
	}

	/** @deprecated */
	public boolean setItemPlat(String key, Map<String, Integer> platInfo) {
		key = RedisKey.keyItemInfo(key, skeyPlat);
		if (key == null)
			return false;

		jedis = jedisPool.getResource();
		jedis.hmset(key, int2Str(platInfo));
		jedisPool.returnResource(jedis);
		return true;
	}

	/** 返回当前商品的分类信息 */
	public Set<String> getItemClass(String key) {
		Set<String> iClass = this.getItemSet(key, skeyClass);
		return iClass;
	}

	public ItemCacheRedis(String item) throws NullPointerException {
		// jedis = jedisPool.getResource();
		this.itemCache = new ItemClassCache(item);
	}

	public ItemCacheRedis(String ite, String cid, String t, String user, String plat)
			throws NullPointerException {
		this.itemCache = new ItemClassCache(ite, cid, t, user, plat);
	}

	public static void destroy() {
		jedisPool.destroy();
	}

	public void set(final String key, final String value) {
		jedis = jedisPool.getResource();

		try {
			jedis.set(key, value);

			jedisPool.returnResource(jedis);
		} catch (Exception e) {
			jedisPool.returnBrokenResource(jedis);
			throw new RuntimeException(e);
		}
	}

	public String get(final String key) {
		jedis = jedisPool.getResource();
		try {
			String result = jedis.get(key);
			jedisPool.returnResource(jedis);
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
		jedis = jedisPool.getResource();
		Long res = null;
		try {
			res = jedis.rpush(key, data);
			jedisPool.returnResource(jedis);
		} catch (Exception e) {
			logger.error("list rpush failed. key=" + key + ".\terr-msg:"
					+ e.getMessage());
		} finally {
			jedisPool.returnBrokenResource(jedis);
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
	 * 对一个文件调用 Redis分类接口.
	 * 
	 * @throws IOException
	 */
	public static boolean process(String path, String key, String opath)
			throws IOException {

		int NBad = 0, Nnil = 0, NJsonErr = 0, N = 0, NoResult = 0;

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
			// TODO 抽取逻辑处理为子函数

			wbuf.write("");
			wbuf.newLine();
		}
		rbuf.close();
		wbuf.close();

		logger.warn("[Info] process::Cnt_Bad=" + NBad + "\tUnclassify="
				+ NoResult);
		return true;
	}

	public static void main(String[] args) {
		if (args.length < 0) { // 2) {
			System.out.println("Usage: <input> <output> [sepa-str] ");
			System.exit(2);
		}
		// args = new String [] { // unit test.
		// "D:/Data/MR-codes/data/test/act_merge.100",
		// "D:/Data/MR-codes/data/test/act_merge.redis"
		// };
		String sepa = args.length < 3 ? "\t" : args[2];
		String ite = "a", cid = "123", t = "20130101", user = "ua", plat = "pa";
		ItemCacheRedis icache;

		try {
			icache = new ItemCacheRedis(ite);

			// } catch (NullPointerException e) {
		} catch (NotFoundException e) {
			icache = new ItemCacheRedis(ite, cid, t, user, plat);
			icache = new ItemCacheRedis(ite);
		}

	}

}
