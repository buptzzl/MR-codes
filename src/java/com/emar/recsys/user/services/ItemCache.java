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
import com.sun.research.ws.wadl.Param;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

/**
 * 商品名分词结果的Redis cache.
 * 
 * @author zhoulm
 * @TODO  1 增加过期时间 。 2 重构代码， 抽象出基本框架。
 */
public class ItemCache {
	private static String host;
	private static int port;
	private static int timeout;

	private static final String sepa = "\u0001", DHOST = "114.111.164.12",
			hkeyCount = "cnt", hkeyOldestTime = "t", skeyClass = "cidx", 
			skeyPlat = "plat", skeyUser = "user";
	private static final int DPORT = 3003, DTEME_OUT = 20000;

	private static JedisPool jedisPool = null;
	private static JedisPoolConfig poolConfig = null;
	private static ItemCache instance = null;
	/** 是否调试， 是否存储 平台信息到Redis */
	private static boolean debug = true, dumpPlat;
	private static Logger logger = Logger.getLogger(ItemCache.class);

	private Jedis jedis;
	public ItemClassCache itemCache;

	static {
		ConfigureTool pro = new ConfigureTool();

		poolConfig = new JedisPoolConfig();
		// 控制一个pool可分配多少个jedis实例, -1指无限制
		poolConfig.setMaxActive(pro.getInt("maxActive", 100));
		// 1个pool最多有多少个状态为idle(空闲的)的jedis实例。
		poolConfig.setMaxIdle(pro.getInt("maxIdle", 10));
		// 当borrow(引入)一个jedis实例时，最大的等待时间，超时则抛异常
		poolConfig.setMaxWait(pro.getLong("maxWait", 10000));
		// 当borrow(引入)一个jedis实例时，进行有效监测 无效则从连接池中移除并继续申请
		poolConfig.setTestOnBorrow(true);
		// 连接空闲的最小时间 达到则可将该连接释放 -1指不移除
		poolConfig.setMinEvictableIdleTimeMillis(pro.getLong(
				"minEvictableIdleTimeMillis", 100000));
		// “空闲链接”检测线程的监测周期
		poolConfig.setTimeBetweenEvictionRunsMillis(pro.getLong(
				"timeBetweenEvictionRunsMillis", 600000));
		host = pro.get("host", DHOST);
		port = pro.getInt("port", DPORT);
		timeout = pro.getInt("timeout", DTEME_OUT);
		jedisPool = new JedisPool(poolConfig, host, port, timeout);

		instance = new ItemCache();
	}

	/** 返还资源到连接池 */
	public static void freeResource(Jedis jedis) {
		if (jedis != null)
			jedisPool.returnResource(jedis);
	}
	/** Redis基本存储对象序列  */
	private class ItemClassCache {
		public String item;
		public Set<String> classId;
		public String time;
		public Integer count;
		/** 对商品有行为的平台频率信息 */
		public Map<String, Integer> platInfo; 
		public Map<String, Integer> userInfo;
		
		/** 创建商品对象  + 查 Redis */
		public ItemClassCache(String item) {
			this.init(item);
			
			boolean bool = this.getCache();
		}
		/** 更新单个用户的信息   + 写Redis. */
		public ItemClassCache(String ite, String cid, String t, 
				String user, String plat, ItemClassCache obj) {
			this.init(item);
			
			this.classId.add(cid);
			this.time = t;
			count = 1; //
			this.platInfo.put(plat, 1);
			this.userInfo.put(user, 1);
			
			this.updateCache(obj);
		}
		private void init(String ite) throws NullPointerException {
			if (ite == null || ite.trim().length()  == 0) 
				throw new NullPointerException("item name is null.");
			final int SIZE = 4;
			item = ite;
			classId = new HashSet<String>(SIZE, 1.0f);
			time = null;
			count = 0;
			if (dumpPlat)
				platInfo = new HashMap<String,Integer>(SIZE, 1.0f);
			userInfo = new HashMap<String,Integer>(SIZE<<SIZE, 1.0f);
		}
		
		private boolean getCache() {
			// 查Redis  读出商品类目的信息
			Map<String, String> base = getItem(this.item);
			if (base.size() == 0) 
				throw new NullPointerException("Redis don't find item:"+this.item);
			
			time = base.get(hkeyOldestTime);
			count = Integer.parseInt(base.get(hkeyCount).trim());
			
			classId = getItemClass(this.item);
			if (dumpPlat)
				platInfo = getItemUsers(this.item);
			userInfo = getItemUsers(this.item);
			return true;
		}
		
		private boolean updateCache(ItemClassCache old) {
			// 合并 历史实例
			this.count += old.count;
			if (this.time.compareTo(old.time) < 0)
				this.time = old.time;
			this.classId.addAll(old.classId);
			if (dumpPlat)
				for (Map.Entry<String, Integer> ei : old.platInfo.entrySet()) {
					if (this.platInfo.containsKey(ei.getKey()))
						this.platInfo.put(ei.getKey(), this.platInfo.get(ei.getKey()) + ei.getValue());
					else 
						this.platInfo.put(ei.getKey(), ei.getValue());
				}
			for (Map.Entry<String, Integer> ei : old.userInfo.entrySet()) {
				if (this.userInfo.containsKey(ei.getKey()))
					this.userInfo.put(ei.getKey(), this.userInfo.get(ei.getKey()) + ei.getValue());
				else 
					this.userInfo.put(ei.getKey(), ei.getValue());
			}
			
			// 插入 Redis
			setHash(this.item, hkeyCount, this.count+"");
			setHash(this.item, hkeyOldestTime, this.time);
			setSet(this.item, skeyClass, this.classId);
			if (dumpPlat) 
				setItemPlat(this.item, this.platInfo);
			setItemUser(this.item, this.userInfo);
			return true;
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

		key = RedisKeyItemclassify.keyItem(key);

		jedis = jedisPool.getResource();
		res = jedis.hgetAll(key);
		return res;
	}
	/** 插入基本信息到Redis  */
	public boolean setHash(String key, String field, String value) {
		key = RedisKeyItemclassify.keyItem(key);
		
		jedis = jedisPool.getResource();
		jedis.hset(key, field, value);
		jedisPool.returnResource(jedis);
		return true;
	}
	/** 插入基本信息的 Redis. key不做变换。  */
	public boolean setRawHash(String key, String field, String value) {
		jedis = jedisPool.getResource();
		jedis.hset(key, field, value);
		jedisPool.returnResource(jedis);
		return true;
	}
	public boolean setSet(String key, String field, Set<String> value) {
		key = RedisKeyItemclassify.keyItemInfo(key, field);
		
		jedis = jedisPool.getResource();
		jedis.sadd(key, value.toArray(new String[value.size()]));
		jedisPool.returnResource(jedis);
		return true;
	}
	
	/** 取Redis的 Item分类的  HASH子域(拼接的key)属性。  */
	public Map<String, String> getItemHash(String key, String field) {
		Map<String, String> res = new HashMap<String, String>();
		key = RedisKeyItemclassify.keyItemInfo(key, field);
		if (key == null) 
			return res;
		jedis = jedisPool.getResource();
		res = jedis.hgetAll(key);
		return res;
	}
	/** 取Redis的 Item分类的  SET子域属性。  */
	public Set<String> getItemSet(String key, String field) {
		Set<String> res = new HashSet<String>();
		key = RedisKeyItemclassify.keyItemInfo(key, field);
		if (key == null) 
			return res;
		jedis = jedisPool.getResource();
		res = jedis.smembers(key);
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
		return res;
	}
	private Map<String, String> int2Str(Map<String, Integer> info) {
		Map<String, String> res = new HashMap<String, String>();
		if (info == null || info.size() == 0) 
			return res;
		
		for (Map.Entry<String, Integer> ei : info.entrySet()) 
			res.put(ei.getKey(), ei.getValue()+"");
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
	/** 返回当前商品的用户信息。  */
	public Map<String, Integer> getItemUsers(String key) {
//		key = RedisKeyItemclassify.
		Map<String, String> tMap = this.getItemHash(key, skeyUser);
		Map<String, Integer> iUser = str2Int(tMap);
		return iUser;
	}
	public boolean setItemUser(String key, Map<String, Integer> userInfo) {
		key = RedisKeyItemclassify.keyItemInfo(key, skeyUser);
		
		jedis = jedisPool.getResource();
		jedis.hmset(key, int2Str(userInfo));
		jedisPool.returnResource(jedis);
		return true;
	}
	/** 返回当前商品的平台信息 */
	public Map<String, Integer> getItemPlat(String key) {
		Map<String, String> tMap = this.getItemHash(key, skeyPlat);
		Map<String, Integer> iPlat = str2Int(tMap);
		return iPlat;
	}
	public boolean setItemPlat(String key, Map<String, Integer> platInfo) {
		key = RedisKeyItemclassify.keyItemInfo(key, skeyPlat);
		
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

	private ItemCache() {
		// jedis = jedisPool.getResource();
	}

	public static ItemCache getInstance() {
		return ItemCache.instance;
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
			// TODO Auto-generated catch block
			e.printStackTrace();
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
		final ItemCache source = ItemCache.getInstance();

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
	 * @param timeout
	 *            最大等待时长
	 */
	public String get(int timeout) throws InterruptedException {
		final String keyRedisOut = "QUEUEHTPP", sepa = "\u0001";
		final BASE64Decoder b64Decoder = new BASE64Decoder();
		final ItemCache source = ItemCache.getInstance();
		int timeStep = timeout / 100;// 最大请求次数
		timeStep = (timeStep <= 0 || 100 < timeStep) ? 100 : timeStep;

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
		final String keyRedisOut = "QUEUEHTPP", S_MR = "\t", NoResult = "NIL", keyArr = IKeywords.RawLog, sVersion = "A_M_1";
		final String[] keys = new String[] { "prod_name", "pagewords" };
		final int order = -1, wMS = 100000;
		ItemCache source = ItemCache.getInstance();
		source.getJedisPool().getResource().del(keyRedisOut);
		int NBad = 0, Nnil = 0, NJsonErr = 0, N = 0;

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
				try {
					jAtom = new JSONObject(jArr.get(i).toString());
				} catch (JSONException e) {
					++NJsonErr;
					if (debug)
						System.out.println("[ERR] process() "
								+ e.getLocalizedMessage() + "\n"
								+ jArr.get(i).toString());
					continue;
				}
				// System.out.println("[info] " + jArr.get(i) + "\n" + jAtom);
				for (int j = 0; j < keys.length; ++j) {
					if (jAtom.has(keys[j])) {
						source.push(jAtom.getString(keys[j]), order);
						try {
							id = source.get(wMS);
							if (id == null && id.indexOf(NoResult) == -1)
								++Nnil;
							else
								Cid.put(id,
										Cid.containsKey(id) ? Cid.get(id) + 1
												: 1);
						} catch (InterruptedException e) {
							e.printStackTrace();
							++NBad;
						}
						id = null;
					}
				}
				++N;
				if (debug && N % 1000 == 0)
					System.out.println("[Info] N=" + N);
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
		// args = new String [] { // unit test.
		// "D:/Data/MR-codes/data/test/act_merge.100",
		// "D:/Data/MR-codes/data/test/act_merge.redis"
		// };
		String sepa = args.length < 3 ? "\t" : args[2];
		try {
			ItemCache.getInstance().process(args[0], sepa, args[1]);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
