package com.emar.recsys.user.action;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.emar.recsys.user.demo.IKeywords;
import com.emar.util.ConfigureTool;

/**
 * 抽取用户聚合日志后key 对应的内容， 结果输出为每个用户的字符串。 抽取参数采用配置文件设置。 两种处理方式： 1 每次一个用户（一行）； 2
 * 每次一个文件。
 * 不容许用null, 使用空串“”表示。
 * @author zhoulm
 * @desc 继承体系中的基类，每一个可能覆盖的方法都应该有日志记录，以便后续跟进函数的调用。
 */
public class ActionExtract {
	static private Logger log = Logger.getLogger(ActionExtract.class);
	static private final String CONF = "user.conf", SEPA = "\t", NULL = "", 
			SUFFIX = ".rest";
	static protected final Set<String> UKeys = new HashSet<String>(
			Arrays.asList(IKeywords.UserMergeAction));

	private BufferedReader input;
	private BufferedWriter output, outRest; // 没有过滤成功的输出，output后增加.rest 后缀

	public ConfigureTool configure_;
	protected String[] UserKey, WordsBlack, WordsWhite;
	private Set<String> keyRetain; // 存储有效行为的KEY
	/** 行为单位存储的数据 对在其基础上更新，不容许外界对其有改写机会。 */
	protected List<String> data;
	protected BitSet flags;
	protected String userID;
	/** 用户行为集合，每个行为是一个JSON对象。  */
	protected JSONArray userAction;
	private int ActMinSize;

	/** 采用单用户（单行）处理，不加载配置的文件路径信息。 */
	public ActionExtract() {
		this.init(new String[] { "", "" });
		input = null;
		log.info("build a single user extractor.");
	}

	/** 采用基于配置文件的批量处理 */
	public ActionExtract(String[] args) throws FileNotFoundException {
		this.init(args);
		input = new BufferedReader(new FileReader(
				configure_.get("extract.input")));
		log.info("build a batch extractor. [input]="
				+ configure_.get("extract.input") + " [output]="
				+ configure_.get("extract.output"));
	}

	/** 采用批加载，单用户处理1个。 */
	public ActionExtract(List<String> mydata) {
		this.init(new String[] { "", "" });
		this.data = mydata;
		for (int i = mydata.size() - 1; 0 <= i; --i) 
			if (mydata.get(i) == null) 
				this.data.remove(i);
		log.info("build a single user extractor. size=" + this.data.size());
	}
	
	/** 仅仅支持单用户处理对象 的浅拷贝构造函数  */
	public ActionExtract(ActionExtract obj) {
		assert (obj.input == null);
		this.configure_ = obj.configure_;
		this.WordsBlack = obj.WordsBlack;
		this.WordsWhite = obj.WordsWhite;
		this.userAction = obj.userAction;
		this.userID = obj.userID;
		this.UserKey = obj.UserKey;
		this.data = obj.data;
		this.flags = obj.flags;
	}

	/**
	 * 通用对象初始化方法。
	 * 
	 * @throws FileNotFoundException
	 */
	private void init(String[] args) {
		if (args != null && args[0].equals("-h")) {
			System.out.println("Usage:-c myname.conf or please set user.conf.");
			System.exit(1);
		}

		configure_ = new ConfigureTool();
		String pconf = (args != null && args[0].equals("-c")) ? args[1] : CONF;
		configure_.addResource(pconf);

		UserKey = configure_.getStrings("extract.user_keys", "");
		keyRetain = new HashSet<String>(Arrays.asList(UserKey));
		WordsBlack = configure_.getStrings("extract.black_list", new String[]{});
		WordsWhite = configure_.getStrings("extract.white_list", new String[]{});
		ActMinSize = configure_.getInt("extract.user_action_min", 3);
		data = new ArrayList<String>();
		flags = new BitSet();
		userID = null;
		userAction = null;
		output = null; // 在使用前再生成。
		outRest = null;
		log.info("init: configure path=" + pconf + "effect min action=" 
				+ ActMinSize + ", UserKey=" + Arrays.asList(UserKey)
				+ ", WordsBlack=" + Arrays.asList(WordsBlack) + ", WordsWrite="
				+ Arrays.asList(WordsWhite));
	}

	/**
	 * 批处理(一个文件)过滤，处理结果都存文件。 读取全部内容，暂时不支持分批处理。
	 * 
	 * @USE 不建议重写该方法。
	 * @throws IOException
	 */
	public static void batchExtract(ActionExtract extractor) throws IOException {
		if (extractor == null || extractor.input == null)
			return;

		extractor.output = new BufferedWriter(new FileWriter(
				extractor.configure_.get("extract.output")));
		extractor.outRest = new BufferedWriter(new FileWriter(
				extractor.configure_.get("extract.output") + SUFFIX));
		String line = null;
		int counter = 0;

		for (; (line = extractor.input.readLine()) != null;) {
			extractor.data.add(line);
			counter++;
		}
		extractor.input.close();
		log.debug("batch read finish. file-size=" + counter);

		extractor.BatchFormat();
		for (int i = 0; i < extractor.data.size(); ++i) {
			if (extractor.flags.get(i)) {
				extractor.output.write(extractor.data.get(i));
				extractor.output.newLine();
				--counter;
			} else {
				extractor.outRest.write(extractor.data.get(i));
				extractor.outRest.newLine();
			}
		}
		extractor.output.close();
		extractor.outRest.close();
		extractor.data.clear();
		log.debug("batch format finish and save. unused-counter=" + counter);
	}

	/** 单独处理一个用户行为序列. 不改变原始数据 */
	public static String singleExtract(ActionExtract extractor, String line) {
		String res = NULL;
		if (extractor == null || line == null || line.trim().length() == 0)
			return res;

		extractor.data.add(line);
		int index = extractor.data.size() - 1;
		if (extractor.format(index)) {
			res = extractor.data.get(index);
			extractor.data.remove(index);
		}
		
		log.debug("success finished.");
		return res;
	}

	/** 自定义批处理输出. 默认调用单用户的输出 */
	public boolean BatchFormat() {
		for (int i = 0; i < this.data.size(); ++i)
			this.format(i);
		
		log.debug("success finished.");
		return true;
	}

	/** 自定义单个用户数据的输出。 默认抽取字段并执行默认过滤 @FMT: uid\nAct1, Act2...。 */
	public boolean format(int index) {
		if (!this.parse(index) 
				|| !this.Filter(index)) {
			this.data.set(index, NULL);
			this.flags.clear(index);
			return false;
		}
		
		// 过滤key 部分不变。
		JSONObject jobj;
		for (int i = 0; i < userAction.length(); ++i) {
			jobj = userAction.getJSONObject(i);
			jobj.keySet().retainAll(keyRetain);
		}
		
		this.data.set(index, formatUserActions(userID, userAction));
		this.flags.set(index);
		
		log.debug("one user default action, [data]=" + this.data.get(index));
		return true;
	}
	/** 默认的用户行为的输出格式 : 按原有格式uid\t[json] */
	public String formatUserActions(String uid, JSONArray action) {
		StringBuffer sbuf = new StringBuffer();
		sbuf.append(uid + "\t");
		sbuf.append(action);
		
		log.debug("success fininshed.");
		return sbuf.toString();
	}

	/** 默认用户数据解析方法 @FMT uid\t[ACT-arr] */
	public boolean parse(int index) {
		userID = null;
		String[] atom = this.data.get(index).split(SEPA);
		if (atom.length != 2) {
			log.info("data not separate by TAB. [data]=" + this.data.get(index));
			return false;
		}

		userID = atom[0];
		userAction = this.parseJSONArray(atom[1]);
		
		log.debug("success finished.");
		return true;
	}
	/** 默认用户行为JSON 的解析方法  */
	protected JSONArray parseJSONArray(String input) {
		JSONArray jArrs = null;
		JSONArray action = new JSONArray();
		try {  // 解析JSON数组
			jArrs = new JSONArray(input);
			for (int i = 0; i < jArrs.length(); ++i) {  // 解析每个元素为JSONObject
				try {
					action.put(new JSONObject(jArrs.getString(i)));
				} catch (JSONException e) { // 处理可能直接为object 的情况
					if (e.getMessage().endsWith("not a string.")) {
						action.put(jArrs.getJSONObject(i));
					} else {
						log.error("fail to parse json. [MSG]" + e.getMessage());
					}
				}
			}
		} catch (JSONException e1) { // 解析单个JSON对象
			try {
				JSONObject jObj = new JSONObject(input);
				action.put(jObj);
			} catch (JSONException e2) {
				log.error("bad user JSON action str. [STR]: " + input
						+ ", [MSG]: " + e2.getMessage());
				return action;
			}
		}
		return action;
	}
	
	/**  原始数据解析parse()后 的过滤, [默认]:通过  */
	public boolean Filter(int index) {
		if (this.userID == null || this.userAction.length() < ActMinSize) {
			return false;
		}
		if (!this.whiteFilter(index))
			return false;
		if (this.blackFilter(index))
			return false;
		
		log.debug("success finished.");
		return true;
	}

	/** 自定义白名单过滤. 默认全通过；建议在format()中调用 */
	protected boolean whiteFilter(int index) {
		log.debug("success finished.");
		return true;
	}

	/** 自定义黑名单过滤. 默认全不通过 */
	protected boolean blackFilter(int index) {
		log.debug("success finished.");
		return false;
	}

	// 获取某个字符串
	public String getData(int index) {
		log.debug("success finished.");
		return this.data.get(index);
	}
	// 获取全部数据量
	public int size() {
		log.debug("success finished.");
		return this.data.size();
	}
	public boolean getFlag(int index) {
		log.debug("success finished.");
		return this.flags.get(index);
	}
	public BufferedReader getInput() {
		log.debug("success finished.");
		return this.input;
	}
	public int getWhiteSize() {
		log.debug("success finished.");
		return this.WordsWhite.length;
	}
	public int getBlackSize() {
		log.debug("success finished.");
		return this.WordsBlack.length;
	}

	// public void setData(String idata, int index) {
	// this.data.set(index, idata);
	// }
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		ActionExtract act;
		try {
			act = new ActionExtract(new String[] { "", "" });
			ActionExtract.batchExtract(act);

		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
		log.debug("success finished.");
	}

}
