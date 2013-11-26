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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import com.emar.recsys.user.demo.IKeywords;
import com.emar.util.ConfigureTool;
import com.mathworks.toolbox.javabuilder.external.org.json.JSONArray;
import com.mathworks.toolbox.javabuilder.external.org.json.JSONException;

/**
 * 抽取用户聚合日志后key 对应的内容， 结果输出为每个用户的字符串。 抽取参数采用配置文件设置。 两种处理方式： 1 每次一个用户（一行）； 2
 * 每次一个文件。
 * 
 * @author zhoulm
 * 
 * @TODO unit test.
 */
public class ActionExtract {
	static private Logger log = Logger.getLogger(ActionExtract.class);
	static private final String CONF = "user.conf", SEPA = "\t";

	private BufferedReader input;
	private BufferedWriter output;
	
	protected final static Set<String> UKeys = new HashSet<String>(
			Arrays.asList(IKeywords.UserMergeAction));
	protected ConfigureTool configure_;
	protected String[] UserKey, WordsBlack, WordsWrite;
	/** 行为单位存储的数据 */
	protected List<String> data;
	protected String userID;
	protected JSONArray userAction;

	/** 单用户处理，不加载配置的文件路径信息。  */
	public ActionExtract() {
		this.init(new String[] { "", "" });
		input = null;
		output = null;
		log.info("build a single user extractor.");
	}
	/** 基于配置中文件的批量处理  */
	public ActionExtract(String[] args) throws FileNotFoundException {
		this.init(args);
		input = new BufferedReader(new FileReader(configure_.get("extract.input")));
		output = null; // 在使用前再生成。
		log.info("build a batch extractor. [input]=" 
				+ configure_.get("extract.input") + " [output]=" 
				+ configure_.get("extract.output"));
	}

	/**
	 * 通用对象初始化方法。 
	 * @throws FileNotFoundException
	 */
	private void init(String[] args) {
		if (args != null && args[0].equals("-h")) {
			System.out.println("Usage:-c myname.conf or please set user.conf.");
			return;
		}

		configure_ = new ConfigureTool();
		String pconf = (args != null && args[0].equals("-c")) ? args[1] : CONF;
		configure_.addResource(pconf);

		UserKey = configure_.getStrings("extract.user_keys", "");
		WordsBlack = configure_.getStrings("extract.black_list", "");
		WordsWrite = configure_.getStrings("extract.white_list", "");
		data = new ArrayList<String>();
		userID = null; 
		userAction = null;
		log.info("init: configure path=" + pconf + ", UserKey=" + UserKey
				 + ", WordsBlack=" + Arrays.asList(WordsBlack) 
				 + ", WordsWrite=" + Arrays.asList(WordsWrite));
	}

	/**
	 * 批处理(一个文件)过滤，处理结果都存文件。 读取全部内容，暂时不支持分批处理。
	 * @USE 不建议重写该方法。
	 * @throws IOException
	 */
	public static void batchExtract(ActionExtract extractor) throws IOException {
		if (extractor == null || extractor.input == null)
			return;

		extractor.output = new BufferedWriter(new FileWriter(
				extractor.configure_.get("extract.output")));
		String line = null;
		int counter = 0;

		for (; (line = extractor.input.readLine()) != null;) {
			extractor.data.add(line);
			counter ++;
		}
		extractor.input.close();
		log.info("batch read finish. file-size=" + counter);
		
		extractor.BatchFormat();
		for (int i = 0; i < extractor.data.size(); ++i) {
			extractor.output.write(extractor.data.get(i));
			extractor.output.newLine();
			-- counter;
		}
		extractor.output.close();
		extractor.data.clear();
		log.info("batch format finish and save. unused-counter=" + counter);
	}

	/** 单独处理一个用户行为序列. 不改变原始数据 */
	public static String singleExtract(ActionExtract extractor, String line) {
		String res = "";
		if (extractor == null || line == null || line.trim().length() == 0)
			return res;

		extractor.data.add(line);
		int index = extractor.data.size() - 1; 
		if(extractor.format(index)) {
			res = extractor.data.get(index);
			extractor.data.remove(index);
		}
		return res;
	}

	/**  自定义批处理输出. 默认调用单用户的输出  */
	public boolean BatchFormat() {
		for (int i = 0; i < this.data.size(); ++i) 
			this.format(i);
		return true;
	}
	/** 自定义单个用户数据的输出。 默认抽取字段并执行默认过滤。  */
	public boolean format(int index) {
		if (!this.Filter(index) || !this.parse(index)) { 
			this.data.set(index, null);
			return false;
		}
		StringBuffer sbuf = new StringBuffer();
		JSONObject jObj;
		String key;
		for (int i = 0; i < userAction.length(); ++i) {
			try {
				jObj = new JSONObject(userAction.getString(i));
			} catch (JSONException e) {
				log.error("userAction's " + i + "'th element bad. [MSG]: " 
						+ e.getMessage());
				continue;
			}
			sbuf.append(this.userID + "\n"); // newline.
			for (String ukey : UserKey) {
				if (jObj.has(ukey)) {
					sbuf.append(ukey + "=");
					sbuf.append(jObj.getString(ukey) + ", ");
				}
			}
		}
		this.data.set(index, sbuf.toString());
		log.info("one user format, [data]=" + sbuf.toString());
		
		return true;
	}
	/**  自定义过滤格式, 默认不过滤。  */
	public boolean Filter(int index) {
		return true;
	}
	
	/** 默认用户数据解析方法 */
	protected boolean parse(int index) {
		userID = null;
		userAction = null;
		String[] atom = this.data.get(index).split(SEPA);
		if (atom.length != 2) {
			log.warn("data not separate by TAB. [data]=" + this.data.get(index));
			return false;
		}
		
		userID = atom[0];
		try {
			userAction = new JSONArray(atom[1]);
		} catch (JSONException e) {
			log.error("bad user JSON action str. [STR]: " + atom[1] + 
					", [MSG]: " + e.getMessage());
			userAction = new JSONArray(); 
		}
		return true;
	}
	
	/** 自定义白名单过滤. 默认全通过 */
	protected boolean whiteFilter(int index) {
		return true;
	}
	
	/** 自定义黑名单过滤. 默认全不通过 */
	protected boolean blackFilter(int index) {
		return true;
	}

	// 增加测试用的开放接口。
	public BufferedReader getInput() {
		return this.input;
	}
	public List getData() {
		return this.data;
	}
	public void setData(List<String> idata) {
		this.data = idata;
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		ActionExtract act;
		try {
			act = new ActionExtract(new String[]{"", ""});
			ActionExtract.batchExtract(act);
			
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		
	}

}
