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

public class IclassifyMap2 extends Mapper<LongWritable, Text, Text, Text> {

	// 定义Map & Reduce 中通用的对象
	private static final String SEPA = LogParse.SEPA, MRSEPA = LogParse.SEPA_MR,
			PLAT = LogParse.PLAT, EMAR = LogParse.EMAR, MAGIC =LogParse.MAGIC;
	private static final String MRKC = "C_", 
			UnuseClass = "*";
	// 分类结果下标
	private static final int IdxClass = 2, IdxClassInfo = 1;  
	// 出现无效值时，指定的默认值
	private static final String VDefPCnt = "1", VDefPPrice = "0.0",
			VDefPTime = "20130101000000", VDefHost = "emar.com";

	private static Calendar c = Calendar.getInstance();
	private static SimpleDateFormat datefmt = new SimpleDateFormat(
			"yyyyMMddHHmmss");
	private LogParse logparse;
	private Text okey = new Text(), oval = new Text();
//	private MultipleOutputs<Text, Text> mos;

	private static enum Counters {
		MoUser, MoCamp,
		Err, ErrLog, ErrMO, ErrInit, ErrClassify, ErrParse, ErrPName
	};

	/**
	 * key: 
	 */
	public void map(LongWritable key, Text val, Context context) {
		// String result = ClassifyGoods("葡萄酒100ml");
		String line = val.toString();
		String path = ((FileSplit)context.getInputSplit()).getPath().toString();
		try {
//			this.logparse.base.isdug = true;
			this.logparse.parse(line, path);
		} catch (ParseException e1) {
			System.out.println("[ERROR] IclassifyMap2::map() " + this.logparse);
		}
		if(!this.logparse.base.status) {
			context.getCounter(Counters.ErrParse).increment(1);
			return;
		}
		if((this.logparse.base.prod_name == null)
				&& (this.logparse.base.prod_type_name == null)){
			context.getCounter(Counters.ErrPName).increment(1);
			System.out.println("[Err] IclassifyMap2::map() path=" + path 
					+ "\nparse-res=" + this.logparse + "\n" + this.logparse.base.isdebug  
					+ "\nindata=" + line);
			return;
		}
		this.logparse.base.prod_name.replaceAll(", ", ","); //便于后续存储到数组中 使用, 做分割符
		this.logparse.base.prod_type_name.replaceAll(", ", ",");
		String[] pclass;
		pclass = this.ClassifyGoods(this.logparse.base.prod_name).split("\t");
		if (pclass.length != 5 || pclass[IdxClass].equals(UnuseClass)) {
			pclass = this.ClassifyGoods(this.logparse.base.prod_type_name).split("\t");
		}
		if (pclass.length != 5 || pclass[IdxClass].equals(UnuseClass)) {
			context.getCounter(Counters.ErrClassify).increment(1);
			try {
				context.write(new Text(""), val);
				// 不能有效写结果， Map的输出为空
//				mos.write("badclassify", new Text(this.logparse.logpath.toString()), 
//						val, "badclassify/");
			} catch (Exception e) {
			}
			return;
		}
		
		try {
			oval.set(logparse.base.time + SEPA + pclass[IdxClass] + SEPA
					+ pclass[IdxClassInfo] + SEPA + logparse.base.prod_name + SEPA 
					+ logparse.base.prod_price + SEPA + logparse.base.domain);
			String os = this.logparse.buildUidKey();
			if(os != null) {
				okey.set(os);
				context.write(okey, oval);  // key=puid
				context.getCounter(Counters.MoUser).increment(1);
			}
			
			String fmt = MRKC + "%s" + MAGIC + logparse.logpath.plat;
			if(this.logparse.base.camp_ids == null) {
				okey.set(String.format(fmt, this.logparse.base.camp_id));
				context.write(okey, oval);
				context.getCounter(Counters.MoCamp).increment(1);
			} else {
				String[] cs = logparse.base.camp_ids.split(LogParse.SEPA_CAMPS);
				for(String s: cs) {
					if(s.length() > 1) {
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

	private Text word = new Text();

	private HashSet<String> keyWord;
	private static Vector<String> vec_config; // 保存配置文件内容
	private Path[] localFiles;

	public String configfile;
	private static Parameter parameter = new Parameter();

	public static HashMap<String, String> Diction = new HashMap<String, String>();
	public static HashMap<String, String> HashZiMu = new HashMap<String, String>();
	public HashMap<String, String> wordtag = new HashMap<String, String>();// 保存部分词语的词性，数字1，字母2
	public TreeMap<Integer, String> Code2Word = new TreeMap<Integer, String>();
	public TreeMap<String, Integer> Word2Code = new TreeMap<String, Integer>();
	public HashSet<String> StopWord = new HashSet<String>();

	public static TreeMap<Integer, String> hsCode2CateName = new TreeMap<Integer, String>(); // 类目编号到类目名称的映射
	public static TreeMap<Integer, Integer> hsCode2ParentCode = new TreeMap<Integer, Integer>(); // 存储类目编号到父类目的映射

	private static HashMap<String, CateInfo> hsCate2IdPid = new HashMap<String, CateInfo>(); // <<类目名称，<类目编号，父
	// 目录编号>>
	public static HashMap<String, CateProbInfo> hsCateProbInfo = new HashMap<String, CateProbInfo>();// 存储词语及其所属的类目和相应的概率

	private static HashMap<String, String> hsCateParent = new HashMap<String, String>(); // 类目及其父类编号
	private static HashMap<String, Integer> hsCateGrade = new HashMap<String, Integer>(); // 类目编号及其所在的列的层次

	public static HashMap<String, String> Newword2Oldword = new HashMap<String, String>();// 保存词语及其变化之前的形态
	public HashMap<String, Integer> Word2CateId = new HashMap<String, Integer>(); // 保存品牌词语及其所属的类别

	private static HashMap<String, String> hsBrand2Cate = new HashMap<String, String>(); // 存储品牌词语到类目词语的映射关系
	private static HashMap<String, String> hsCombineWord2Cate = new HashMap<String, String>(); // 组合词语到类目的映射关系
	private static HashMap<String, WordGender> hsWordGender = new HashMap<String, WordGender>();
	private static HashMap<String, String> hsGenderWord = new HashMap<String, String>();

	private String m_genderword = ""; // 标识性别的词语
	private String m_genderpos = ""; // 表示类型
	private String m_brandword = ""; // 表示品牌词语

	private static String[] DanWei = { "年", "月", "日", "天", "周", "季", "届", "集",
			"位", "排", "列", "组", "个" };
	private static String Money = "元";
	private static String[] ZhongLiangCiYu = { "g", "kg", "l", "ml", "克", "千克",
			"毫升", "升", "颗", "元" };
	private static String[] ZhongWenSuZi = { "〇", "一", "二", "三", "四", "五", "六",
			"七", "八", "九", "十" };

	private static HashSet<String> hsDanWei = new HashSet<String>();
	private static HashSet<String> hsZhongLiangDanWei = new HashSet<String>();
	private static HashSet<String> hsZhongWenSuzi = new HashSet<String>();
	private HashSet<String> hashSymbol = new HashSet<String>();// 保存标点符号

	// 这个经过测试是正确的
	/*
	 * public void setup(Context context) throws IOException{ keyWord = new
	 * HashSet<String>(); Configuration conf = context.getConfiguration();
	 * localFiles = DistributedCache.getLocalCacheFiles(conf);
	 * 
	 * for(int i=0;i<localFiles.length;i++){ String aKeyWord; BufferedReader br =
	 * new BufferedReader(new FileReader(localFiles[i].toString()));
	 * while((aKeyWord=br.readLine()) != null){ keyWord.add(aKeyWord); }
	 * br.close(); } }
	 */

	// 这样加载文件也可以
	public void setup2(Context context) throws IOException {
		keyWord = new HashSet<String>();

		Configuration conf = context.getConfiguration();
		localFiles = DistributedCache.getLocalCacheFiles(conf);

		for (int i = 0; i < localFiles.length; i++) {
			loadword(localFiles[i].toString(), keyWord);
		}
	}

	public void loadword(String filename, HashSet<String> hsWord)
			throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(filename));
		String aKeyWord;
		while ((aKeyWord = br.readLine()) != null) {
			hsWord.add(aKeyWord);
		}
		br.close();

	}

	public static void Load2HashMap(Vector<String> vec_line,
			HashMap<String, String> hashDiction) throws IOException {

		for (int i = 0; i < vec_line.size(); i++) {
			String[] result = vec_line.elementAt(i).split("\t");
			if (!hashDiction.containsKey(result[0]))
				hashDiction.put(result[0], result[1]); // save the word and the
			// pos
		}
	}

	public static void LoadWord2CateName(Vector<String> vec_line,
			TreeMap<Integer, String> hashDiction) throws IOException {

		for (int i = 1; i < vec_line.size(); i++) {

			String line = vec_line.elementAt(i);
			String[] result = line.split("\t");
			hashDiction.put(Integer.parseInt(result[0]), result[2]); // 保存编号和类目名称
		}

	}

	public static void LoadCode2CateName(Vector<String> vec_line,
			TreeMap<Integer, String> hashDiction) throws IOException {
		for (int i = 1; i < vec_line.size(); i++) {
			String[] result = vec_line.elementAt(i).split("\t");
			if (!hashDiction.containsKey(Integer.parseInt(result[0]))) {
				hashDiction.put(Integer.parseInt(result[0]), result[1]); // 保存编号和类目名称
			}

		}
	}

	public static void LoadCode2ParentCode(Vector<String> vec_line,
			TreeMap<Integer, Integer> hsCode2ParentCode) throws IOException {

		for (int i = 1; i < vec_line.size(); i++) {

			String[] result = vec_line.elementAt(i).split("\t");
			if (!hsCode2ParentCode.containsKey(Integer.parseInt(result[0]))) {
				hsCode2ParentCode.put(Integer.parseInt(result[0]),
						Integer.parseInt(result[1])); // 保存编号和类目名称
			}

		}

	}

	public void LoadCateGender(Vector<String> vec_con,
			HashMap<String, WordGender> hsWordGender) throws IOException {

		for (int i = 1; i < vec_con.size(); i++) {
			String[] array = vec_con.elementAt(i).split("\t");
			// System.out.println(array.length+"  "+vec_con.elementAt(i));
			String cateword = array[0];
			String manid = array[1];
			String womanid = array[3];
			String motherid = array[5];
			String childid = array[7];
			WordGender wordgender = new WordGender();
			if (!manid.equals("-1")) {
				wordgender.SetManId(Integer.parseInt(manid));
			}
			if (!womanid.equals("-1")) {
				wordgender.SetWomanId(Integer.parseInt(womanid));
			}
			if (!motherid.equals("-1")) {
				wordgender.SetMotherId(Integer.parseInt(motherid));
			}
			if (!childid.equals("-1")) {
				wordgender.SetChildId(Integer.parseInt(childid));
			}
			hsWordGender.put(cateword, wordgender);

		}

	}

	public void setup(Context context) throws IOException {
		// 日志解析初始化
		try {
			logparse = new LogParse();
		} catch (ParseException e) {
			context.getCounter(Counters.ErrInit).increment(1);
			e.printStackTrace();
			System.exit(e.getErrorOffset());
		}
//		mos = new MultipleOutputs(context);

		vec_config = new Vector<String>();
		Configuration conf = context.getConfiguration();
		localFiles = DistributedCache.getLocalCacheFiles(conf);

		for (int i = 0; i < localFiles.length; i++) {
			vec_config.add(localFiles[i].toString());// 加载配置文件中的所有文件名
		}

		ClassifyCommodity();

		Vector<String> vec_line = new Vector<String>();
		loadcontent(vec_config.elementAt(0), vec_line);
		LoadWord23(vec_line, "ca", hsCate2IdPid, Diction);// 加载四级类目词语
		vec_line.clear();
		loadcontent(vec_config.elementAt(1), vec_line);
		LoadWord23(vec_line, "ca", hsCate2IdPid, Diction);// 加载三级类目词语
		vec_line.clear();
		loadcontent(vec_config.elementAt(2), vec_line);
		LoadWord23(vec_line, "ca", hsCate2IdPid, Diction);// 加载二级类目词语
		vec_line.clear();
		loadcontent(vec_config.elementAt(3), vec_line);
		LoadWord23(vec_line, "ca", hsCate2IdPid, Diction);// 加载一级类目词语
		vec_line.clear();
		loadcontent(vec_config.elementAt(4), vec_line);
		LoadWord23(vec_line, "ba", hsCate2IdPid, Diction);// 加载品牌词语

		vec_line.clear();
		loadcontent(vec_config.elementAt(5), vec_line);
		Load2HashMap(vec_line, HashZiMu); // 加载全角字符映射文件

		vec_line.clear();

		loadcontent(vec_config.elementAt(6), vec_line);
		LoadWord3(vec_line, "dp", Diction);// 加载产地词语
		vec_line.clear();
		loadcontent(vec_config.elementAt(7), vec_line);
		LoadWord3(vec_line, "ys", Diction);// 加载颜色词语
		vec_line.clear();
		loadcontent(vec_config.elementAt(8), vec_line);
		LoadWord3(vec_line, "bz", Diction);// 加载包装方式

		vec_line.clear();
		loadcontent(vec_config.elementAt(9), vec_line);
		LoadWord2CateName(vec_line, hsCode2CateName);
		vec_line.clear();
		loadcontent(vec_config.elementAt(10), vec_line);
		LoadWord2CateName(vec_line, hsCode2CateName);
		vec_line.clear();
		loadcontent(vec_config.elementAt(11), vec_line);
		LoadWord2CateName(vec_line, hsCode2CateName);
		vec_line.clear();
		loadcontent(vec_config.elementAt(12), vec_line);
		LoadWord2CateName(vec_line, hsCode2CateName);

		vec_line.clear();
		loadcontent(vec_config.elementAt(13), vec_line);
		LoadWord3(vec_line, "zq", Diction);// 加载周期类词语
		vec_line.clear();
		loadcontent(vec_config.elementAt(14), vec_line);
		LoadWord23(vec_line, "ba", hsCate2IdPid, Diction);// 加载品牌直接关联类目词语
		vec_line.clear();
		loadcontent(vec_config.elementAt(15), vec_line);
		LoadWord23(vec_line, "wz", hsCate2IdPid, Diction);// 加载组合词语

		vec_line.clear();
		loadcontent(vec_config.elementAt(14), vec_line);
		Load2HashMap(vec_line, hsBrand2Cate);// 加载品牌直接关联类目词语
		vec_line.clear();
		loadcontent(vec_config.elementAt(15), vec_line);
		Load2HashMap(vec_line, hsCombineWord2Cate);// 加载组合词语

		vec_line.clear();
		loadcontent(vec_config.elementAt(16), vec_line);
		LoadWord23(vec_line, "ca", hsCate2IdPid, Diction);// 加载类目词语，除开类目词典，其余的类目词语都在这里

		vec_line.clear();
		loadcontent(vec_config.elementAt(20), vec_line);
		LoadWord23(vec_line, "ca", hsCate2IdPid, Diction);// 加载景点名

		vec_line.clear();
		loadcontent(vec_config.elementAt(17), vec_line);
		LoadCode2CateName(vec_line, hsCode2CateName); // 加载类目编号到类目名称的映射

		vec_line.clear();
		loadcontent(vec_config.elementAt(21), vec_line);
		LoadCode2ParentCode(vec_line, hsCode2ParentCode); // 加载类目编号到上级类目编号的映射

		vec_line.clear();
		loadcontent(vec_config.elementAt(22), vec_line);

		LoadCateGender(vec_line, hsWordGender);
		vec_line.clear();
		loadcontent(vec_config.elementAt(23), vec_line);
		Load2HashMap(vec_line, hsGenderWord);

		vec_line.clear();
		loadcontent(vec_config.elementAt(24), vec_line);
		LoadWord3(vec_line, "a", Diction);// 加载形容词
		vec_line.clear();
		loadcontent(vec_config.elementAt(26), vec_line);
		// System.out.println("Diction.size() ="+Diction.size());
		LoadWord3(vec_line, "n", Diction);// 加载名词
		vec_line.clear();
		loadcontent(vec_config.elementAt(27), vec_line);
		// System.out.println("Diction.size() ="+Diction.size());
		LoadWord3(vec_line, "v", Diction);// 加载动词
		// System.out.println("Diction.size() ="+Diction.size());
		Vector<String> vec_con = new Vector<String>();
		loadcontent(vec_config.elementAt(25), vec_con);
		LoadCateProb(vec_con, hsCateProbInfo); // 加载词语概率文件
		// System.out.println("hsCateProbInfo.size() ="+hsCateProbInfo.size());
		LoadCateInfo();

	}

	// 加载配置文件
	public void loadconfig(String filename, Vector<String> vec_config)
			throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(filename));
		String aKeyWord;
		while ((aKeyWord = br.readLine()) != null) {
			String[] array = aKeyWord.split("\t");
			vec_config.add(array[1]); // 将配置文件名存入向量中
		}
		br.close();

	}

	public void loadcontent(String filename, Vector<String> vec_config)
			throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(filename));
		String line;
		line = br.readLine();

		while (line != null && !line.equals("")) {

			vec_config.add(line); // 将配置文件名存入向量中
			line = br.readLine();
		}
		br.close();

	}

	// 加载类目信息
	public void LoadCateInfo() throws UnsupportedEncodingException, IOException {
		Vector<String> vec_con = new Vector<String>();
		loadcontent(vec_config.elementAt(9), vec_con);// 一级类目

		SaveCateInfo(vec_con, 1);
		vec_con.clear();
		loadcontent(vec_config.elementAt(10), vec_con);// 二级类目
		SaveCateInfo(vec_con, 2);
		vec_con.clear();
		loadcontent(vec_config.elementAt(11), vec_con);// 三级类目
		SaveCateInfo(vec_con, 3);
		vec_con.clear();
		loadcontent(vec_config.elementAt(12), vec_con);
		;// 四级类目
		SaveCateInfo(vec_con, 4);

	}

	public static void LoadWord23(Vector<String> vec_line, String pos,
			HashMap<String, CateInfo> hashDiction, HashMap<String, String> hsdiction)
			throws IOException {

		for (int i = 0; i < vec_line.size(); i++) {
			String line = vec_line.elementAt(i);
			// 将大写字母全部转换成小写字母，记下它们之间的映射关系
			String[] array = line.split("\t");
			// //System.out.println("array.size = "+array.length);
			if (array.length == 0) {
				continue;
			}
			String newstr = array[0].replaceAll(" ", ""); // 将空格去掉
			String str = parameter.ChangeBig2Small(newstr);
			// //System.out.println("str ="+str);
			if (!str.equals(array[0])) // 对原来的词语做了转换，需要记下 <新词到原词>之间的关系
			{
				if (!Newword2Oldword.containsKey(str))
					Newword2Oldword.put(str, array[0]); // 记下做了小写转换后
			}

			CateInfo ccateinfo = new CateInfo();
			if (array.length == 1) {
				ccateinfo.setpos(pos); // 只有一行词语，就只设置词性
			} else if (array.length == 3) {
				ccateinfo.setcid(Integer.parseInt(array[1]));
				ccateinfo.setparent_cid(Integer.parseInt(array[2]));
				ccateinfo.setpos(pos);
			} else if (array.length == 2) {
				ccateinfo.setcid(Integer.parseInt(array[1]));
				ccateinfo.setpos(pos);
			}
			if (!hashDiction.containsKey(str) && str.length() > 1) {
				hashDiction.put(str, ccateinfo); // save the word and the pos
				hsdiction.put(str, pos);
			}

		}

	}

	public Vector<WordPos> SegWord(String inputstr) {
		Vector<WordPos> vec_word = new Vector<WordPos>();
		String[] array0 = new String[inputstr.length()];
		for (int i = 0; i < inputstr.length(); i++) {
			String newstr = inputstr.substring(i, i + 1);
			// System.out.println("newstr ="+newstr);

			// 先判断全角，将全角转换成小写，然后判断是否是大写，将大写转换成小写
			String xiaoxiestr = QuanJiao2XiaoXie(newstr);
			if (xiaoxiestr != "") {
				array0[i] = xiaoxiestr; // 转换成小写
			} else {
				if (parameter.isBigABC(newstr)) {
					array0[i] = parameter.Big2Small(newstr);
				} else
					array0[i] = newstr;
				// 保持原来的字母不变
			}

		}
		Vector<String> vec_str = new Vector<String>();
		vec_str = parameter.splitsentence(array0);
		for (int k = 0; k < vec_str.size(); k++) {
			Vector<String> vec_str0 = new Vector<String>();
			// System.out.println("guacha "+vec_str.elementAt(k));
			Str2Array3(vec_str.elementAt(k), vec_str0);

			SegmentWord(vec_str0, vec_word);

		}

		// System.out.println();
		return vec_word;
	}

	public boolean IsDanWei(String word) {
		if (hsZhongLiangDanWei.contains(word))
			return true;
		return false;
	}

	public String QuanJiao2XiaoXie(String str) {
		String xiaoxie = "";
		if (parameter.isQuanJiao(str)) {
			if (HashZiMu.containsKey(str)) {
				xiaoxie = HashZiMu.get(str);
				return xiaoxie;

			}
			// System.out.println("这个是全角字母字符"+HashZiMu.get(str) );

		}
		return "";
	}

	// 判断输入的字符是否是“一”“二”等中文数字
	public static int isHanZiSuZi(String str) {

		if (hsZhongWenSuzi.contains(str)) // 如果是中文数字
			return 1;
		else if (hsDanWei.contains(str)) // 如果是单位数字
			return 2;
		return 3; // 不是以上两类词语中的任何一个

	}

	public static int subHanZiSuZi(String str, int inputpos) {

		int i = inputpos;

		for (; i < str.length(); i++) {
			if (i + 1 < str.length()) {
				String newstr = str.substring(i, i + 1);
				if (isHanZiSuZi(newstr) == 1)
					continue;
				else
					break;

			}
		}
		return i;
	}

	// 将左右书名号中的内容单独作为一个部分划分出来，标记上"图书或影视节目"(用“sm”)等表示词性
	public boolean isLeftShumingHao(String inputstr) {

		if (inputstr.equals("《"))
			return true; // 输入词语不是书名号，就不处理
		return false;
	}

	// 判断是否是右书名号
	public boolean isRightShumingHao(String inputstr) {

		if (inputstr.equals("》"))
			return true; // 输入词语不是书名号，就不处理
		return false;
	}

	public int FindLastShuming(String inputstr, int inputpos) {

		int i = inputpos;
		for (; i < inputstr.length(); i++) {

			String newstr = inputstr.substring(i, i + 1);
			if (isRightShumingHao(newstr))
				return i + 1;
			System.out.println("newstr =" + newstr);
		}
		return i;

	}

	// 考虑如何将价格这类词语划分出来

	public void Str2Array3(String inputstr0, Vector<String> vec_str) {

		String word = "";
		for (int i = 0; i < inputstr0.length();) {

			String subword = inputstr0.substring(i, i + 1);

			if (isLeftShumingHao(subword)) {
				int firtshuming = FindLastShuming(inputstr0, i + 1);
				if (firtshuming != i + 1) {
					String shuword = inputstr0.substring(i, firtshuming);
					vec_str.add(shuword);
					wordtag.put(shuword, "sm");
					i = firtshuming;
					continue;
				}
			}

			word = subword;
			if (parameter.isNumeric(subword)) { // 从i开始是数字
				int firstendpos = parameter.subNumpos2(inputstr0, i + 1);// 找到第一个非数字字符
				if (firstendpos == inputstr0.length()) // 最末一个是数字
				{
					String lastword = inputstr0.substring(i, firstendpos);
					vec_str.add(lastword);
					wordtag.put(lastword, "cm"); // "cm"表示是纯粹有数字组成

					break;
				}
				if (firstendpos != (i + 1) && firstendpos != -1) {

					if (firstendpos + 1 <= inputstr0.length()) {
						String firstword = inputstr0
								.substring(firstendpos, firstendpos + 1); // 取出这个非数字字符
						if (firstword.equals(".")) // 如果该数字字符是小数点号
						{
							// 从该小数点号下一个位置开始找到第一个非数字字符的位置
							int endpos = parameter.subNumpos2(inputstr0, firstendpos + 1);
							if (endpos != firstendpos + 1) {
								String newword2 = inputstr0.substring(i, endpos);
								vec_str.add(newword2);
								wordtag.put(newword2, "cm");
								i = endpos;
								// 有一个完整的小数
							} else {
								String newword2 = inputstr0.substring(i, firstendpos);
								vec_str.add(newword2);
								wordtag.put(newword2, "cm");
								i = firstendpos;
							}
						}

						else // 不是小数点符号，则表明是一个整数
						{

							String lastword = inputstr0.substring(i, firstendpos);

							if (firstendpos + 1 < inputstr0.length()) {
								String nextwordh0 = inputstr0.substring(firstendpos,
										firstendpos + 1);
								if (nextwordh0.endsWith("年")) {
									word = inputstr0.substring(i, firstendpos + 1);
									vec_str.add(word);
									wordtag.put(word, "t");
									i = firstendpos + 1;
									continue;
								} else {
									vec_str.add(lastword);
									wordtag.put(lastword, "cm");
									i = firstendpos; // 添加时间:2013-02-27
								}
							} else {

								vec_str.add(lastword);
								wordtag.put(lastword, "cm");
								i = firstendpos;
							}
							// 还可以继续判断是否是年月日
						}
					} else {
						String lastword = inputstr0.substring(i + 1);
						vec_str.add(lastword);
						wordtag.put(lastword, "cm");
						break;
					}
				} else {
					String lastword = inputstr0.substring(i, firstendpos);
					vec_str.add(lastword);
					wordtag.put(lastword, "cm");
					i = firstendpos;
				}

				continue;
			}
			if (parameter.isABC(subword)) {
				int newpos = parameter.subABCpos(inputstr0, i + 1);

				word = inputstr0.substring(i, newpos);
				vec_str.add(word); // 记下该字母的词性
				wordtag.put(word, "zm");// 字母2

				i = newpos;
				continue;
			}
			if (isHanZiSuZi(subword) == 1) // 如果是汉字数字
			{
				int pos00 = subHanZiSuZi(inputstr0, i + 1); // int pos0 =
				// subHanZiPos(inputstr0,i);
				if (pos00 + 1 < inputstr0.length()) {
					String nextwordh0 = inputstr0.substring(pos00, pos00 + 1);
					if (nextwordh0.endsWith("年")) {
						word = inputstr0.substring(i, pos00 + 1);
						vec_str.add(word);
						wordtag.put(word, "t");
						i = pos00 + 1;
						continue;
					}
				} else {

					word = inputstr0.substring(i, pos00);

					vec_str.add(word);
					wordtag.put(word, "zm");
					i = pos00;
					continue;
				}
			}
			if (parameter.isOrderWord(subword)) // 如果这个词是“第”
			{
				if (i + 1 < inputstr0.length() && i + 2 < inputstr0.length()) {
					String nextword = inputstr0.substring(i + 1, i + 2);
					if (isHanZiSuZi(nextword) == 1) // 如果是汉字数字
					{
						int pos01 = subHanZiSuZi(inputstr0, i + 1); // int pos0
						// =
						// subHanZiPos(inputstr0,i);
						if (pos01 + 1 < inputstr0.length()) {
							String nextword0 = inputstr0.substring(pos01, pos01 + 1);
							if (isHanZiSuZi(nextword0) == 2) {
								word = inputstr0.substring(i, pos01 + 1);
								vec_str.add(word);
								wordtag.put(word, "m");
								i = pos01 + 1;
								continue;
							}
						} else {
							word = inputstr0.substring(i, pos01);
							vec_str.add(word);
							wordtag.put(word, "m");
							i = pos01;
							continue;
						}

					}
				}

			}

			if (subword.equals("百") || subword.equals("千") || subword.equals("万")) // 如果这个词是“百”或者是“千”或者是“万”
			{
				if (i + 1 < inputstr0.length() && i + 2 < inputstr0.length()) {
					String nexword = inputstr0.substring(i + 1, i + 2);
					if (nexword.equals("分")) // 如果是汉字数字
					{
						if (i + 3 < inputstr0.length()) {
							String nexword0 = inputstr0.substring(i + 2, i + 3);
							if (nexword0.equals("之")) {

								if (i + 4 < inputstr0.length()) {
									String nexword00 = inputstr0.substring(i + 3, i + 4);
									if (isHanZiSuZi(nexword00) == 1) // 如果是汉字数字
									{
										int pos000 = subHanZiSuZi(inputstr0, i + 4); // int pos0 =
										// subHanZiPos(inputstr0,i);
										word = inputstr0.substring(i, pos000);
										vec_str.add(word);
										wordtag.put(word, "m");
										i = pos000;
										continue;
									}

								}
							}
						}

					}
				}

			}

			vec_str.add(word);

			word = "";
			i++;
		}

	}

	// 分词，同时返回词语的词性
	public void SegmentWord(Vector<String> veclist, Vector<WordPos> vec_word) {

		int i, j, n;
		int MaxLength = 8; // 包含的最多的词语
		int length;
		HashMap<String, String> HashWordPos = new HashMap<String, String>();
		for (i = 0; i < veclist.size();) {
			HashMap carwordmap = new HashMap();

			String word = veclist.elementAt(i);
			// 先看看这个词语是不是数字，如果是，就看下一个词语是不是人民币单位“元”，如果是，就组成价格词语
			String pos00 = "";
			if (wordtag.containsKey(word)) {
				pos00 = wordtag.get(word);

			}
			if (pos00.equals("cm")) // 该词语纯粹是数字
			{
				if (i + 1 < veclist.size()) {
					String neword = veclist.elementAt(i + 1);

					if (neword.equals(Money)) {
						String gnewword = word + veclist.elementAt(i + 1);

						WordPos cwordpos = new WordPos();
						cwordpos.setwordpos(gnewword, "jg");
						vec_word.add(cwordpos);
						i += 2;
						continue;

					}
				}
			}

			if (hashSymbol.contains(word)) {
				WordPos newwordpos0 = new WordPos();
				newwordpos0.setwordpos(word, "w"); // 保存标点符号
				i++;
				continue;
			}

			String oldword = word;
			HashWordPos.clear();

			length = 1;
			String[] wordlength = new String[MaxLength + 1];
			for (int k = 0; k < MaxLength + 1; k++) {
				wordlength[k] = "";
			}

			if (Diction.containsKey(word)) {
				wordlength[length] = word;
				carwordmap.put(word, length); // 词语及其长度
				String pos = Diction.get(word);
				HashWordPos.put(word, pos); // 记下该词语的词性

			}

			for (j = i + 1; j < veclist.size() && length < MaxLength; j++) {

				String wordpos1 = veclist.elementAt(j);
				if (wordpos1.equals(" "))
					continue;
				// System.out.println("wordpos1 ="+wordpos1+"*");
				if (wordpos1 == null)
					continue;
				word += wordpos1;
				length++;
				if (Diction.containsKey(word)) {
					wordlength[length] = word;
					carwordmap.put(word, length);
					String pos = Diction.get(word);
					HashWordPos.put(word, pos); // 记下该词语的词性
				}
			}

			for (n = MaxLength; n > 0; n--) {
				if (!wordlength[n].equals("")) {
					String newword = wordlength[n];
					String newpos = HashWordPos.get(newword);
					// System.out.println("newword = "+newword +
					// " newpos = "+newpos);
					WordPos newwordpos = new WordPos();
					newwordpos.setwordpos(newword, newpos);
					// System.out.println(newwordpos.getword()+"  "+newwordpos.getpos());
					vec_word.add(newwordpos);
					i += n;
					break;
				}
			}
			if (n == 0) { // 没有生成词典中的词语，就将原词原样留下
				String pos0 = "";
				if (wordtag.containsKey(oldword)) {
					pos0 = wordtag.get(oldword);
				}

				if (pos0.equals("m") && i + 1 < veclist.size()) // 如果是数词，要考虑与"ml"
				// "g" "kg" "克"
				// 等组成单位词语
				{

					String nextword = veclist.elementAt(i + 1);
					// 判断这个词语是否是单位词
					if (IsDanWei(nextword)) {
						oldword = oldword + nextword;
						i += 2;
					} else {
						i += 1;
					}

					WordPos cwordpos = new WordPos();
					cwordpos.setwordpos(oldword, "m");
					vec_word.add(cwordpos);

				} else if (!pos00.equals("")) {
					WordPos cwordpos = new WordPos();
					cwordpos.setwordpos(oldword, pos00);
					vec_word.add(cwordpos);
					i += 1;
				} else {
					pos0 = "wz"; // 未知的词性
					WordPos cwordpos = new WordPos();
					cwordpos.setwordpos(oldword, pos0);
					vec_word.add(cwordpos);
					i += 1;
				}
			}

		}
	}

	public void ClassifyCommodity() {
		int i;
		for (i = 0; i < DanWei.length; i++) {
			hsDanWei.add(DanWei[i]);
		}
		for (i = 0; i < ZhongWenSuZi.length; i++) {
			hsZhongWenSuzi.add(ZhongWenSuZi[i]);
		}
		for (i = 0; i < ZhongLiangCiYu.length; i++) {
			hsZhongLiangDanWei.add(ZhongLiangCiYu[i]);
		}
	}

	public static void LoadCateProb(Vector<String> vec_con,
			HashMap<String, CateProbInfo> hsCateProbInfo) // 加载词语概率文件
	{

		for (int i = 1; i < vec_con.size(); i++) {
			String[] array = vec_con.elementAt(i).split("\t");
			if (array.length < 3)
				continue;
			String cateword = array[0];

			CateProbInfo cateprobinfo = new CateProbInfo();
			for (int j = 1; j < array.length; j += 2) {
				CateProb cateprob = new CateProb();
				cateprob.SetCate(array[j]);
				cateprob.SetProb(Double.parseDouble(array[j + 1]));
				cateprobinfo.AddCateProb(cateprob);
			}
			hsCateProbInfo.put(cateword, cateprobinfo);
		}
	}

	public static void SaveCateInfo(Vector<String> vec_con, int degrade) {
		for (int i = 1; i < vec_con.size(); i++) {
			String[] array = vec_con.elementAt(i).split("\t");
			if (array.length != 4) {
				System.out.println(" the is " + vec_con.elementAt(i));
				System.exit(1);
			}
			String cateid = array[0]; // 类目编号
			String parentid = array[3];// 父类目编号
			hsCateParent.put(cateid, parentid); // 记下类目编号以及其上级类目编号
			hsCateGrade.put(cateid, degrade); // 记下类目编号以及其类目所在的层级
		}

	}

	// 给输入的句子做商品分类
	public String ClassifyGoods(String inputstr) {
		String cate = "";
		// 第一步：判断句子长短
		if (inputstr.length() < 50) // 小于50的就按照以前的分类方法
		{

			cate = ClassifySentence(inputstr);
			// return cate;
		}

		else { // 大于50的就先做处理，然后做分类
			cate = ClassifyLongGoods(inputstr);
		}
		String lastgender = "*";
		if (!m_genderword.equals("")) {
			lastgender = m_genderword;
		}
		String lastbrand = "*";
		if (!m_brandword.equals("")) {
			lastbrand = m_brandword;
		}

		return cate + "\t" + lastbrand + "\t" + lastgender;

	}

	// 获得商品中关于性别的词语
	public void GetGender(Vector<WordPos> vec_wordpos) {

		m_genderword = "";
		m_genderpos = "";
		// 判断该词语前后是否有标识性别的词语
		for (int j = vec_wordpos.size() - 1; j >= 0; j--) {

			String word = vec_wordpos.elementAt(j).getword();
			String pos = vec_wordpos.elementAt(j).getpos();

			if (hsGenderWord.containsKey(word)) // 如果该词语是某个标识性别的词语，就中断循环
			{
				m_genderword = word;
				m_genderpos = hsGenderWord.get(word); // 获得词语的词性类别
				// //从《词语，性别标记》这样的键值对里取得词语的性别标记
				break;
			}
		}
	}

	public void GetBrandWord(Vector<WordPos> vec_wordpos) {

		m_brandword = "";

		// 判断该词语前后是否有标识性别的词语
		for (int j = 0; j < vec_wordpos.size(); j++) {

			String word = vec_wordpos.elementAt(j).getword();
			String pos = vec_wordpos.elementAt(j).getpos();
			if (pos.equals("ba")) {
				m_brandword = word;
				break;
			}

		}
	}

	// 获得指定词性的词语
	public Vector<String> GetCustomWord(Vector<WordPos> vec_wordpos, String pos) {

		Vector<String> vec_word = new Vector<String>();
		for (int i = 0; i < vec_wordpos.size(); i++) {
			String word = vec_wordpos.elementAt(i).getword();
			String thepos = vec_wordpos.elementAt(i).getpos();

			if (thepos == pos) {

				vec_word.add(word); // 将这种词性的词语存入到向量中
			}
		}
		return vec_word;

	}

	public String JudgeByCombineWord2(Vector<WordPos> vec_mainword,
			Vector<WordPos> vec_restword) {
		// 判断词语所属类别步骤二：

		// 第三：如果上述词语都不存在，就看是否存在“品牌”与“包装方式”词语组合来决定类目的形式
		String cate = "";
		m_brandword = "";
		Vector<String> vec_brand = new Vector<String>(); // 品牌词语
		Vector<String> vec_baozhuang = new Vector<String>(); // 包装类词语
		Vector<String> vec_zhouqi = new Vector<String>();// 佩戴周期

		vec_brand = GetCustomWord(vec_mainword, "ba"); // 品牌词语

		vec_baozhuang = GetCustomWord(vec_restword, "bz"); // 包装方式

		vec_zhouqi = GetCustomWord(vec_restword, "zq"); // 周期类词语
		String brandword = "";
		for (int i = 0; i < vec_brand.size(); i++) {
			brandword = vec_brand.elementAt(i); // 品牌词语
			m_brandword = brandword;

			for (int j = 0; j < vec_baozhuang.size(); j++) {
				String baozhuang = vec_baozhuang.elementAt(j);

				String combineword = brandword + baozhuang; // 词语组合
				// 判断该词语组合是否存在于类目组合词语中
				if (hsCombineWord2Cate.containsKey(combineword)) {
					cate = hsCombineWord2Cate.get(combineword);
					break;
				}
			}

			for (int k = 0; k < vec_zhouqi.size(); k++) {
				String peidai = vec_baozhuang.elementAt(k);
				String combineword2 = brandword + peidai; // 词语组合
				// 判断该词语组合是否存在于类目组合词语中
				if (hsCombineWord2Cate.containsKey(combineword2)) {
					cate = hsCombineWord2Cate.get(combineword2);
					break;
				}
			}
		}
		if (!brandword.equals(""))
			return brandword + "\t" + cate; // 返回品牌词语+类目编号的组合
		return cate; // 返回类目词语，结果可能为空，后续操作需要做判断
	}

	public String ClassifySentence(String sentence) {
		// sentence = sentence.replaceAll(" ","");
		String lastsentence = "";
		Vector<String> vec_restsentence = new Vector<String>();
		// 第一步：将"()"内的句子单独划分出来，单独分类，这样句子可能有2个部分，也可能有2个以上的部分
		lastsentence = parameter.CutSentenceBySymbol(sentence, vec_restsentence);// lastsentence表示的是正文部分
		// System.out.println("now = "+ lastsentence);
		Vector<WordPos> vec_wordpos = new Vector<WordPos>();

		vec_wordpos = SegWord(lastsentence);// 对非括号部分先做分词处理
		String result = "";
		/*
		 * for(int i=0;i<vec_wordpos.size();i++) { result =vec_wordpos.elementAt(
		 * i).getword()+"/"+vec_wordpos.elementAt(i).getpos()+"\t";
		 * System.out.print(result+"   "); } System.out.println();
		 * System.out.println("分词结果输出完毕");
		 */

		String cate = GetCateByNormalSentence(vec_wordpos); // 先对非括号部分做分类判断

		Vector<WordPos> vec_restwordpos = new Vector<WordPos>();

		if (cate.equals("")) // 非括号部分无法判断出类别，就对括号部分做处理
		{
			String restsentence = "";
			for (int i = 0; i < vec_restsentence.size(); i++) {
				restsentence += vec_restsentence.elementAt(i) + "。"; // 将非括号部分句子连接起来当做一个句子处理
			}

			vec_restwordpos = SegWord(restsentence);

			cate = GetCateByNormalSentence(vec_restwordpos);
			GetGender(vec_restwordpos);
			GetBrandWord(vec_wordpos);
		}

		if (cate.equals("")) {

			cate = JudgeByCombineWord2(vec_wordpos, vec_restwordpos);
			GetGender(vec_restwordpos);
			if (m_genderword.equals("")) {
				GetGender(vec_restwordpos);
			}
		}
		if (cate.equals(""))
			return "";

		String lastcate = "";
		String[] array = cate.split("\t");
		String strid = "";
		String cateword = "";

		if (array.length == 2) {
			strid = array[1]; // 根据这个编号获得类目名称
			cateword = array[0];
			String catename = "";
			if (hsCode2CateName.containsKey(Integer.parseInt(strid))) {
				catename = hsCode2CateName.get(Integer.parseInt(strid));
			}
			lastcate = cateword + "\t" + catename + "\t" + strid;
		} else {

			lastcate = GetCateRecordOfTaoBao(cate);
			lastcate = cate + "\t" + lastcate;
		}
		return lastcate;

	}

	// hsCateProb // 存储类别以及该类别的概率和
	// 该方法用来计算一个句子里，出现多个可能分类时，计算各个类别的概率。
	public void ComputeCateSum(Vector<WordPos> vec_wordpos, int position,
			HashMap<String, Double> hsCateProb) {
		for (int i = vec_wordpos.size() - 1; i >= 0; i--) {
			if (i == position)
				continue;
			String word = vec_wordpos.elementAt(i).getword();
			String pos = vec_wordpos.elementAt(i).getpos();

			if (hsCateProbInfo.containsKey(word)) {
				CateProbInfo ccateprobinfo = new CateProbInfo();
				ccateprobinfo = hsCateProbInfo.get(word);

				for (int j = 0; j < ccateprobinfo.veccateprob.size(); j++) {
					String cateword = ccateprobinfo.veccateprob.elementAt(j).GetCate();
					double prob = ccateprobinfo.veccateprob.elementAt(j).GetProb();

					if (!hsCateProb.containsKey(cateword)) {
						hsCateProb.put(cateword, prob);
					} else {
						double sum = hsCateProb.get(cateword);
						sum += prob;
						hsCateProb.remove(cateword);
						hsCateProb.put(cateword, sum);
					}
				}
			}

		}
	}

	// position表示类目词语在该向量中的位置
	public String JudgeByCateWordProb2(int position, Vector<WordPos> vec_wordpos) {
		String strcodeid = "";
		HashMap<String, Double> hsCateProb = new HashMap<String, Double>(); // 存储类别以及该类别的概率和
		// 判断聚类所属类别步骤：
		// 第一:如果句子中有类目词语，就找最后一个类目词语，作为判断依据；
		// 第二：如果句子中不存在任何类目词语，就取唯一起类别表示作用的品牌词语；
		// 第三：如果上述词语都不存在，就看是否存在“品牌”与“包装方式”词语组合来决定类目的形式
		String word = vec_wordpos.elementAt(position).getword(); // 获得类目词语

		String pos = vec_wordpos.elementAt(position).getpos();

		CateProbInfo ccateprobinfo = new CateProbInfo();
		ccateprobinfo = hsCateProbInfo.get(word);
		if (ccateprobinfo.veccateprob.size() == 1) {
			// 先获得该词语的类目编号
			if (hsCate2IdPid.containsKey(word)) {
				CateInfo cateinfo = new CateInfo();
				cateinfo = hsCate2IdPid.get(word);
				int cid = cateinfo.getcid();
				strcodeid = Integer.toString(cid);
				return strcodeid;
			}
			// return word; //目前也没有该词在多个类别的信息，也不需要消除歧义，直接返回该类目词语
		}

		for (int j = 0; j < ccateprobinfo.veccateprob.size(); j++) {

			String cateword = ccateprobinfo.veccateprob.elementAt(j).GetCate();

			double prob = ccateprobinfo.veccateprob.elementAt(j).GetProb();

			if (!hsCateProb.containsKey(cateword)) {
				hsCateProb.put(cateword, prob);
			} else {
				double sum = hsCateProb.get(cateword);
				sum += prob;
				hsCateProb.remove(cateword);
				hsCateProb.put(cateword, sum);

			}
		}

		ComputeCateSum(vec_wordpos, position, hsCateProb);
		// 商品属于多个类目时，要判断能否将类目合并到同一个高级类目中，如果能够合并，就取更高一级的类目作为商品的类目，并将概率累加
		String maxcate = parameter.SearchMaxValue(hsCateProb);// 值最大的类就是最终要划分到的类别
		return maxcate;// 这个是类目编号
	}

	public String GetCateName(String strcode) {
		if (hsCode2CateName.containsKey(Integer.parseInt(strcode)))
			return hsCode2CateName.get(Integer.parseInt(strcode));
		return "";
	}

	// 分类，返回整型的类目编号
	public String JudgeIDByCateWord(Vector<WordPos> vec_wordpos) {
		int cateid = 0;
		// 判断聚类所属类别步骤：
		// 第一:如果句子中有类目词语，就找最后一个类目词语，作为判断依据；
		// 第二：如果句子中不存在任何类目词语，就取唯一起类别表示作用的品牌词语；
		// 第三：如果上述词语都不存在，就看是否存在“品牌”与“包装方式”词语组合来决定类目的形式
		String cate = "";
		// String genderword = ""; //标识性别的词语
		// String genderpos = ""; //性别的类型，是：男性，女性 ，孕妇，儿童？
		int i = 0;
		for (i = vec_wordpos.size() - 1; i >= 0; i--) {
			String word = vec_wordpos.elementAt(i).getword();
			String pos = vec_wordpos.elementAt(i).getpos();
			// System.out.println(word+"\t"+pos);
			if (pos.equals("ca")) {
				cate = word; // 当前类目词
				// 看看前面有没有表示价格的词语，如果有，则表示该表示类目的词语是有效的，如果没有，则表示该词无效

				break;
			} else
				continue;
		}

		if (cate.equals(""))
			return "";

		// 判断一下该词语是否需要做性别判断
		// 第一步：判断该词语是否在需要做性别判断的词语范围 //需要有一个结构存储 ：词语 男性编号 女性编号 孕妇编号 ，儿童编号
		if (!hsWordGender.containsKey(cate)) // 如果不需要做性别判断，就做消除歧义的判断
		{

			if (!hsCateProbInfo.containsKey(cate)) {
				return cate;
			}

			String catecode = JudgeByCateWordProb2(i, vec_wordpos);// 返回的是类目编号
			// 根据类目编号获得类目名称
			String catename = GetCateName(catecode);
			return catename; // 如果标识类别的词语不在性别判断的范围，就返回原来的类目词语。

		}

		if (m_genderword.equals(""))
			return cate; // 不包含标识性别的词语

		WordGender cwordgender = new WordGender();

		cwordgender = hsWordGender.get(cate);

		// 根据相应的性别，获得该词语在淘宝里的类别编号
		if (m_genderpos.equals("na")) // 如果有男性类词语，就返回该词语在男性类别的编号
		{
			cateid = cwordgender.GetManId();
		} else if (m_genderpos.equals("nv")) { // 如果有女性类词语，就返回该词语在女性类别的编号
			cateid = cwordgender.GetWomanId();
		} else if (m_genderpos.equals("yf")) { // 如果有孕妇类词语，就返回该词语在孕妇类别的编号
			cateid = cwordgender.GetMotherId();
		} else { // 如果有婴童类词语，就返回该词语在婴童类别的编号
			cateid = cwordgender.GetChildId();
		}

		return cate + "\t" + Integer.toString(cateid); // 返回类目词语+"\t"+类目编号，结果可能为空，后续操作需要做判断
	}

	public String JudgeByCombineWord(Vector<WordPos> vec_wordpos) {
		// 判断词语所属类别步骤二：

		// 第三：如果上述词语都不存在，就看是否存在“品牌”与“包装方式”词语组合来决定类目的形式
		String cate = "";

		Vector<String> vec_brand = new Vector<String>(); // 品牌词语
		Vector<String> vec_baozhuang = new Vector<String>(); // 包装类词语
		Vector<String> vec_zhouqi = new Vector<String>();// 佩戴周期

		vec_brand = GetCustomWord(vec_wordpos, "ba"); // 品牌词语
		vec_baozhuang = GetCustomWord(vec_wordpos, "bz"); // 包装方式
		vec_zhouqi = GetCustomWord(vec_wordpos, "zq"); // 周期类词语
		String brandword = "";
		String lastbrand = "";
		for (int i = 0; i < vec_brand.size(); i++) {
			brandword = vec_brand.elementAt(i); // 品牌词语

			for (int j = 0; j < vec_baozhuang.size(); j++) {
				String baozhuang = vec_baozhuang.elementAt(j);
				String combineword = brandword + baozhuang; // 词语组合

				// 判断该词语组合是否存在于类目组合词语中
				if (hsCombineWord2Cate.containsKey(combineword)) {

					lastbrand = brandword;
					cate = hsCombineWord2Cate.get(combineword);
					break;
				}
			}
			// if(vec_zhouqi.isEmpty()||vec_zhouqi.size()==0) continue;
			// System.out.println("vec_zhouqi.size() ="+vec_zhouqi.size());
			for (int k = 0; k < vec_zhouqi.size(); k++) {
				String peidai = vec_zhouqi.elementAt(k);
				String combineword2 = brandword + peidai; // 词语组合
				// 判断该词语组合是否存在于类目组合词语中
				if (hsCombineWord2Cate.containsKey(combineword2)) {
					cate = hsCombineWord2Cate.get(combineword2);
					break;
				}
			}
		}

		if (!cate.equals(""))
			return lastbrand + "\t" + cate;

		// String result =GetCateName(cate);
		return ""; // 返回类目词语，结果可能为空，后续操作需要做判断
	}

	public String GetCateByNormalSentence(Vector<WordPos> vec_wordpos) {
		GetGender(vec_wordpos);
		GetBrandWord(vec_wordpos);
		// 判断商品所属类别步骤：
		// 第一:如果句子中有类目词语，就找最后一个类目词语，作为判断依据；
		// 第二：如果句子中不存在任何类目词语，就取唯一起类别表示作用的品牌词语；
		// 第三：如果上述词语都不存在，就看是否存在“品牌”与“包装方式”词语组合来决定类目的形式

		String cate = JudgeIDByCateWord(vec_wordpos);
		if (cate.equals("")) {
			cate = JudgeByBrandWord(vec_wordpos); // 根据指示性强的品牌词语判断分类
		}
		if (cate.equals("")) {
			cate = JudgeByCombineWord(vec_wordpos); // 根据词语组合判断分类
		}
		if (cate.equals("")) {
			cate = JudgeByCombineWord(vec_wordpos); // 根据词语组合判断分类
		}
		return cate;
	}

	public String JudgeByBrandWord(Vector<WordPos> vec_wordpos) {
		// 判断词语所属类别步骤二：
		// 第二步：如果句子中不存在任何类目词语，就取唯一起类别表示作用的品牌词语；
		// 第三：如果上述词语都不存在，就看是否存在“品牌”与“包装方式”词语组合来决定类目的形式
		// System.out.println("根据品牌词语来判断");
		String cate = "";
		String word = "";
		for (int i = 0; i < vec_wordpos.size(); i++) {
			word = vec_wordpos.elementAt(i).getword();
			String pos = vec_wordpos.elementAt(i).getpos();
			// System.out.println(word+"\t"+pos);
			if (pos.equals("ba")) {
				cate = GetCateByBrand(word); // 根据品牌词语获得类目，只取第一个能获得类目的品牌词语
				m_brandword = word;
				if (!cate.equals("")) {

					break;
				}
			} else
				continue;
		}

		// if(cate.equals("")) return cate;
		// String result =GetCateName(cate);
		// System.out.println("根据品牌词语来判断结束"+result);
		if (!cate.equals(""))
			return word + "\t" + cate;
		return cate; // 返回类目词语，结果可能为空，后续操作需要做判断
	}

	public String GetCateByBrand(String brandword) {
		// System.out.println("brandword ="+brandword);
		// if(hsBrand2Cate.containsKey("可口可乐")) System.out.println("包含可口可乐");
		String cate = "";
		if (hsBrand2Cate.containsKey(brandword)) {
			cate = hsBrand2Cate.get(brandword);

		}
		return cate;

	}

	// 判断句子中是否包含“价格”类的词语
	public Boolean JudegeJiaGeWord(Vector<WordPos> vec_wordpos) {
		for (int i = 0; i < vec_wordpos.size(); i++) {
			String word = vec_wordpos.elementAt(i).getword();
			String pos = vec_wordpos.elementAt(i).getpos();

			if (pos.equals("jg")) // 表示价格的词语
			{
				return true;
			}
		}
		return false;
	}

	public String ClassifyVec2(Vector<String> vec_con) {
		Vector<Vector<WordPos>> vec2wordpos = new Vector<Vector<WordPos>>();

		String cate = "";
		for (int i = 0; i < vec_con.size(); i++) {

			Vector<WordPos> vec_wordpos = new Vector<WordPos>();

			vec_wordpos = SegWord(vec_con.elementAt(i));// 对非括号部分先做分词处理
			vec2wordpos.add(vec_wordpos);
		}
		int i_jiage = -1;
		for (int j = 0; j < vec2wordpos.size(); j++) {
			Vector<WordPos> vecwordpos = new Vector<WordPos>();
			vecwordpos = vec2wordpos.elementAt(j);
			if (JudegeJiaGeWord(vecwordpos)) {
				i_jiage = j;
				break;
			} // 第几句中包含表示“价格“的词语，找到了一个表示价格的词语，就不再往下找
		}
		int begpos = -1;
		if (i_jiage != -1)
			begpos = i_jiage;
		else
			begpos = 0;
		int k = 0;
		for (k = begpos; k < vec2wordpos.size(); k++) {
			Vector<WordPos> vecwordpos = new Vector<WordPos>();
			vecwordpos = vec2wordpos.elementAt(k);
			cate = GetCateByNormalSentence(vecwordpos); // 先对非括号部分做分类判断
			if (!cate.equals(""))
				break;
		}
		if (cate.equals("")) // 如果表示价格的词语后面无法得到一个非空的类目词语，就从表示价格的词语前面开始判断
		{
			for (k = 0; k < begpos; k++) {
				Vector<WordPos> vecwordpos = new Vector<WordPos>();
				vecwordpos = vec2wordpos.elementAt(k);
				cate = GetCateByNormalSentence(vecwordpos); // 先对非括号部分做分类判断
				if (!cate.equals(""))
					break;
			}
		}
		return cate;

	}

	// 获得类目词语在淘宝类目体系里的信息，包括：类目编号，类目名称
	public String GetCateRecordOfTaoBao(String cateword) {

		if (cateword.equals(""))
			return "";
		String newcate = "";
		String cateid = "";
		if (hsCate2IdPid.containsKey(cateword)) {
			CateInfo cateinfo = new CateInfo();
			cateinfo = hsCate2IdPid.get(cateword);
			int cid = cateinfo.getcid();

			if (hsCode2ParentCode.containsKey(cid)) {
				cid = hsCode2ParentCode.get(cid);

			}

			while (hsCode2ParentCode.containsKey(cid)) {
				cid = hsCode2ParentCode.get(cid);

			}
			if (hsCode2CateName.containsKey(cid)) {
				cateid = Integer.toString(cid);
				newcate = hsCode2CateName.get(cid);
			}
		}

		return newcate + "\t" + cateid;

	}

	// 返回类目的层级
	public int GetCateGrade(String cateid) {
		if (hsCateGrade.containsKey(cateid)) {
			return hsCateGrade.get(cateid);
		}

		return -1;

	}

	// 返回类目的父类目编号
	public String GetParentId(String cateid) {
		// if(cateid=="0") return "0";
		String parentid = "";
		if (hsCateParent.containsKey(cateid)) {
			parentid = hsCateParent.get(cateid);
			return parentid;
		} else {
			return "0";

		}
	}

	public String GetHighestParent(String cateid) {
		String inputid = cateid;
		String parentid = "";
		int level = GetCateGrade(cateid); // 获得类目的层级
		if (level == 1)
			return cateid; // 如果是第一层级的类目，就直接返回该类目本身
		for (int i = 0; i < level - 1; i++) {
			parentid = GetParentId(inputid);
			inputid = parentid;
		}

		return parentid;
	}

	// 获得类目中的最高类目层级
	public String GetHighestGrade(Vector<String> vec_cateid) {
		int num_of_meishi = 0;// 餐饮美食类目编号计数
		int num_of_shushi = 0; // 熟食类目编号计数
		int num_of_waimai = 0; // 外卖类目编号计数
		int num_of_shuichan = 0; // 水产肉类类目编号计数
		int num_of_haiwei = 0; // 海味类目编号计数
		int num_of_ganhuo = 0;
		int num_of_xiangchang = 0;

		int num_of_menpiao = 0; // 景点门票类目编号计数

		HashMap<String, Integer> hsCateTime = new HashMap<String, Integer>();
		HashSet<Integer> hsgrade = new HashSet<Integer>(); // 存储类目所在的层级编号
		for (int i = 0; i < vec_cateid.size(); i++) {

			String parentid = GetHighestParent(vec_cateid.elementAt(i)); // 获得最高层级的父类编号(不包括类别0)
			String upid = GetParentId(vec_cateid.elementAt(i)); // 上一层类目的编号
			if (hsCateTime.containsKey(parentid)) {
				int num = hsCateTime.get(parentid);
				num++;
				hsCateTime.remove(parentid);
				hsCateTime.put(parentid, num);
			} else {
				hsCateTime.put(parentid, 1);
			}
			if (upid.equals("50008613") || upid.equals("50009556")
					|| upid.equals("50016820") || upid.equals("50050573")) {
				if (hsCateTime.containsKey(upid)) {
					int num0 = hsCateTime.get(upid);
					num0++;
					hsCateTime.remove(upid);
					hsCateTime.put(upid, num0);
				} else {
					hsCateTime.put(upid, 1);
				}
			}

		}

		Iterator iter = hsCateTime.keySet().iterator();
		while (iter.hasNext()) {
			Object key = iter.next();
			Object value = hsCateTime.get(key);
			String strcate = key.toString();
			int catenum = Integer.parseInt(value.toString());
			if (strcate.equals("50050359"))// 顶级类目
			{
				num_of_shuichan = catenum;
			} else if (strcate.equals("50008075"))// 顶级类目
			{
				num_of_meishi = catenum;
			} else if (strcate.equals("50008613"))// 只要往上一级类目
			{
				num_of_shushi = catenum;
			} else if (strcate.equals("50024451")) // 顶级类目
			{
				num_of_waimai = catenum;

			} else if (strcate.equals("50009556")) // 只要往上去一级类目
			{
				num_of_haiwei = catenum;
			}

			else if (strcate.equals("50016820"))// 只要往上去一级类目
			{
				num_of_ganhuo = catenum;

			} else if (strcate.equals("50050573"))// 只要往上去一级类目
			{
				num_of_xiangchang = catenum;
			} else if (strcate.equals("50025707")) {
				num_of_menpiao = catenum;
			}

		}

		int sum = num_of_shuichan + num_of_meishi + num_of_shushi + num_of_waimai
				+ num_of_haiwei + num_of_ganhuo + num_of_xiangchang;// 混合求和，

		if (sum >= 3)
			return "餐饮美食" + "\t" + "50008075"; // ; ////返回餐饮美食类目编号
		// //同类词语或者不同类词语要达到3个或者以上才可以判断
		else if (num_of_menpiao >= 2)
			return "景点门票" + "\t" + "50025707"; // ;//;

		return "";
	}

	// 判断句子中是否有多个同类词语或相关词语出现
	public String JudgeCanYin(Vector<WordPos> vec_wordpos) {

		Vector<String> vec_catecode = new Vector<String>();
		for (int i = 0; i < vec_wordpos.size(); i++) {
			String word = vec_wordpos.elementAt(i).getword();
			String pos = vec_wordpos.elementAt(i).getpos();

			if (pos.equals("ca")) // 如果是个标识类别的词语
			{
				// 获得该词语的类目编号，将类目编号保存在向量中
				String catecode = GetCateRecordOfTaoBao(word);// 获得该词语的类目编号

				String[] array = catecode.split("\t");
				vec_catecode.add(array[1]);
			}

		}
		String result = GetHighestGrade(vec_catecode);

		return result;
		// 计算类目编号向量中，属于同一个类目的类别数量，如果这些类目在“蔬菜”“餐饮美食”类，则认为该句子讲述的是关于“餐饮美食”类的商品
	}

	public String ClassifyVec(Vector<String> vec_con) {

		String cate = "";
		for (int i = 0; i < vec_con.size(); i++) {

			Vector<WordPos> vec_wordpos = new Vector<WordPos>();

			vec_wordpos = SegWord(vec_con.elementAt(i));// 对非括号部分先做分词处理

			cate = GetCateByNormalSentence(vec_wordpos); // 先对非括号部分做分类判断
			if (!cate.equals(""))
				break;
		}

		return cate;

	}

	// 单独读加号部分做处理
	public String ClassifyJiaHao(Vector<String> vec_con) {
		String cate = "";
		String sentence = "";
		for (int i = 0; i < vec_con.size(); i++) {
			sentence += vec_con.elementAt(i); // 将加号中的内容连接起来，使之成为一句话
		}
		// 先对这句话分词，标注词性
		Vector<WordPos> vec_wordpos = new Vector<WordPos>();
		vec_wordpos = SegWord(sentence);// 对非括号部分先做分词处理

		// 判断这句话中是否有几个与蔬菜 以及餐饮相关的词语，如果有，就可以认为这些讲述的是关于“餐饮美食”类的句子
		cate = JudgeCanYin(vec_wordpos);
		return cate;
	}

	public String ClassifyLongGoods(String inputstr) {
		String sentence = parameter.ReplaceSomeStr(inputstr);
		// System.out.println("sentence ="+sentence);

		Vector<String> vec_kuohao = new Vector<String>(); // 保存普通括号中间的部分
		Vector<String> vec_fangkuohao = new Vector<String>(); // 保存方括号中间的部分
		Vector<String> vec_jiahao = new Vector<String>();
		Vector<String> vec_jiahao2 = new Vector<String>();
		String reststr = parameter.CutSenBySpecSymbol(sentence, "(", ")",
				vec_kuohao);
		// System.out.println("去掉括号标记后的句子   reststr ="+reststr);
		String laststr = parameter.CutSenBySpecSymbol(reststr, "【", "】",
				vec_fangkuohao);
		// System.out.println("去掉括号标记后的句子   laststr ="+laststr);
		// System.out.println("下面输出括号内的句子");
		String finalstr = parameter.SplitByJiaHao(laststr, vec_jiahao2);// finalstr
		// 就是要处理的正文
		// 对vec_jiahao中间包含“免费”“赠送”这样的短语去掉
		parameter.EraseZengSong(vec_jiahao2, vec_jiahao);
		// System.out.println("finalstr = "+finalstr);
		Vector<String> vec_senten = new Vector<String>();
		// 将finalstr根据标点符号切分成一个一个的小句子
		parameter.SplitSentence(finalstr, vec_senten);
		// System.out.println("vec_sentence.size() = "+vec_senten.size());
		// 对切分成的小句子做判断，判断商品所属的类别
		String cate = ClassifyVec2(vec_senten);

		String JiaHaoCate0 = ClassifyJiaHao(vec_jiahao);
		String JiaHaoCate = "";
		String JiaHaoCateWord = "";
		if (!JiaHaoCate0.equals("")) {
			String[] JiaHao = JiaHaoCate0.split("\t");
			JiaHaoCate = JiaHao[1]; // 类目编号
			JiaHaoCateWord = JiaHao[0]; // 类目词语
		}
		if (!cate.equals("") && !JiaHaoCate.equals("")) // 如果两个结果都不为空，则需要验证一下
		{
			String strcateid = GetCateRecordOfTaoBao(cate);// 获得类目词对应的类别编号
			String[] arrayid = strcateid.split("\t");
			String parentid = GetHighestParent(arrayid[1]); // 获得最高层级的父类编号(不包括类别0)
			if (!parentid.equals(JiaHaoCate)) {
				cate = JiaHaoCateWord; // 如果类目编号不一致，取加号部分的作为正确的判断

			}
		}
		if (cate.equals("") && !JiaHaoCate.equals("")) {

			cate = JiaHaoCateWord;

		}

		if (cate.equals("")) {
			cate = ClassifyVec(vec_fangkuohao);
		}
		if (cate.equals("")) {
			cate = ClassifyVec(vec_jiahao);
		}
		if (cate.equals("")) {
			cate = ClassifyVec(vec_kuohao);
		}

		String lastcate = "";
		String[] array = cate.split("\t");
		String strid = "";
		String cateword = "";

		if (array.length == 2) {
			strid = array[1]; // 根据这个编号获得类目名称
			cateword = array[0];
			String catename = "";
			if (hsCode2CateName.containsKey(Integer.parseInt(strid))) {
				catename = hsCode2CateName.get(Integer.parseInt(strid));
			}
			lastcate = cateword + "\t" + catename + "\t" + strid;
		} else {

			lastcate = GetCateRecordOfTaoBao(cate);
			lastcate = cate + "\t" + lastcate;
		}
		return lastcate;
	}

	public static void LoadWord3(Vector<String> vec_line, String pos,
			HashMap<String, String> hashDiction) throws IOException {

		for (int i = 0; i < vec_line.size(); i++) {
			String line = vec_line.elementAt(i);
			String[] array = line.split("\t");
			if (array.length == 0) {
				continue;
			}
			String newstr = array[0].replaceAll(" ", ""); // 将空格去掉
			String str = parameter.ChangeBig2Small(newstr);
			// //System.out.println("str ="+str);
			if (!str.equals(array[0])) // 对原来的词语做了转换，需要记下 <新词到原词>之间的关系
			{
				Newword2Oldword.put(str, array[0]); // 记下做了小写转换后
			}

			if (!hashDiction.containsKey(str)) {
				hashDiction.put(str, pos); // save the word and the pos
				// Word2CateId.put(str, Integer.parseInt(array[1]));
			}

		}

	}

}
