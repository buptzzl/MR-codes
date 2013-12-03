package com.emar.recsys.user.model.ins;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;

import com.emar.recsys.user.util.UtilStr;
import com.sun.xml.internal.ws.api.streaming.XMLStreamReaderFactory.Default;

/**
 * 解析 订单数据，生成性别相关的训练集。
 * 
 * @func 不处理null的情况
 * @author zhoulm
 * 
 */
public final class ParseOrder implements IAttrParser {

	/**
	 * 解析 订单中的部分关键字
	 */
	// public static class ParseOrder implements IAttrParser {
	/** 分类时统计各类目的样例数。 */
	// private static int CntUnk; // CntClass[0] 替代
	// private static int[] CntClass = new int[100];

	public static final String sepa = "\t", sepa_com = ", ", sleft = "[",
			sright = "]";
	public static final int Ifemale = 0, Imale = 1, Iuid = 2, Ifea = 3,
			Iclass = 4, Icf = 5, DConfidence = 1, IdxSumIn = 99, IdxUnk = 0;
//	public static final BigDecimal ZERO = new BigDecimal(0);
	public static final double ZERO = 1e-8;
	private JSONArray rawdata;
	private JSONObject obj;

	public float Smale, Sfemale;
	public float confidence;
	public String uid;
	public List<ArrayList<String>> classes; // 类别层次信息
	public String[] features;
	public ClassType OperType;
	public boolean debug;
	/** 抽取各ins的weight。 不同enum值指示不同的权重抽取方式。 */
	public WeightType fweight;
	/** 特征向量 到 不同用途的arff 枚举类 */
	public static enum ClassType {
		/** 男女分值的大小 确定返回0 or 1 */
		BinarySex, 
		/** 男女性别回归， 直接使用对应的分值 */
		RegressFemale, RegressMale, 
		BinaryClass, // 类别性别分类, 分值与BinarySex 相同
		RegressClassFemale, RegressClassMale, // 类别性别回归
		/** 男性分值优先， 不为0时返回1 */
		BinaryMale,
		BinaryFemale,
		/** 按M|F|U 三个类别打标签 */
		SexUnk;

		public static ClassType setType(String ctype) {
			for (ClassType s : ClassType.values())
				if (s.toString().equals(ctype))
					return s;
			return ClassType.BinaryClass; // 默认操作的类型
		}
	};
	/** 权重的计算样例 枚举类 */
	public static enum WeightType {
		/** 直接使用1 */
		One,
		Default, 
		/** 直接使用 男女 分值 */
		Female, Male,  
		/** 直接使用原始日志中的频率(置信度 ) */
		FCount,
		/** conf*(female_score - male_score) */
		Conf_Diff,
		/** 使用男女的频率 */
		CountFemale, CountMale;
		
		public static WeightType setType(String ctype) {
			for(WeightType w : WeightType.values())
				if (w.toString().equals(ctype))
					return w;
			return WeightType.One;
		}
	}

	/** 默认构造方法。 注意： 在使用前需要调用 init() 指定 OperType 的值 */
	public ParseOrder() {
		classes = new ArrayList<ArrayList<String>>();
		OperType = null;
		// rawdata = new JSONArray();
		// obj = new JSONObject();
	}

	public String getType() {
		return this.OperType.toString();
	}

	@Override
	public Object parseLine(String line) {
		// 解析 IntPair 对象。
		this.countClass(IdxSumIn);

		String[] atom = line.split(sepa);
		if (atom.length < 5) {
			if (debug)
				System.out.println("[ERR] ParseOrder::parseLine() line size<5");
			return null;
		}
		try {
			Sfemale = Float.parseFloat(atom[Ifemale].trim());
		} catch (NumberFormatException e) {
		}
//		if (Double.isNaN(Sfemale))
//			Sfemale = 0;
		try {
			Smale = Float.parseFloat(atom[Imale].trim());
		} catch (Exception e) {
		}
//		if(Double.isNaN(Smale))
//			Smale = 0;
		uid = atom[Iuid].trim();
		
		// List<String> ftmp = new ArrayList<String>();
		features = UtilStr.str2arr(atom[Ifea]);
		classes.clear();
		int flag = UtilStr.str2list(atom[Iclass], sleft, sright, sepa_com,
				classes);
		this.countClass(this.getClassIndex());
		try {
			confidence = Integer.parseInt(atom[Icf].trim());
		} catch (Exception e) {
		}
		if (features == null || classes == null) {
			if (debug)
				System.out.println("[ERR] ParseOrder::parseLine() return null."
						+ "\ninput-fea=" + atom[Ifea] + "\nin-class="
						+ atom[Iclass]);
			return null;
		}
		return true;
	}

	/** 
	 * 解析最后一列的值为权重。 不同解析方式须指定weight的值。 default=1. 
	 * @see 必须在解析了 对应字段的值 如： Sfemale, Smale之后调用。
	 */
	public Object getWeight() {
		float weight = confidence;
		switch (fweight) {
		case Default:
			weight = DConfidence;
			break;
		case Female:
			weight = Double.isNaN(Sfemale) ? 1: Sfemale;
			break;
		case Male:
			weight = Double.isNaN(Smale) ? 1: Smale;
			break;
		case FCount:
			weight = confidence;
			break;
		case Conf_Diff:
			weight = confidence * (Sfemale - Smale);
			break;
		case CountFemale:
			weight = confidence * Sfemale;
			break;
		case CountMale:
			weight = confidence * Smale;
			break;
		case One:
			weight = 1;
			break;
		default:
			break;
		}
		if (weight < 0)
			weight *= -1;
		if (weight < 1)
			weight = 1; 
		
		return weight;
	}

	@Override
	public String getClassify() {
		switch (this.OperType) {
		case BinarySex:
			return Smale < Sfemale ? "1" : "0"; // 概率相等时默认为男性(0)更可信。
		case BinaryClass:
			// TODO 将类别数的分类ID展开
			return Smale < Sfemale ? "1" : "0";
		case RegressFemale:
			return Sfemale + "";
		case RegressMale:
			return -1 * Smale + "";
		case RegressClassFemale:
		case RegressClassMale:
			// TODO 按类别体系进行回归？
			return null;
		case BinaryFemale:
			return (ZERO < Sfemale || Sfemale < -(ZERO)) ? "1" : "0"; 
		case BinaryMale:
			return (Smale > ZERO || Smale < -(ZERO)) ? "1" : "0";
		case SexUnk:
			if (Double.isNaN(Smale) || (-(ZERO) < Smale && Smale < ZERO)) {
				if (Double.isNaN(Sfemale) || (-(ZERO) < Sfemale && Sfemale < ZERO))
					return "2";  // UNK
				else 
					return "1";  // female
			} else {
				if(Double.isNaN(Sfemale) || Sfemale <= Smale)
					return "0"; // male
				else 
					return "1";
			}
		default:
			return Smale < Sfemale ? "1" : "0";
		}
	}

	@Override
	public String[] getFeatures() {
		return this.features;
	}

	/** 获取 第k层的分类 结果 */
	public String[] getClassLevel(int level) {
		if (level < 0 || this.classes.size() < (level + 1))
			return null;
		return this.classes.get(level).toArray(
				new String[this.classes.get(level).size()]);
	}

	@Override
	public String getAttribute() {
		switch (this.OperType) {
		case BinarySex:
		case BinaryClass:
			return "@attribute classtype {0, 1}\n";
		case SexUnk:
			return "@attribute tri_class { 0, 1, 2}\n";
		case RegressClassFemale:
		case RegressClassMale:
			return "@attribute RegressClassSex numeric\n";
		case RegressFemale:
		case RegressMale:
			return "@attribute RegressSex numeric\n";
		default:
			break;
		}
		return "@attribute classtype {0, 1}\n";
	}

	@Override
	public void setAttribute(String s) {
	}

	/** 参数： class字段的解析类型， 是否为debug，实例权重计算方式， */
	@Override
	public boolean init(String... args) {
		if (args == null || args.length < 1)
			return false;
		this.OperType = ClassType.setType(args[0]);
		if (args.length > 1)
			debug = args[1].equals("debug"); // 设置调试信息
		fweight = WeightType.One;
		if (args.length > 2)
			fweight = WeightType.setType(args[2].trim());
		
		if (debug)
			System.out.println("[Info] Parseorder::init() " + this.toString());

		return this.OperType == null ? false : true;
	}

	@Override
	public String toString() {
		if (features == null || classes == null)
			return "ParseOrder[" + getAttribute() + "\nfeature=" + features
					+ "\nclass=" + classes + "\nuid=" + uid + "\tdebug="
					+ debug + "\tconfidence=" + confidence + "\tOperType=" + OperType
					+ "\tweight_type=" + fweight + "]";
		return String.format(
				"ParseOrder:[%s%s\t%s\nclass=%s\nuser=%s\ndebug=%s\tconfidence=%f\tOperType=%s\tweight_type=%s]",
				getAttribute(), getClassify(), Arrays.asList(getFeatures()),
				classes.toString(), uid, debug + "", confidence, OperType.toString(), fweight.toString());
	}
	
	@Override
	public int getClassIndex() {
		int idx = 0;
		switch (this.OperType) {
		case BinarySex:
		case BinaryClass:
		case BinaryFemale:
		case BinaryMale:
		case SexUnk:
			String c = this.getClassify();
			if (c != null)
				idx = c.charAt(0) - '0' + (1 + IdxUnk);
			break;
		case RegressFemale:
		case RegressClassFemale:
			idx = this.segment(this.Sfemale);
			break;
		case RegressMale:
		case RegressClassMale:
			idx = this.segment(this.Smale);
			break;
		default:
			break;
		}

		return idx;
	}

	/** 对【0-1】的分值按边界分层 */
	private int segment(float score) {
		// 按分值的升序继续 比较
		if (score <= 0.1)
			return 1;
		else if (score <= 0.2)
			return 2;
		else if (score <= 0.3)
			return 3;
		else if (score <= 0.4)
			return 4;
		else if (score <= 0.5)
			return 5;
		else if (score <= 0.6)
			return 6;
		else if (score <= 0.7)
			return 7;
		else if (score <= 0.8)
			return 8;
		else if (score <= 0.9)
			return 9;
		else
			return 10;
	}

	@Override
	public void countClass(int idx) {
		IAttrParser.CntClass[idx] += 1;
	}

	@Override
	public String getStaticInfo() {
		String sinfo = String.format(
				"total input data=%d, unknows ins size=%d", CntClass[IdxSumIn],
				CntClass[IdxUnk]);
		List<Integer> cinfo = new ArrayList<Integer>(10);
		switch (OperType) {
		case BinaryClass:
		case BinarySex:
		case BinaryFemale:
		case BinaryMale:
			sinfo = String.format("%s, male size=%d, female size=%d", sinfo,
					CntClass[1], CntClass[2]);
			break;
		case SexUnk:
			sinfo = String.format("%s, male size=%d, female size=%d, unk size=%d", 
					sinfo, CntClass[1], CntClass[2], CntClass[3]);
		case RegressFemale:
		case RegressMale:
			for (int i = 1; i < 11; ++i)
				cinfo.add(CntClass[i]);
			sinfo = String.format(
					"%s, \nscore range [0, 1] in 10-ASC-segment:\n%s", sinfo,
					cinfo);
		default:
			break;
		}

		return sinfo;
	}

	// }

	public static void main(String[] args) {
		ParseOrder pord = new ParseOrder();

		String in = "D:/Data/MR-codes/data/test/good_sex.txt";
		try {
			 pord.init(ClassType.SexUnk.toString(), "",
//			pord.init(ClassType.RegressMale.toString(), "debug", 
					WeightType.Default.toString());
			
			String line;
			System.out.println("[Info] attr=" + pord.getAttribute());
			DataInputStream fr = new DataInputStream(new BufferedInputStream(
					new FileInputStream(in)));
			while ((line = fr.readLine()) != null) {
				pord.parseLine(line);
				System.out.print("[Info] class=" + pord.getClassify());
				if (pord.getFeatures() == null)
					continue;
				System.out.println("[Info] feature-size="
						+ pord.getFeatures().length);
			}
			fr.close();
			System.out.println("staticinfo: " + pord.getStaticInfo());

			IAttrParser iojb = (IAttrParser) Class.forName(
					"com.emar.recsys.user.model.ins.ParseOrder").newInstance();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
