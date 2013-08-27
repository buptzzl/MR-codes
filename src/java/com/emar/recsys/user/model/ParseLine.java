package com.emar.recsys.user.model;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.file.tfile.RandomDistribution.Flat;
import org.json.JSONArray;
import org.json.JSONObject;

import weka.core.WeightedInstancesHandler;

import com.emar.recsys.user.demo.IKeywords;
import com.emar.recsys.user.model.ParseLine.ParseOrder.ClassType;
import com.emar.recsys.user.util.UtilStr;

/**
 * 训练数据的字段解析类 集合.
 * 
 * @func 不处理null的情况
 * @author zhoulm
 * 
 */
public final class ParseLine {

	/**
	 * 解析 classid, f1, f2 ....
	 */
	public static class ParseArrayAtom implements IAttrParser {
		// 定义 arff 的分类属性说明字符串
		String[] atom;
		String[] features;
		private static final int IdxClass = 0, IdxFBegin = 1;

		public ParseArrayAtom() {
		}

		@Override
		public String[] parseLine(String line) {
			final String sepa = ", ";
			atom = line.split(sepa);
			if (atom.length < 2) // 合法性检验
				atom = null;
			return atom;
		}

		@Override
		public String getClassify() {
			if (atom != null && atom.length != 0)
				return atom[IdxClass]; // 直接返回 原始字符串。
			return null;
		}

		@Override
		public String[] getFeatures() {
			if (atom == null)
				return null;
			String[] fset = new String[atom.length - 1];
			System.arraycopy(atom, 1, fset, 0, fset.length);
			return fset;
		}

		@Override
		public String getAttribute() {
			return "@attribute classtype {0, 1}\n";
		}

		@Override
		public void setAttribute(String s) {
		}

		@Override
		public boolean init(String[] args) {
			return true;
		}

		@Override
		public String toString() {
			if (atom == null)
				return null;
			return String.format("%s%s\t%s", getAttribute(), getClassify(),
					Arrays.asList(getFeatures()));
		}

		@Override
		public Object getWeight() {
			return null;
		}

	}

	/**
	 * 解析 订单中的部分关键字
	 */
	public static class ParseOrder implements IAttrParser {
		private JSONArray rawdata;
		private JSONObject obj;

		public static final String sepa = "\t", sepa_com = ", ", sleft = "[",
				sright = "]";
		public static final int Ifemale = 0, Imale = 1, Iuid = 2, Ifea = 3,
				Iclass = 4, Icf = 5, DConfidence = 1;

		public float Smale, Sfemale;
		public float confidence;
		public String uid;
		public List<ArrayList<String>> classes; // 类别层次信息
		public String[] features;
		public ClassType OperType;
		public boolean debug;
		/** w==0？ 是否抽取各ins的weight。 不同值指示不同的抽取方式。 */
		public int fweight;

		public static enum ClassType {
			BinarySex, // 男女性别分类
			RegressFemale, RegressMale, // 男女性别回归
			BinaryClass, // 类别性别分类
			RegressClassFemale, RegressClassMale; // 类别性别回归

			public static ClassType setType(String ctype) {
				for (ClassType s : ClassType.values())
					if (s.toString().equals(ctype))
						return s;
				return ClassType.BinaryClass; // 默认操作的类型
			}
		};

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

			String[] atom = line.split(sepa);
			if (atom.length < 5) {
				if (debug)
					System.out
							.println("[ERR] ParseOrder::parseLine() line-LEN="
									+ atom.length);
				return null;
			}
			Sfemale = Float.parseFloat(atom[Ifemale].trim());
			Smale = Float.parseFloat(atom[Imale].trim());
			uid = atom[Iuid].trim();
			confidence = this.getWeight(atom);
			// List<String> ftmp = new ArrayList<String>();
			features = UtilStr.str2arr(atom[Ifea]);
			classes.clear();
			int flag = UtilStr.str2list(atom[Iclass], sleft, sright, sepa_com,
					classes);
			if (features == null || classes == null) {
				if (debug)
					System.out
							.println("[ERR] ParseOrder::parseLine() return null."
									+ "\ninput-fea="
									+ atom[Ifea]
									+ "\nin-class=" + atom[Iclass]);
				return null;
			}
			return true;
		}

		/** 解析最后一列的值为权重。 不同解析方式须指定weight的值。 default=1. */
		private float getWeight(String[] atom) {
			final int N = Icf + 1;
			if (atom.length < N)
				return DConfidence;
			switch (fweight) {
			case 0:
				return DConfidence; // default weight.
			case 1:
				return Float.parseFloat(atom[Icf]); // use orignal value.

			default:
				break;
			}
			return DConfidence;
		}

		@Override
		public String getClassify() {
			switch (this.OperType) {
			case BinarySex:
				return Smale > Sfemale ? "0" : "1";
			case BinaryClass:
				// TODO 将类别数的分类ID展开
				return Smale > Sfemale ? "0" : "1";
			case RegressFemale:
				return Sfemale + "";
			case RegressMale:
				return -1 * Smale + "";
			case RegressClassFemale:
			case RegressClassMale:
				// TODO 按类别体系进行回归？
				return null;
			default:
				return Smale > Sfemale ? "0" : "1";
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
			case RegressClassFemale:
			case RegressClassMale:
				return "@attribute RegressClassSex numeric\n";
			case RegressFemale:
			case RegressMale:
				return "@attribute RegressSex numeric\n";
			default:
				break;
			}
			return null;
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
			if (args.length > 2)
				fweight = Integer.parseInt(args[2].trim());

			return this.OperType == null ? false : true;
		}

		@Override
		public String toString() {
			if (features == null || classes == null)
				return "ParseOrder\t" + getAttribute() + "\nfeature="
						+ features + "\nclass=" + classes + "\nuid=" + uid
						+ "\tdebug=" + debug;
			return String.format("%s%s\t%s\nclass=%s\nuser=%s\ndebug=%s",
					getAttribute(), getClassify(),
					Arrays.asList(getFeatures()), classes.toString(), uid,
					debug + "");
		}

		@Override
		public Object getWeight() {
			return String.format("%f", confidence);
		}

	}

	public static void main(String[] args) {
		ParseOrder pord = new ParseOrder();

		String in = "D:/Data/MR-codes/data/test/good_sex.txt", out = "D:/Data/MR-codes/data/test/good_sex.f.txt";

		try {
			// pord.init(ClassType.BinarySex.toString());
			pord.init(ClassType.RegressMale.toString());
			String line;
			System.out.println("[Info] attr=" + pord.getAttribute());
			DataInputStream fr = new DataInputStream(new BufferedInputStream(
					new FileInputStream(in)));
			while ((line = fr.readLine()) != null) {
				pord.parseLine(line);
				System.out.println("[Info] class=" + pord.getClassify());
				if (pord.getFeatures() == null)
					continue;
				System.out.println("[Info] feature="
						+ pord.getFeatures().length);
			}

			// / 反射测试代码
			Object ipar = (ParseLine) Class.forName(
					"com.emar.recsys.user.model.ParseLine").newInstance();
			for (Class c : ipar.getClass().getDeclaredClasses()) {
				System.out.println("sub-class:\t" + c.toString());
			}
			IAttrParser iojb = (IAttrParser) Class.forName(
					"com.emar.recsys.user.model.ParseLine$ParseArrayAtom")
					.newInstance();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
