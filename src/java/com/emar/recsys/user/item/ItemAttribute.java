package com.emar.recsys.user.item;

import java.io.IOException;
import java.io.StringReader;
import java.text.ParseException;
import java.util.*;
import java.util.regex.Pattern;

import com.chenlb.mmseg4j.Word;
import com.emar.recsys.user.feature.FeatureType;
import com.emar.recsys.user.util.IUserException;
import com.emar.recsys.user.util.UtilStr;
import com.emar.recsys.user.util.WordSegment;
import com.webssky.jcseg.core.IWord;
import com.webssky.jcseg.core.JcsegException;

/**
 * TODO 按TB 商品名挖掘流程， 实现商品名切分 item属性： 属性有对应的优先级别；每识别一个属性，对应的内容即被删除 Order:
 * ItemShop, ItemSize, ItemProducer, ItemCategory, ItemDescribe. 注：
 * 各属性抽取时，默认采用先序优先。
 * 
 * @author zhoulm
 * 
 */
public class ItemAttribute {
	private static boolean isWS;
	private static WordSegment ws;
	private static final Pattern cpatt = Pattern
			.compile("\\(|\\)|（|）|\\[|\\]|【|】|\\{|\\}| |,|\\?|;|\"|\\t|，|。|；|？|“|”|、|…|—|！|￥"),
			unwordPatt = Pattern.compile("\\&[A-Za-z]+;");
	private static Set Units, UnitEs, Nums;
	private static final String FPrefix = String
			.format("%s%s%s", FeatureType.AD, FeatureType.SEG,
					FeatureType.TITLE, FeatureType.SEG);
	private static final String EMP = " ", POS_ = "__",
			POS_EMP = POS_ + "null", POS_UNIT = POS_ + "unit", POS_NUM = POS_
					+ "num", POS_SH = POS_ + "shop", POS_PRO = POS_ + "prod";
	public static final int KGram = 3; // 单字生成ngram的最大值
	public static boolean debug;
	private static IItemFilter myfiler; // 分词过滤器
	/** 标示量： 是否已完成分词； 是否执行单子词的kgram拼接 */
	private boolean fsegment, kgram;

	final Character[] CN_NUMERIC = { '一', '二', '三', '四', '五', '六', '七', '八',
			'九', '壹', '贰', '叁', '肆', '伍', '陆', '柒', '捌', '玖', '○', 'Ｏ', '零',
			'十', '百', '千', '拾', '佰', '仟', '万', '亿',
	// '兆', '京', '顺',
	};
	static {
		debug = false;
		Nums = new HashSet<Character>(Arrays.asList('一', '二', '三', '四', '五',
				'六', '七', '八', '九', '壹', '贰', '叁', '肆', '伍', '陆', '柒', '捌',
				'玖', '○', 'Ｏ', '零', '十', '百', '千', '拾', '佰', '仟', '万', '亿'));
		// 瓶装 等词已经加入词典
		Units = new HashSet<String>(Arrays.asList("元", "$", "￥", "克", "千克",
				"斤", "公斤", "盎司", "磅", // 重量
				"毫安", "安", //
				"升", "毫升", "加伦", "品脱", // 容积
				"米", "分米", "厘米", "毫米", "寸", "英寸", "英尺", "英里", "千米", // 长度
				"平方米", "平方厘米", // 面积
				"小样", "中样", "大样", // 化妆品独有
				"大码", "中码", "小码", "条", "件", "套", "双", // 衣服
				"包", "盒", "箱", "袋", "片", "枚", "次"));
		UnitEs = new HashSet<String>(Arrays.asList("g", "kg", "ml", "l"));

		myfiler = new ItemFilter();
		try {
			ws = WordSegment.getInstance();
			isWS = true;
		} catch (Exception e) {
			e.printStackTrace();
			isWS = false;
		}
	}

	// private final List<String> wseg = new ArrayList<String>(10);
	private String item;
	private List<String> attributes;
	private List<String> sword; // 分词结果
	private List<String> spos; // 所有词的词性类别
	private List<String> size; // 是否有容量字符串出现
	/** cate: 类目{keyword, id}，prod: 产品{name, info} */
	private String[] cate, prod; // name,type pair.
	/** 按标点等切分开的语义边界点。 */
	private List<Integer> segmentWordIndex;
	/** 基本语义片段 */
	private String[] segments;

	public ItemAttribute(String item) throws ParseException {
		if (!isWS || item == null) {
			throw new ParseException("initial ItemAttribute with NULL",
					IUserException.ErrADItemNull);
		}
		this.item = myfiler.prefilter(item);

		fsegment = false;
		kgram = false;
		attributes = new ArrayList<String>();
		sword = new ArrayList<String>(); // 分词结果
		spos = new ArrayList<String>(); // 所有词的词性类别,V_desc1_desc2...
		size = new ArrayList<String>(); // 是否有容量字符串出现

		cate = new String[] { "", "" };
		prod = new String[] { "", "" };
		segmentWordIndex = new ArrayList<Integer>();
	}

	/** 预处理： 去除无效字符 小写化。 后处理： null */
	public static class ItemFilter implements IItemFilter {
		@Override
		public String prefilter(String line) {
			String[] items = ItemAttribute.unwordPatt.split(line);
			StringBuffer sbuf = new StringBuffer();
			for (int i = 0; i < items.length; ++i)
				sbuf.append(items[i] + " ");// 用空格替代无效字符

			return sbuf.toString().toLowerCase(); // 小写
		}

		@Override
		public List<String> postFilter(List<String> atom) {
			return atom;
		}

		@Override
		public String sfilter(String ai) {
			return ai;
		}

	}

	public List<String> getWord() {
		if (!fsegment)
			// fsegment = this.ItemSegment();
			this.FeatureExtract();
		return sword;
	}

	public List<String> getPos() {
		if (!fsegment)
			// fsegment = this.ItemSegment();
			this.FeatureExtract();
		return spos;
	}

	public List<String> getSize() {
		if (!fsegment)
			// fsegment = this.ItemSegment();
			this.FeatureExtract();
		return size;
	}

	public String[] getCate() {
		if (!fsegment)
			// fsegment = this.ItemSegment();
			this.FeatureExtract();
		return cate;
	}

	/** 返回产品名 与POS|索引； 失败时返回空串"". */
	public String[] getProd() {
		if (!fsegment)
			// fsegment = this.ItemSegment();
			this.FeatureExtract();
		String si;
		int icate = -1;

		if (this.prod[0].length() == 0) {
			for (int i = 0; i < this.spos.size(); ++i) {
				si = this.spos.get(i);
				if (si != null && si.equals(POS_PRO)) {
					prod[0] = this.sword.get(i);
					prod[1] = i + ""; // 记录索引信息
					break;
				}
				if (icate == -1 && si != null && si.length() != 0
						&& !si.equals("null"))
					icate = i;

			}
			if (prod[0].length() == 0 && icate != -1) {
				prod[0] = this.sword.get(icate);
				prod[1] = icate + "";
			}
		}
		return prod;
	}

	/** 抽取全部属性。 */
	public List<String> getAttribute() {
		if (!fsegment)
			// fsegment = this.ItemSegment();
			this.FeatureExtract();

		this.getWord();
		this.getPos();
		this.getSize();
		this.getCate();
		this.getProd();
		// raw info.
		this.attributes.addAll(this.sword);
		this.attributes.addAll(this.spos);
		this.attributes.addAll(this.size);
		return this.attributes;
	}

	private boolean FeatureExtract() {
		if (this.item == null)
			return false;

		List<String> info = new ArrayList<String>(); // 临时存储分词结果，调试用
		String w, si;
		String[] pos = null;
		IWord word = null;

		int pbeg = 0, pend = sword.size(), plast = -1;
		segments = cpatt.split(this.item); // 基于正则做粗切分
		for (int i = 0; i < segments.length; ++i) {
			si = segments[i];
			try {
				ws.Jcseg.reset(new StringReader(si));
				while ((word = ws.Jcseg.next()) != null) {
					pos = word.getPartSpeech();
					w = word.getValue();
					sword.add(w.trim());
					spos.add((pos == null || pos[0].equals("null")) ? POS_EMP
							: pos[0].toLowerCase()); // 每个词都有对应的词性
					if (debug)
						info.add(w + "/" + spos.get(spos.size() - 1));

				}
				pend = sword.size();
				// ItemShop, ItemSize, ItemProducer, ItemCategory, ItemDescribe.
				this.ItemShop(pbeg, pend);
				this.ItemSize(pbeg, pend);
				this.ItemProducer(pbeg, pend);
				segmentWordIndex.add(pbeg);
				segmentWordIndex.add(pend);
				plast = pbeg;
				pbeg = pend;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		fsegment = true;

		if (debug)
			System.out.println("[Debug] ItemSegment() segmented words:\ninput="
					+ this.item + "\nsegment=" + info);

		return true;
	}

	/** @deprecated 商品标题粗切分。 分隔符为： 空格、成对的 括号、非数字与英文 两个字、 三个字中末尾为 装 等不再分词 */
	private boolean ItemSegment() {
		if (this.item == null)
			return false;

		List<String> item = new ArrayList<String>(); // 单子词临时空间
		List<String> info = new ArrayList<String>(); // 临时存储分词结果，调试用
		String w, si;
		String[] pos = null;
		IWord word = null;

		int pbeg = 0, pend = sword.size(), plast = -1;
		segments = cpatt.split(this.item); // 基于正则做粗切分
		for (int i = 0; i < segments.length; ++i) {
			si = segments[i];
			// ws.Jcseg.reset(new StringReader(this.item));
			try {
				ws.Jcseg.reset(new StringReader(si));
				while ((word = ws.Jcseg.next()) != null) {
					pos = word.getPartSpeech();
					w = word.getValue();
					if (debug)
						info.add(w + "/" + (pos == null ? "null" : pos[0]));

					if (pos != null && !Units.contains(w)) { // 非计量单元
						if (item.size() != 0) {
							this.SWordCombine(item);
							item.clear();
						}
						sword.add(w.trim()); // add word & POS.
						spos.add(pos[0].trim());
					} else {
						int[] wcnt = UtilStr.strCharCnt(w);
						if (w.length() == 1 || Units.contains(w)
								|| (wcnt[1] + wcnt[3]) == w.length()
								|| (wcnt[2] + wcnt[3]) == w.length()) {
							// 单汉字 连续的字符or数字串看做单个字
							item.add(w);
							continue;
						}
						if (item.size() != 0) {
							this.SWordCombine(item);
							item.clear();
						}
						if ((wcnt[1] + wcnt[2]) == w.length()
								&& UtilStr.isDigital(w.substring(0, wcnt[2]))) // 数量模式
							size.add(w);
						sword.add(w.trim());
						spos.add(POS_EMP); // 每个词都有对应的词性
					}
				}
				this.SWordCombine(item);
				item.clear();

				pend = sword.size();
				this.ItemShop(pbeg, pend);
				// this.ItemSize(pbeg, pend);
				this.ItemProducer(pbeg, pend);
				segmentWordIndex.add(pbeg);
				segmentWordIndex.add(pend);
				plast = pbeg;
				pbeg = pend + 1;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		if (debug)
			System.out.println("[Debug] ItemSegment() segmented words:\ninput="
					+ this.item + "\nsegment=" + info);

		return true;
	}

	/** @deprecated 合并单子词 并生成数量单元。 处理k-gram, update sword & spos. */
	private void SWordCombine(List<String> item) {
		String s;
		final int NUnitTrueMn = 1, NUnitTrueEMx = 3; // 中英文单位词的最大可能长度
		int idx = 0;
		boolean isD = false, isU = false, isE = false; // 数字 单位 英文
		for (int i = 0; i < item.size(); ++i) {
			// 识别数量模式
			s = item.get(i);
			if ((Units.contains(s) && (s.length() > NUnitTrueMn || isD))
					|| (s.length() == 1 && isD)) {
				if (i != 0) {
					ItemSize(item.get(i - 1), s);
				} else {
					sword.add(s);
					spos.add(POS_EMP);
				}
				isD = false;
				isU = true;
				isE = false;
				SWordNGram(item, idx, i - 1);
				idx = i + 1;
				continue;
			}
			int[] wcnt = UtilStr.strCharCnt(s);
			if ((wcnt[2] + wcnt[3]) == s.length()) {
				if (!isD) { // 非数字
					SWordNGram(item, idx, i);
					idx = i;
				}
				isE = false;
				isU = false;
				isD = true;
				continue;
			}
			if ((wcnt[1] + wcnt[2] + wcnt[3]) == s.length() && wcnt[2] != 0
					&& wcnt[1] < NUnitTrueEMx && 0 < wcnt[1]
					&& UtilStr.isChars(s.substring(s.length() - wcnt[1]))) {
				// 英文单位的大小
				SWordNGram(item, idx, i);
				ItemSize(s.trim()); //
				isD = false;
				isU = true;
				isE = false;
				idx = i + 1;
				continue;
			}
			if ((wcnt[1] + wcnt[2] != 0) && !(isE || isD)) {
				SWordNGram(item, idx, i); // 中文与非中文 处分割开
				idx = i;
			}
			if (wcnt[0] == 0 && (wcnt[3] > 1 || (wcnt[1] + wcnt[2]) < 3)) {
				// 长度小于3且非度量单元的 en+dig 或符号数超过1的原子 抛弃。
				item.set(i, "");
			}
		}
		SWordNGram(item, idx, item.size());
	}

	/** @deprecated 生成Kgram, 修正某些特殊词无法识别的问题。 */
	private void SWordNGram(List<String> item, int beg, int end) {
		if (!kgram)
			return;
		if (beg < 0 || end <= beg || item.size() < end)
			return;
		StringBuffer sbuf = new StringBuffer();
		if ((end - beg) == 1) {
			int[] wcnt = UtilStr.strCharCnt(item.get(0));
			if (wcnt[0] == 0) // 无汉字时直接抛弃
				return;
		}
		if ((end - beg) < 3) { // 2个以内的词不做k-gram
			for (int i = 0; i < (end - beg); ++i) {
				sbuf.append(item.get(i + beg));
			}
			sword.add(sbuf.toString().trim());
			spos.add(POS_EMP);
		} else {
			List<List> grams = UtilStr.Xgram(item.subList(beg, end), KGram);
			for (int i = 0; i < grams.size() - 1; ++i) { // no 1-gram.
				sword.addAll(grams.get(i));
				for (int j = 0; j < grams.size(); ++j)
					spos.add(POS_UNIT);
			}
		}
		return;
	}

	@Deprecated
	private boolean ItemSize(String... words) {
		if (words == null || words.length == 0)
			return false;

		sword.add(words[words.length - 1]); // 单独增加单位
		spos.add(POS_UNIT);
		if (words.length == 1) { // 可能为 汉字单位 or 数字+英文
			words[0] = words[0];
			int i = words[0].length();
			while (i != 0
					&& ('a' <= words[0].charAt(i) && words[0].charAt(i) < 'z'))
				--i;
			words[0] = words[0].substring(0, i); // 删除 单位部分
		}
		int digits = words[0].indexOf('.');
		words[0] = words[0].trim();
		if (words[0].length() != 0) {
			int k = words[0].charAt(0) - '0';
			if (0 < k && k <= 9) {
				sword.add(k
						* (int) Math.pow(10, digits == -1 ? words[0].length()
								: digits) + "");
				spos.add(POS_UNIT);
			}
		}
		return false;
	}

	/** 抽取 数量大小 特征. 要求单位在最后。 结果写入 words列表 */
	private boolean ItemSize(int pbeg, int pend) {
		boolean res = false;
		if (pbeg < 0 || pend <= pbeg || this.sword.size() < pend)
			return res;
		
		// TODO 模式： num+unit; 价格+num; +num; 原价+num; 100g中英混合； 不处理"盒装"等。
		final Set<String> Prices = new HashSet<String>(Arrays.asList("原价",
				"特价", "价值", "价格"));
		final String[] kSuffix = new String[] { "装" };
		String[] words = this.sword.subList(pbeg, pend).toArray(
				new String[pend - pbeg]);

		String s = null, tmp = null;
		boolean pfind = false;
		for (int i = pbeg; i < pend; ++i) {
			s = this.sword.get(i);
			pfind = false;
			int[] wcnt = UtilStr.strCharCnt(s);
			// 将单位做数字看待
			for (int j = 0; j < s.length(); ++j)
				wcnt[2] += Nums.contains(s.charAt(j)) ? 1 : 0;
			for (String si : (Set<String>) UnitEs)
				if (s.lastIndexOf(si) + si.length() == s.length()) {
					wcnt[2] += si.length();
					break;
				}

			if (0 < wcnt[2] && (s.length() - wcnt[2] - wcnt[1]) <= 2) {
				if (i < (pend - 1) && Units.contains(this.sword.get(i + 1))) {
					s += this.sword.get(i + 1);
					this.sword.set(i + 1, EMP);
					pfind = true;
				}
				if (pbeg < i && Prices.contains(this.sword.get(i - 1))) {
					s = this.sword.get(i - 1) + s;
					this.sword.set(i - 1, EMP);
					pfind = true;
				}
				if (pfind) {
					this.sword.set(i, s);
					this.spos.set(i, POS_NUM);
					res = true;
				}
				// this.spos.set(i, POS_NUM);// 一般数量词
			}

		}
		return res;
	}

	/** 产品名抽取。 按优先级识别，并在最终结果中进行最终筛选 */
	private boolean ItemProducer(int pbeg, int pend) {
		boolean res = false;
		if (pbeg < 0 || pend <= pbeg || sword.size() < pend
				|| prod[0].length() != 0)
			return res;

		final Set<String> Book = new HashSet<String>(Arrays.asList("出版", "标准",
				"手册", "著", "词典", "考试", "修订", "新版", "旧版", "阅读", "人文", "教辅",
				"光盘", "电子版", "诗词", "小说", "文学", "宝典"));
		final String cstr = "c_";
		String s = null, tmp = null;
		String prodClass = null, prodPatt = null; // 类目 词>模式 词>末尾词
		int idxClass = -1, idxPatt = -1;
		StringBuffer sbuf = new StringBuffer();
		boolean PGra = false, PBef = false, PLat = false; // 无效的标志词性

		for (int i = pbeg; i < pend; ++i) {
			s = this.sword.get(i);
			if (Book.contains(s)) {
				for (int j = pbeg; j < pend; ++j) { // 完整的书名
					sbuf.append(this.sword.get(j));
					this.sword.set(j, EMP);
				}
				this.sword.set(pbeg, sbuf.toString());
				this.spos.set(pbeg, POS_PRO);

				if (debug)
					System.out.println("[info] ItemProducer() " + sbuf);
				break;
			}
			tmp = this.spos.get(i);
			PLat = s.length() < 2 || tmp.equals(POS_UNIT) || tmp.equals(POS_NUM)
					|| tmp.equals(POS_SH);
			if (tmp.toLowerCase().indexOf(cstr) != -1) {
				prodClass = s;
				idxClass = i;
				if (pbeg < i && 2 < this.spos.get(i - 1).length()
						&& this.spos.get(i - 1).indexOf(POS_) == -1)
					prodClass = this.sword.get(i - 1) + prodClass;
			}

			if (prodClass == null && (pbeg < i)) {// 描述则末序优先
				if (PGra && !PBef && PLat) {
					prodPatt = this.sword.get(i - 1);
					idxPatt = i - 1;
				} else if (prodPatt == null) {
					if (!PLat && PBef) {
						idxPatt = i;
						prodPatt = this.sword.get(idxPatt);
					} else if (!PBef && PLat) {
						idxPatt = i - 1;
						prodPatt = this.sword.get(idxPatt);
					}
				}
			}
			PGra = PBef;
			PBef = PLat; // 状态更新
		}
		if (prodClass != null) {
			// this.spos.set(idxClass, POS_PRO); //
			prod[0] = prodClass;
			prod[1] = this.spos.get(idxClass);
			res = true;
		}
		if (prodPatt != null) {
			this.spos.set(idxPatt, POS_PRO);
		}
		return res;
	}

	/** 抽取商品类目信息， 无类目则在最后一次时用 signN 个特殊词拼接。 */
	private boolean ItemCategory(int pbeg, int pend, boolean last) {
		// TODO 厂家， 品牌PINPAI 根据商品分类器识别 空格分隔开的全英文 为品牌
		boolean res = false;
		if (pbeg < 0 || pend <= pbeg || sword.size() < pend
				|| cate[0].length() != 0)
			return res;

		final String c = "c_", NULL = "null";
		final int signN = 3;
		int cnt = 0, fixIdx = -1;
		String word, pos;
		for (int i = pbeg; i < pend; ++i) {
			pos = spos.get(i);
			if (pos != null && pos.indexOf(c) != -1) {
				cate[0] = sword.get(i);
				if (0 < i && !spos.get(i - 1).equals(NULL))
					fixIdx = i - 1;
				else if ((i + 1) < spos.size() && !spos.get(i + 1).equals(NULL))
					fixIdx = i + 1;
				if (fixIdx != -1)
					cate[0] = sword.get(fixIdx) + cate[0];
				cate[1] = spos.get(i);
				break;
			} else if (pos.indexOf(POS_) != 0 && last) {
				cate[0] += sword.get(i) + "_";
				cate[1] += spos.get(i) + "~";
				++cnt;
				if (cnt == signN)
					break;
			}
		}
		if (last && cate[0] == null) {
			cate[0] = sword.get(sword.size() - 1);
			cate[1] = spos.get(spos.size() - 1);
			res = true;
		}

		return res;
	}

	// TODO sex,time,功效,产地,制材,颜色,外观特征
	private boolean ItemDescs() {
		// 家用
		boolean res = false;

		return res;
	}

	/** 抽取商家信息。 更新方式： 将第1个词替换为修正词，并更新POS；将余下的值写为NULL；保证不改变元素个数。 */
	private boolean ItemShop(int pbeg, int pend) {
		// TODO 商家：优惠、资质、活动、服务
		boolean res = false;
		if (pbeg < 0 || pend <= pbeg || this.sword.size() < pend)
			return res;

		final Set<String> keySet = new HashSet<String>(Arrays.asList("正品",
				"进口", "行货", "原装", "正宗", // 描述
				"打折", "换购", "促销", "大促", "包邮", "免邮费", "赠品", "优惠", "清仓", "团购")), 
				keyPre = new HashSet<String>(Arrays.asList("送", "赠", "赠送", 
						"仅", "仅仅", "享", "享受")), 
				keySub = new HashSet<String>(Arrays.asList("折"));
		final String[] keyStr = new String[] { "日销千件", "冲三冠", "冲钻价" };

		int ibeg = pbeg, iend = pend, iupdate = -1;
		String s, tmp;
		StringBuffer sbuf = new StringBuffer();
		boolean Fpre = false, Flast = false;

		List<String> words = new ArrayList<String>();
		for (int i = pbeg; i < pend; ++i) { //
			iupdate = -1;
			s = this.sword.get(i);
			sbuf.append(s);
			if (keySet.contains(s)) {
				words.add(s);
				this.spos.set(i, POS_SH);
			} else if (keyPre.contains(s) && i < (pend - 1)) {
				// word+Num
				if (this.sword.get(i + 1).length() < 5) {
					if ((pend - i) == 3) {
						this.sword.set(i + 1, 
								this.sword.get(i+1) + this.sword.get(i + 2));
						this.sword.set(i + 2, EMP);
						this.spos.set(i+2, POS_SH);
					}
					this.sword.set(i, s + this.sword.get(i+1));
					this.spos.set(i, POS_SH);
					this.sword.set(i+1, EMP);
					this.spos.set(i+1, POS_SH);
					continue;
				}
			} else if (keySub.contains(s) && pbeg < i) {
				// Num+word.
				s = this.sword.get(i - 1);
				if (Nums.contains(s)
						|| ('0' <= s.charAt(0) && s.charAt(0) <= '9')) {
					iupdate = i - 1;
					s = this.sword.get(i - 1) + this.sword.get(i);
					this.sword.set(i, EMP);
					this.spos.set(i, POS_SH);
					this.sword.set(i - 1, s);
					this.spos.set(i - 1, POS_SH);
					res = true;
					continue;
				}
			}
		}

		String segment = sbuf.toString(); // segments[segments.length - 1];
		sbuf.delete(0, sbuf.length());
		for (int i = 0; i < keyStr.length; ++i) { // 多个词组成的串
			if (segment.indexOf(keyStr[i]) != -1) {
				s = keyStr[i];
				words.add(s);
				for (int j = pbeg; j < pend; ++j) {
					// 按 长度找第1个匹配的词作结果
					if (s.indexOf(this.sword.get(j)) == 0) {
						sbuf.append(this.sword.get(j));
						int k = 0;
						for (k = j + 1; k < pend && sbuf.length() < s.length(); ++k) {
							sbuf.append(this.sword.get(k));
						}
						if (s.equals(sbuf.toString().trim())) {
							for (int l = k - 1; j <= l; --l) {
								this.sword.set(l, EMP);
								this.spos.set(l, POS_SH);
							}
							this.sword.set(j, s);
							this.spos.set(j, POS_SH);
							res = true;
							break;
						}
						j = k;
						sbuf.delete(0, sbuf.length());
					}
				}
			}
		}

		return res;
	}

	private boolean ItemNumber() {
		// 货号
		return true;
	}

	private boolean ItemBaseInfo() {
		// TODO 宣传词： 款式、质量、价格
		return true;
	}

	@Deprecated
	private String[] WordFilter(String s) {
		// 对字符串进行 非有效字符过滤, 连续数字、字母合并 的基本切分
		List words = new ArrayList<String>();
		StringBuffer sbuf = new StringBuffer();
		if (s.length() <= 2) {
			words.add(s);
		} else {
			// 基于符号进行切割
			char c;
			boolean isC = false, isD = false, isE = false;
			for (int i = 0; i < s.length(); ++i) {
				c = s.charAt(i);
				if (UtilStr.isChinese(c)) {
					if (sbuf.length() != 0 && !isC) { // 已经存在非中文
						if (isD) { // TODO 数字|中文： 可能为数量

						} else if (isE) { // TODO 英文|中文 可能为品牌名

						}
						words.add(sbuf.toString());
						sbuf.delete(0, sbuf.length());
					}
					isC = true;
					isD = false;
					isE = false;
					sbuf.append(c);
				} else if (('0' <= c && c <= '9')) {
					// 处理数字
					if (sbuf.length() != 0 && !isD) {
						if (isC && sbuf.length() > 2) {
							// TODO 对大于 2个字符的中文子串分词
							// String[] wordseg =
							// Util.segmentword(sbuf.toString());
							// words.addAll(Arrays.asList(wordseg));
						} else {
							words.add(sbuf.toString());
						}
						sbuf.delete(0, sbuf.length());
					}
					sbuf.append(c);
					isC = false;
					isD = true;
					isE = false;
				} else if (('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z')) {
					if (sbuf.length() != 0 && !isE) {
						if (isC && sbuf.length() > 2) {
							// TODO 对大于 2个字符的中文子串分词
							// String[] wordseg =
							// Util.segmentword(sbuf.toString());
							// words.addAll(Arrays.asList(wordseg));
						} else {
							words.add(sbuf.toString());
						}
						sbuf.delete(0, sbuf.length());
					}

					sbuf.append(c);
					isC = false;
					isD = true;
					isE = true;
				} else { // 非 中文、字符、数字
					if (c == '.' && isD) {
						sbuf.append(c); // 数字后的小数点
					} else if (!isC
							&& (c == '-' || c == '/' || c == '_' || c == 'x')) {
						// 通用连接符号 不修改前后状态
						continue;
					} else if (sbuf.length() != 0) {
						if (isC && sbuf.length() > 2) {
							// TODO 对大于 2个字符的中文子串分词
							// String[] wordseg =
							// Util.segmentword(sbuf.toString());
							// words.addAll(Arrays.asList(wordseg));
						} else {
							words.add(sbuf.toString());
						}
						sbuf.delete(0, sbuf.length());
					}
					// 抛弃字符 新的起点
					isC = false;
					isD = false;
					isE = false;
				}
			}
		}
		return (String[]) words.toArray(new String[words.size()]);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String[] items = new String[] {
				// "南极人高档竹纤维无痕女式内裤 纯色平角女式舒适女内裤买3送一",
				// "日销千件 4.5英寸IPS视网膜屏 800万像素 5GHz WIFI技术 WCDMA/GSM 3G智能手机",
				"228元大富南4人套餐",
				"仅32元，享我买价62元的ab【琨cd山水产 密云水库野生满籽池沼公鱼 (袋装 500g)】1袋，生长在全北京市一级饮用水源地密云水库，纯天然无污染，常年出口日本韩国和加拿大，首次内销的纯野生池沼公鱼。鳞细、刺软、肉嫩，甘甜香糯，通体一根刺，有着淡淡的黄瓜清香，鱼中的上品，美味中的精华！",
				"" };
		ItemAttribute.debug = true;
		for (int i = 0; i < items.length; ++i) {
			try {
				ItemAttribute itr = new ItemAttribute(items[i]);
				System.out.println("[Attributes] \n" + itr.getAttribute()
						+ "\nprod=" + Arrays.asList(itr.getProd()) + "\nword="
						+ itr.getWord() + "\npos =" + itr.getPos());
			} catch (ParseException e) {
				e.printStackTrace();
			}

		}
	}

}
