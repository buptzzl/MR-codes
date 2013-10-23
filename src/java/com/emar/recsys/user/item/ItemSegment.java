package com.emar.recsys.user.item;

import java.io.IOException;
import java.io.StringReader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import com.emar.recsys.user.feature.FeatureType;
import com.emar.recsys.user.item.ItemAttribute.ItemFilter;
import com.emar.recsys.user.util.IUserException;
import com.emar.recsys.user.util.WordSegment;
import com.webssky.jcseg.core.IWord;

/**
 * TODO 基础分词、特征抽取类。 输入串必须 用系统(UTF-8)编码。
 * @author zhoulm
 * 
 * @overwrite ItemAttribute
 */
public class ItemSegment {
	
	private static final Pattern cpatt = Pattern
			.compile("\\(|\\)|（|）|\\[|\\]|【|】|\\{|\\}| |,|\\?|;|\"|\\t|，|。|；|？|“|”|、|…|—|！|￥"),
			unwordPatt = Pattern.compile("\\&[A-Za-z]+;");
	private static final String POS_EMP = "null", POS_UNIT = "unit", 
			POS_NUM = "num", 
			FPre="__",
			FCate=FPre+"c", FDesc=FPre+"d", FProd=FPre+"p", 
			FShop=FPre+"s", FSize=FPre+"sz";
	public static final int KGram = 3; // 单字生成ngram的最大值
	private final static Character[] CN_NUMERIC = { '一', '二', '三', '四', '五', '六', '七', '八',
			'九', '壹', '贰', '叁', '肆', '伍', '陆', '柒', '捌', '玖', '○', 'Ｏ', '零',
			'十', '百', '千', '拾', '佰', '仟', '万', '亿',
	// '兆', '京', '顺',
	};
	
	public static boolean debug, kgram, fsegment, WSinit;
	private static Set Units, Nums;
	private static WordSegment ws;
	
	static {
		debug = false;
		kgram = false;
		fsegment = false;
		// 瓶装 等词已经加入词典
		Units = new HashSet<String>(Arrays.asList(
				"元", "$", "￥", 
				"克", "千克", "斤", "公斤", "盎司", "磅", "g","kg", // 重量
				"毫安", "安", //
				"升", "毫升", "加伦", "品脱", "ml", "l", // 容积
				"米", "分米", "厘米", "毫米", "寸", "英寸", "英尺", "英里", "千米", // 长度
				"平方米", "平方厘米", // 面积
				"小样", "中样", "大样", // 化妆品独有
				"大码", "中码", "小码", "条", "件", "套", "双", // 衣服
				"包", "盒", "箱", "袋", "片", "枚", "次"
		));
		Nums = new HashSet<String>(Arrays.asList(
				"一", "二", "三", "四", "五", "六", "七", "八","九"
		));		
		try {
			ws = WordSegment.getInstance();
			WSinit = true;
		} catch (Exception e) {
			e.printStackTrace();
			WSinit = false;
		}
	}
	
	private String item, itemPart;
	private List<String> attributes;
	private List<String> sword; // 分词结果
	private List<String> spos; // 所有词的词性类别
	private List<String> size; // 是否有容量字符串出现
	private IItemFilter myfiler; // 分词过滤器
	private String[] prod; // name,type pair.
	
	/** 商品的一种自定义过滤方式 */
	public class ItemFilter implements IItemFilter {
		@Override
		public String prefilter(String line) {
			String[] items = unwordPatt.split(line);
			StringBuffer sbuf = new StringBuffer();
			for (int i = 0; i < items.length; ++i)
				sbuf.append(items[i] + " ");// 用空格替代无效字符

			return sbuf.toString().toLowerCase();
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
	
	public ItemSegment(String item) throws ParseException {
		if (!WSinit || item == null) {
			throw new ParseException("initial ItemAttribute with NULL",
					IUserException.ErrADItemNull);
		}
		this.myfiler = new ItemFilter();
		this.item = this.myfiler.prefilter(item);

		attributes = new ArrayList<String>();
		sword = new ArrayList<String>(); // 分词结果
		spos = new ArrayList<String>(); // 所有词的词性类别,V_desc1_desc2...
		size = new ArrayList<String>(); // 是否有容量字符串出现

		prod = new String[2];
	}
	
	public List<String> getWord() {
		if (!fsegment)
			fsegment = this.ItemSegment();
		return sword;
	}

	public List<String> getPos() {
		if (!fsegment)
			fsegment = this.ItemSegment();
		return spos;
	}

	public List<String> getSize() {
		if (!fsegment)
			fsegment = this.ItemSegment();
		return size;
	}

	public String[] getProd() {
		if (!fsegment)
			fsegment = this.ItemSegment();
		return prod;
	}

	/** 抽取全部属性。 */
	public List<String> getAttribute() {
		if (!fsegment)
			fsegment = this.ItemSegment();

		// raw info.
		this.attributes.addAll(this.sword);
		this.attributes.addAll(this.spos);
		this.attributes.addAll(this.size);
		return this.attributes;
	}
	
	/** 商品标题粗切分。 分隔符为： 空格、成对的 括号、非数字与英文 两个字、 三个字中末尾为 装 等不再分词 */
	private boolean ItemSegment() {
		if (this.item == null)
			return false;

		List<String> item = new ArrayList<String>(); // 单子词临时空间
		List<String> info = new ArrayList<String>(); // 临时存储分词结果，调试用
		String w;
		String[] pos = null;
		IWord word = null;

		int pbeg = 0, pend = sword.size(), plast = -1;
		String[] sitem = cpatt.split(this.item); // 基于正则做粗切分
		for (String si : sitem) {
			// ws.Jcseg.reset(new StringReader(this.item));
			try {
				this.itemPart = si;
				ws.Jcseg.reset(new StringReader(si));
				while ((word = ws.Jcseg.next()) != null) {
					pos = word.getPartSpeech();
					w = word.getValue();
					spos.add(pos == null ? POS_EMP : pos[0]);
					sword.add(w);
				}
			} catch (IOException e) {
				e.printStackTrace();
				continue;
			}
			pend = sword.size();
			
			ItemShop(pbeg, pend, false);
			ItemSize(null); // TODO
			ItemProducer(pbeg, pend, false);
			ItemDescribe(pbeg, pend, false);
			ItemCategory(pbeg, pend, false);
			
		}
		if (debug)
			System.out.println("[Debug] ItemSegment() segmented words:\ninput="
					+ this.item + "\nsegment=" + info);

		return true;
	}
	
	/** 抽取 数量大小 特征. 要求单位在最后。 结果写入 words列表 */
	private void ItemSize(String... words) { //int pbeg, int pend, boolean last
		// TODO 容量|尺码，价格 等
		if (words == null || words.length == 0)
			return;
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
				sword.add(k * (int) Math.pow(10, digits == -1 ? words[0].length() : digits) + "");
				spos.add(POS_UNIT);
			}
		}
		return;
	}

	private boolean ItemCategory(int pbeg, int pend, boolean last) {
		// TODO 产品类目 根据商品分类器识别
		// BOOK 书名： 出版、标准|手册|书|著|词|考|、修订、最新版、N版、N册、编、阅读、人文、教辅
		
		return true;
	}

	/** 抽取商品名，默认用商品类目信息， 无类目则在最后一次时用 signN 个特殊词拼接。 */
	private boolean ItemProducer(int pbeg, int pend, boolean last) {
		// TODO 厂家， 品牌PINPAI 根据商品分类器识别 空格分隔开的全英文 为品牌
		if (pbeg < 0 || pend <= pbeg || sword.size() < pend || prod[0] != null)
			return false;

		final String c = "c_", NULL = "null";
		final int signN = 3;
		int cnt = 0, fixIdx = -1;
		String word, pos;
		for (int i = pbeg; i < pend; ++i) {
			pos = spos.get(i).toLowerCase();
			if (pos != null && pos.indexOf(c) != -1) {
				prod[0] = sword.get(i);
				if (0 < i && !spos.get(i - 1).equals(NULL))
					fixIdx = i - 1;
				else if ((i+1) < spos.size() && !spos.get(i+1).equals(NULL))
					fixIdx = i + 1;
				if (fixIdx != -1)
					prod[0] = sword.get(fixIdx) + prod[0];
				prod[1] = spos.get(i);
				break;
			} else if (!pos.equals(NULL) && last) {
				prod[0] += sword.get(i) + "_";
				prod[1] += spos.get(i) + "~";
				++cnt;
				if (cnt == signN)
					break;
			}
		}
		if (last && prod[0] == null) {
			prod[0] = sword.get(sword.size() - 1);
			prod[1] = spos.get(spos.size() - 1);
		}

		return true;
	}
	/** 商品描述。 sex,time,功效,产地,制材,颜色,外观特征 款式, 家用 */
	private boolean ItemDescribe(int pbeg, int pend, boolean last) {
		// TODO 
		
		return true;
	}
	/** 商家：优惠、资质、活动、服务 */
	private boolean ItemShop(final int pbeg, final int pend, final boolean last) {
		if (pbeg < 0 || pend <= pbeg || this.sword.size() < pend)
			return false;
		
		final Set<String> keySet = new HashSet<String>(Arrays.asList(
				"正品", "进口", "行货", // 描述
				"打折", "换购", "促销", "大促", "包邮", "免邮费", "赠品", "优惠", 
				"团购")),
				keyPre = new HashSet<String>(Arrays.asList("送", "赠")),
				keySub = new HashSet<String>(Arrays.asList("折"));
		final String[] keyStr = new String[] {"日销千件", "冲三冠"};

		int ibeg=pbeg, iend=pend;
		String s, tmp;
		StringBuffer sbuf = new StringBuffer();
		
		List<String> words = new ArrayList<String>();
		for(int i = pbeg; i < pend; ++i) { // 
			s = this.sword.get(i);
			if (keySet.contains(s)) {
				words.add(s);
				this.sword.set(i, FShop);
				this.spos.set(i, FShop);
			} else if (keyPre.contains(s) && i < (pend - 1)) {
//				if ()
			} else if (keySub.contains(s) && pbeg < i) {
				// Num+word.
				if (Nums.contains(this.sword.get(i - 1))) {
					words.add(this.sword.get(i - 1) + s);
					this.sword.set(i, FShop);
					this.spos.set(i, FShop);
					this.sword.set(i - 1, FShop);
					this.spos.set(i - 1, FShop);
				}
			}
		}
		for (int i = 0; i < keyStr.length; ++i) { // 多个词组成的串
			if (this.itemPart.indexOf(keyStr[i]) != -1) {
				s = keyStr[i];
				words.add(s);
				for (int j = pbeg; j < pend; ++j) {
					if (s.indexOf(this.sword.get(j)) != -1) {
						sbuf.append(this.sword.get(j));
						int k;
						for (k = j + 1; k <pend && sbuf.length() < s.length(); ++k) {
							sbuf.append(this.sword.get(k));
						}
						if (s.equals(sbuf.toString().trim())) {
							for (int l = k - 1; j <= l; --l) {
								this.sword.set(l, FShop);
								this.spos.set(l, FShop);
							}
						}
					}
				}
			}
		}
		
		return true;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
