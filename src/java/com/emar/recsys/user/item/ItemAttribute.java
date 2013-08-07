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
 * TODO 按TB 商品名挖掘流程， 实现商品名切分 item属性： 属性有对应的优先级别；每识别一个属性，对应的内容即被删除
 * 
 * @author zhoulm
 * 
 */
public class ItemAttribute {
	private static boolean isWS;
	private static WordSegment ws;
	private static final Pattern cpatt = Pattern
			.compile("\\(|\\)|（|）|\\[|\\]|【|】");
	private static Set PUNC, Units;
	private static final String FPrefix = String.format("%s%s%s",
			FeatureType.AD, FeatureType.SEG, FeatureType.TITLE, FeatureType.SEG);
	private static final String EMP_POS = ""; //
	public static final int KGram = 3; // ngram最大值
	public static boolean debug;

	final Character[] CN_NUMERIC = { '一', '二', '三', '四', '五', '六', '七',
			'八', '九', '壹', '贰', '叁', '肆', '伍', '陆', '柒', '捌', '玖', '○',
			'Ｏ', '零', '十', '百', '千', '拾', '佰', '仟', '万', '亿', 
			// '兆', '京', '顺',
	};
	static {
		debug = false;
		// 瓶装 等词已经加入词典
		Units = new HashSet<String>(Arrays.asList( "元",
				"克", "千克", "斤", "公斤", "盎司", "磅", // 重量
				"升", "毫升", "加伦", "品脱", // 容积
				"米", "分米", "厘米", "毫米", "寸", "英寸", "英尺", "英里", "千米", // 长度
				"平方米", "平方厘米", // 面积
				"小样", "中样", "大样", // 化妆品独有
				"大码", "中码", "小码", "条", "件", "套" // 衣服
		));
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

	public ItemAttribute(String item) throws ParseException {
		if (!isWS || item == null) {
			throw new ParseException("initial ItemAttribute with NULL",
					IUserException.ErrADItemNull);
		}
		this.item = item;
		
		attributes = new ArrayList<String>();
		sword = new ArrayList<String>(); // 分词结果
		spos = new ArrayList<String>(); // 所有词的词性类别
		size = new ArrayList<String>(); // 是否有容量字符串出现
	}

	public List<String> getAttribute() {
		try {
			this.ItemSegment();
		} catch (IOException e) {
			return this.attributes;
		}
		
		this.ItemShop();
		this.ItemCategory();
		this.ItemPattern();
		this.ItemProducer();
		this.ItemBaseInfo();
		// raw info.
		this.attributes.addAll(this.sword);
		this.attributes.addAll(this.spos);
		this.attributes.addAll(this.size);
		return this.attributes;
	}

	private void ItemSegment() throws IOException {
		// 商品标题粗切分, 并分词。 分隔符为： 空格、成对的 括号、非数字与英文  两个字、 三个字中末尾为 装 等不再分词，
		List<String> item = new ArrayList<String>(); // 单子词临时空间
		List<String> info = new ArrayList<String>(); // 临时存储分词结果，调试用
		String w;
//		boolean fsingle = false;
		String[] pos = null;
		IWord word = null;
		ws.Jcseg.reset(new StringReader(this.item));
		while ((word = ws.Jcseg.next()) != null) {
			pos = word.getPartSpeech();
			w = word.getValue();
			if(debug)
				info.add(w + "/" + (pos==null? "null":pos[0]));
			
			if (pos != null && !Units.contains(w)) { // 非计量单元
				if (item.size() != 0) {
					this.SWordCombine(item);
					item.clear();
				}
//				fsingle = true;
				sword.add(w); // add word.
				spos.add(pos[0]);
			} else {
				int[] wcnt = UtilStr.strCharCnt(w);
				if (w.length() == 1 || Units.contains(w)
						|| (wcnt[1] + wcnt[3]) == w.length() || (wcnt[2] + wcnt[3]) == w.length()) {
					// 单汉字 连续的字符or数字串看做单个字
					item.add(w);
					continue;
				}
				if (item.size() != 0) {
					this.SWordCombine(item);
					item.clear();
				}
//				fsingle = true;
				if ((wcnt[1] + wcnt[2]) == w.length()
						&& UtilStr.isDigital(w.substring(0, wcnt[2]))) // 数量模式
					size.add(w);
				sword.add(w);
				spos.add(EMP_POS); // 每个词都有对应的词性
			}
		}
		this.SWordCombine(item);
		
		if(debug) 
			System.out.println("[Debug] ItemSegment() segmented words:\ninput="
					+ this.item + "\nsegment=" + info);
		
		return;
	}

	private void SWordCombine(List<String> item) {
		// 处理k-gram, update sword & spos.
		String s;
		final int NUnitTrueMn = 1, NUnitTrueEMx = 3;
		int idx = 0;
		boolean isD = false, isU = false, isE = false; // 数字 单位 英文
		for (int i = 0; i < item.size(); ++i) {
			// 识别数量模式
			s = item.get(i);
			if (Units.contains(s) && (isD || s.length() > NUnitTrueMn)) { // 单位
				sword.add(item.get(i-1) + s);
				size.add(sword.get(sword.size() - 1)); //
				isD = false;
				isU = true;
				isE = false;
				SWordNGram(item, idx, i - 1);
				idx = i + 1;
				// size.remove(i-1);
				// size.remove(i);
				continue;
			}
			int[] wcnt = UtilStr.strCharCnt(s);
			if ((wcnt[2] + wcnt[3]) == s.length()) {
				if(!isD) { // 非字符数字
					SWordNGram(item, idx, i-1);
					idx = i;
				}
				isE = false;
				isU = false;
				isD = true;
				continue;
			}
			if ((wcnt[1] + wcnt[3]) == s.length() && isD && wcnt[2] < NUnitTrueEMx) {
				sword.add(item.get(i-1) + s);
				size.add(sword.get(sword.size() - 1)); //
				isD = false;
				isU = false;
				isE = true;
				SWordNGram(item, idx, i - 1);
				idx = i + 1;
				// size.remove(i-1);
				// size.remove(i);
				continue;
			}
			if((wcnt[1] + wcnt[2] != 0) && !(isE || isD)) {
				SWordNGram(item, idx, i);  // 中文与非中文 处分割开
				idx = i;
			}
		}
		SWordNGram(item, idx, item.size());
	}

	private void SWordNGram(List<String> item, int beg, int end) {
		// 生成Kgram
		if (beg < 0 || end <= beg || item.size() < end)
			return;
		StringBuffer sbuf = new StringBuffer();
		if ((end - beg) < 3) {
			for (int i = 0; i < (end - beg); ++i)
				sbuf.append(item.get(i + beg));
			sword.add(sbuf.toString());
			spos.add(EMP_POS);
		} else {
			List<List> grams = UtilStr.Xgram(item.subList(beg, end), KGram);
			for (int i = 0; i < grams.size() - 1; ++i) { // no 1-gram.
				sword.addAll(grams.get(i));
			}
		}
		return;
	}

	private boolean ItemCategory() {
		// TODO 产品类目 根据商品分类器识别
		// BOOK 书名： 出版、标准|手册|书|著|词|考|、修订、最新版、N版、N册、编、阅读、人文、教、辅
		return true;
	}

	private boolean ItemProducer() {
		// TODO 厂家， 品牌PINPAI 根据商品分类器识别
		// 空格分隔开的全英文 为品牌
		return true;
	}

	private boolean ItemPattern() {
		// TODO sex,time,功效,产地,制材,颜色,外观特征
		// 家用

		return true;
	}

	private boolean ItemShop() {
		// TODO 商家：优惠、资质、活动、服务
		// 日销千件， 送耳机，冲三冠， 积分，换购；
		// 大陆行货, 进口

		return true;
	}

	private boolean ItemShipper() {
		// TODO 邮递信息
		// 顺丰8折
		return true;
	}

	private boolean ItemNumber() {
		// 货号
		return true;
	}

	private boolean ItemBaseInfo() {
		// TODO 宣传词： 款式、质量、价格
		return true;
	}

	private boolean SizeMulti() {
		// TODO size中 指示多个物品的一些模式 ： N+1, 200g*3

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
				"柴火大院珍珠大米(袋装 5kg)",
				"4.5英寸IPS视网膜屏 800万像素 5GHz WIFI技术 WCDMA/GSM 3G智能手机",
				"仅32元，享我买价62元的【琨山水产 密云水库野生满籽池沼公鱼 (袋装 500g)】1袋，生长在全北京市一级饮用水源地密云水库，纯天然无污染，常年出口日本韩国和加拿大，首次内销的纯野生池沼公鱼。鳞细、刺软、肉嫩，甘甜香糯，通体一根刺，有着淡淡的黄瓜清香，鱼中的上品，美味中的精华！",
				""
		};
		ItemAttribute.debug = true;
		for(int i = 0; i < items.length; ++i) {
			try {
				ItemAttribute itr = new ItemAttribute(items[i]);
				System.out.println("[Info] \n" + itr.getAttribute());
			} catch (ParseException e) {
				e.printStackTrace();
			}
			
		}
	}

}
