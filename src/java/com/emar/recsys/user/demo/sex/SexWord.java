package com.emar.recsys.user.demo.sex;

import java.util.*;

import com.emar.recsys.user.util.UtilStr;

/**
 * 基于关键词的性别识别
 * 
 * @author zhoulm
 * 
 */
public class SexWord {

	public static Set<String> discade;
	public static List<Set> fset; // 优先级： 从后往前
	public static List<Set> mset;
	public static boolean isdebug;
	public static int WMxLen; // Xgram的最大长度

	static { // 初始化关键词集合
		discade = new HashSet<String>(10);
		discade.addAll(Arrays.asList(" ", "\t", "#", "\\", "/", ",", ";", "+",
				"=", "|", "?", "-", "&", "$", "%", "*", "!", "@", "，", "；",
				"。", "？", "『", "』", "[", "]", "【", "】", "(", ")", "（", "）"));
		final int N = 2;
		fset = new ArrayList<Set>(N);
		for (int i = 0; i < N; ++i)
			fset.add(new HashSet<String>(100));
		mset = new ArrayList<Set>(N);
		for (int i = 0; i < N; ++i)
			mset.add(new HashSet<String>(100));
		Set si;
		si = fset.get(0);
		si.addAll(Arrays.asList("女", "妇", "妈", "母亲", "妻子", "娘子", "新娘", "公主",
				"闺蜜", "姐妹", "御姐", "萝莉"));
		si = mset.get(0);
		si.addAll(Arrays.asList("男", "爸", "父亲", "丈夫", "新郎", "王子"));
		si = fset.get(1);
			// 口红(提)
		si.addAll(Arrays.asList("发饰", "发夹", "发卡", "发箍", "发带", "面膜", "耳环", "耳钉",
				"耳夹", "耳线", "粉底", "粉饼", "唇笔", "唇线", "睫毛", "眼影", "眼线",
				"眉笔", "眉粉", "眉膏", "首饰盒", "精油", "身体霜", "身体乳", "手链", "手镯", "指甲油",
				"美甲", "胸", "撞色包", "臀", "卫生巾", "裙", "丝袜", "打底裤", "塑身裤", "旗袍",
				"腿袜", "高跟", "流苏", "妈妈鞋", "松糕鞋", "马丁靴", "洞洞鞋",

				"蕾丝", "雪纺", "吊带", "显瘦", "妩媚"));
		si = mset.get(1);
			// 烟 
		si.addAll(Arrays.asList("褪须刀", "啤酒", "白酒", "烈酒", "打火机", "zippo", "沙滩裤", "驾车鞋"));
		WMxLen = 3; // 最大子串长

		if (isdebug)
			System.out.println("[Info] SexWord::static-init female-size="
					+ (fset.get(0).size() + fset.get(1).size())
					+ "\nmale-size="
					+ (mset.get(0).size() + mset.get(1).size())
					+ "\nstop-words-size=" + discade.size());
	}

	/**
	 * 识别字符串的性别特征
	 * 
	 * @param name
	 * @return 0:unk, 1:f, -1:man.
	 */
	public static int isman(String name) {
		int res = 0;
		if (name == null || name.trim().length() == 0)
			return res;

		name = name.trim();
		String tname = SexWord.trim(name); // 清洗无效字符串
		if (tname.length() != 0) {
			res = SexWord.getSex(name);
		}

		return res;
	}

	private static String trim(String name) {
		// TODO 裁剪字符串中无关的字符

		return name.trim();
	}

	private static int getSex(String name) {
		int res = 0, f = 1, m = -1;

		Set<String> multiWords = SexWord.wordSegment(name);
		int size = multiWords.size();
		Set<String> mwords = new HashSet<String>(multiWords);
		// 后序优先, 女性优先
		while (true) {
			mwords.removeAll(fset.get(1));
			if (mwords.size() != size) {
				if (isdebug)
					multiWords.removeAll(mwords);
				// System.out.println("[Info] SexWord::getSex() " + multiWords);

				res = f;
				break;
				// return res;
			}
			mwords.removeAll(mset.get(1));
			if (mwords.size() != size) {
				res = m;
				// return res;
				if (isdebug)
					multiWords.removeAll(mwords);
				break;
			}
			mwords.removeAll(fset.get(0));
			if (mwords.size() != size) {
				res = f;
				if (isdebug)
					multiWords.removeAll(mwords);
				break;
				// return res;
			}
			mwords.removeAll(mset.get(0));
			if (mwords.size() != size) {
				res = m;
				if (isdebug)
					multiWords.removeAll(mwords);
				break;
				// return res;
			}
			break;
		}
		if (isdebug && res != 0)
			System.out.println("[Info] SexWord::getSex() key=" + multiWords);
		return res;
	}

	private static Set<String> wordSegment(String name) {
		Set<String> res = new HashSet<String>(name.length() * 2);
		List<List> xgram;
		xgram = UtilStr.Xgram(name, SexWord.WMxLen, discade); // 含汉字的串的xgram切割
		for (List xi : xgram) {
			res.addAll(xi);
		}
		return res;
	}

	private static boolean getPattern(String s) {
		// TODO 识别特定的模式，修正性别识别

		return true;
	}

	// private SexWord ins;
	// private SexWord() {}
	// public static SexWord getInstance() {
	// if(ins == null) {
	// ins = new SexWord()
	// }
	// }
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String[] atom = new String[] {
				"酒店客栈, 钦州紫荆花城市酒店",
				"蜂蜜, 百氏郎结晶蜂蜜(瓶装 400g)",
				"三都港醇香黄鱼鲞 (袋装 200g)",
				"仅80元/张，乐享价值198元东丽湖温泉欢乐谷单人通票1张（温泉+娱乐设施，不含餐）！通票有效期至2013年5月31日！亚洲最大室内温泉游乐主题公园，冬日养生温泉，和家人一起体验温泉水上游乐场~",
				"碧柔男士控油爽肤乳液(瓶装 95ml)", "碧柔女士控油爽肤乳液(瓶装 95ml)",
				"阳光少女无籽葡萄干(进口食品 168g)" };

		int res;
		for (int i = 0; i < atom.length; ++i) {
			SexWord.isdebug = true;
			res = SexWord.isman(atom[i]);
			System.out.println(String.format("[Info] res=%d\tinput=%s", res,
					atom[i]));
			break;
		}
	}

}
