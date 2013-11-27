package com.emar.recsys.user.action;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

/**
 * 抽取用户有对应关键字出现时的数据 和上方的K个样本
 * 
 * @author zhoulm
 * 
 */
public class ActionViewPages extends ActionExtract {

	private Logger log = Logger.getLogger(ActionViewPages.class);

	private static final int DEF_K = 2;
	private int iRange;
	private List<String> hitWhite;
	private List<String> hitBlack;

	public ActionViewPages() {
		super();
		iRange = this.configure_.getInt("extract.iRange", DEF_K);
		hitWhite = new ArrayList<String>();
		hitBlack = new ArrayList<String>();
		log.warn("init iRange=" + iRange);
	}

	public ActionViewPages(String[] args) throws FileNotFoundException {
		super(args);
		iRange = this.configure_.getInt("extract.iRange", DEF_K);
		hitWhite = new ArrayList<String>();
		hitBlack = new ArrayList<String>();
		log.warn("init iRange=" + iRange);
	}

	@Override
	public boolean BatchFormat() {
		int i = 0, j = 0;
		for (i = 0; i < this.data.size();) {
			this.format(i);
			if (this.whiteFilter(i)) {
				this.data.set(i, this.data.get(i) + ", " + this.hitWhite);
				this.flags.set((i - iRange) < 0 ? 0 : (i - iRange), i);
//				for (j = i + 1; (j - i) < iRange; ++j) // 向下加入K 个
//					this.format(j); 
//				i = j - 1;
			} else {
				this.flags.clear(i);
			}
			++i;
		}

		return false;
	}

//	@Override
//	public boolean format(int index) {
//		return super.format(index);
//	}

	@Override
	public boolean Filter(int index) {
		return true;
	}

	@Override
	public boolean whiteFilter(int index) {
		// 对抽取后的字符串 进行过滤。
		boolean unfind = true;
		this.hitWhite.clear();
		String tmp = this.data.get(index);
		for (int i = 0; i < WordsWhite.length; ++i) {  // 不采用匹配即终止
			if (tmp.indexOf(WordsWhite[i]) != -1) {
				unfind = false;
				this.hitWhite.add(WordsWhite[i]);
			}
		}
		return unfind;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
	}

}
