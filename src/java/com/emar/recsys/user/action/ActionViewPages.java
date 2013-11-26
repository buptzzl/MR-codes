package com.emar.recsys.user.action;

import java.io.FileNotFoundException;

import org.apache.log4j.Logger;

/**
 * 抽取用户有对应关键字出现时的数据 和下方的K个样本
 * 
 * @author zhoulm
 * 
 */
public class ActionViewPages extends ActionExtract {

	private Logger log = Logger.getLogger(ActionViewPages.class);

	private static final int DEF_K = 2;
	private int iRange;

	public ActionViewPages() {
		super();
		iRange = this.configure_.getInt("extract.iRange", DEF_K);
		log.warn("init iRange=" + iRange);
	}

	public ActionViewPages(String[] args) throws FileNotFoundException {
		super(args);
		iRange = this.configure_.getInt("extract.iRange", DEF_K);
		log.warn("init iRange=" + iRange);
	}

	@Override
	public boolean BatchFormat() {
		int i = 0, j = 0;
		for (i = 0; i < this.data.size();) {
			this.format(i);
			if (this.whiteFilter(i)) {
				for (j = i + 1; (j - i) < iRange; ++j)
					this.format(j);
				i = j - 1;
			}
			++i;
		}

		return false;
	}

	@Override
	public boolean format(int index) {
		return super.format(index);
	}

	@Override
	public boolean Filter(int index) {
		return true;
	}

	@Override
	public boolean whiteFilter(int index) {
		// 对抽取后的字符串 进行过滤。
		boolean unfind = true;
		String tmp = this.data.get(index);
		for (int i = 0; i < WordsWrite.length && unfind; ++i) {
			unfind = (tmp.indexOf(WordsWrite[i]) == -1);
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
