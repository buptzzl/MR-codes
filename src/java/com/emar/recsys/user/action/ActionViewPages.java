package com.emar.recsys.user.action;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

/**
 * 抽取用户有对应关键字出现时的数据 和上方的K个样本
 * 支持黑白名单关键词集，关系：在白名单成功时再执行黑名单否决。
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
		this.init();
	}

	public ActionViewPages(String[] args) throws FileNotFoundException {
		super(args);
		this.init();
	}
	
	public ActionViewPages(List<String> mydata) {
		super(mydata);
		this.init();
	}
	
	private void init() {
		iRange = this.configure_.getInt("extract.iRange", DEF_K);
		hitWhite = new ArrayList<String>();
		hitBlack = new ArrayList<String>();
		log.warn("init iRange=" + iRange);
	}

	@Override
	public boolean BatchFormat() {
		int i = 0, j = 0;
		for (i = 0; i < this.data.size();++i) {
			if (this.format(i) && this.whiteFilter(i) && !this.blackFilter(i)) {
				this.data.set(i, this.data.get(i) + ", " + this.hitWhite);
				this.flags.set((i - iRange) < 0 ? 0 : (i - iRange), i); //向上加入K个
			} else {
				this.flags.clear(i);
			}
		}

		return true;
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
		return (!unfind);
	}
	
	@Override
	protected boolean blackFilter(int index) {
		boolean unfind = true;
		this.hitBlack.clear();
		String tmp = this.data.get(index);
		for (int i = 0; i < WordsBlack.length; ++i) {
			if (tmp.indexOf(WordsBlack[i]) != -1) {
				unfind = false;
				this.hitBlack.add(WordsBlack[i]);
			}
		}
		return (!unfind);
	}
	
	public int getRange() {
		return this.iRange;
	}
	
	

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
	}

}
