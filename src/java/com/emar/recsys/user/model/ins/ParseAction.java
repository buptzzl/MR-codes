package com.emar.recsys.user.model.ins;

/**
 * 解析一个已分词的用户行为数据文件中的行，整个文件为一个类，生成一个训练样本。
 * @author zhoulm
 *
 */
public class ParseAction implements IAttrParser {

	@Override
	public boolean init(String... args) {
		// TODO Auto-generated method stub
		// 
		return false;
	}

	@Override
	public String getAttribute() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setAttribute(String s) {
		// TODO Auto-generated method stub

	}

	@Override
	public Object parseLine(String line) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object getClassify() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object[] getFeatures() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object getWeight() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getClassIndex() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void countClass(int idx) {
		// TODO Auto-generated method stub

	}

	@Override
	public String getStaticInfo() {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
