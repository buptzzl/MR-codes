package com.emar.recsys.user.model.ins;

import java.util.Arrays;

/**
 * 解析 classid, f1, f2 ....
 */
public final class ParseArrayAtom implements IAttrParser {
	// 定义 arff 的分类属性说明字符串
	String[] atom;
	String[] features;
	private static final int IdxClass = 0, IdxFBegin = 1, 
			IdxSumIn = 99, IdxSum = 0;

	public ParseArrayAtom() {
	}

	@Override
	public String[] parseLine(String line) {
		this.countClass(IdxSumIn);  // Total
		
		final String sepa = ", ";
		atom = line.split(sepa);
		if (atom.length < 2) // 合法性检验
			atom = null;
		this.countClass(getClassIndex());
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
	public boolean init(String... args) {
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

	@Override
	public int getClassIndex() {
		return IdxSum;
	}

	@Override
	public void countClass(int idx) {
		IAttrParser.CntClass[idx] += 1;
	}

	@Override
	public String getStaticInfo() {
		return String.format("total input data=%d, unnull instances=%d", 
				CntClass[IdxSumIn], CntClass[IdxSum]);
	}

}
