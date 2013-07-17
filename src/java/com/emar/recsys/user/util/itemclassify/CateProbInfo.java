package com.emar.recsys.user.util.itemclassify;
import java.util.Vector;

public class CateProbInfo {
	public Vector<CateProb> veccateprob = new Vector<CateProb>();

	public CateProbInfo() {
		veccateprob.clear();
	}

	public void AddCateProb(CateProb cateprob) {
		veccateprob.add(cateprob);
	}

	public CateProb GetCateProb(int pos) {
		return veccateprob.elementAt(pos);
	}
}
