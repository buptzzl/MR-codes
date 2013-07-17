package com.emar.recsys.user.util.itemclassify;
public class CateProb {
	public String cate;
	public double prob;

	public CateProb() {
		cate = "";
		prob = 0;
	}

	public void SetCate(String _cate) {
		this.cate = _cate;
	}

	public void SetProb(double _prob) {
		this.prob = _prob;
	}

	public String GetCate() {
		return this.cate;
	}

	public double GetProb() {
		return this.prob;
	}

	public void SetValue(String _cate, double _prob) {
		this.cate = _cate;
		this.prob = _prob;
	}

	public CateProb GetValue() {
		return this;
	}

}
