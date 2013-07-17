package com.emar.recsys.user.util.itemclassify;

public class WordGender {
	public int manid;
	public int womanid;
	public int motherid;
	public int childid;

	public WordGender() {
		this.manid = 0;
		this.womanid = 0;
		this.motherid = 0;
		this.childid = 0;
	}

	public void SetManId(int _manid) {
		this.manid = _manid;
	}

	public void SetWomanId(int _womanid) {
		this.womanid = _womanid;
	}

	public void SetMotherId(int _motherid) {
		this.motherid = _motherid;
	}

	public void SetChildId(int _childid) {
		this.childid = _childid;
	}

	public int GetManId() {
		return this.manid;
	}

	public int GetWomanId() {
		return this.womanid;
	}

	public int GetMotherId() {
		return this.motherid;
	}

	public int GetChildId() {
		return this.childid;
	}

}
