package com.emar.recsys.user.util.itemclassify;

public class CateInfo {
	private int cid;
	private int parent_cid;
	private String pos;

	public CateInfo() {
		cid = -1;
		parent_cid = -1;
		pos = "wz";
	}

	public void setcid(int _cid) {
		cid = _cid;
	}

	public void setpos(String _name) {
		pos = _name;
	}

	public String getpos() {
		return pos;
	}

	/*
	 * public static void setis_parent(boolean _is_parent) { is_parent =
	 * _is_parent; }
	 */
	public void setparent_cid(int _parent_cid) {
		parent_cid = _parent_cid;
	}

	public int getcid() {
		return cid;
	}

	public int getparent_cid() {
		return parent_cid;
	}

}
