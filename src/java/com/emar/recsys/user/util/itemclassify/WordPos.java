package com.emar.recsys.user.util.itemclassify;
public class WordPos {
	private String word;
	private String pos;

	public WordPos() {
		word = "";
		pos = "";
	}

	public void setword(String _word) {
		word = _word;
	}

	public void setpos(String _pos) {
		pos = _pos;
	}

	public void setwordpos(String _word, String _pos) {
		word = _word;
		pos = _pos;
	}

	public String getword() {
		return word;
	}

	public String getpos() {
		return pos;
	}

	public WordPos getwordpos() {
		WordPos cwordpos = new WordPos();
		cwordpos.word = word;
		cwordpos.pos = pos;
		return cwordpos;
	}

}
