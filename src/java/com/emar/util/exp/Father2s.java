package com.emar.util.exp;

public class Father2s extends Father1s {

	@Override
	public void test() {
		System.out.println("from 2s.");
	}
	
	public static void main(String[] args) {
		Father f1 = new Father1s();
		Father f2 = new Father2s();
		f1.test();
		f2.test();
	}
	
}
