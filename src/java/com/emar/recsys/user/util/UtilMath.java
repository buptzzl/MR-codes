package com.emar.recsys.user.util;

public class UtilMath {

	public static Integer max(int[] arr) {
		if (arr == null || arr.length == 0) {
			return null;
		}
		int r = arr[0];
		for (int i : arr) {
			if (r < i) {
				i = r;
			}
		}
		return r;
	}

	public static Integer min(int[] arr) {
		if (arr == null || arr.length == 0) {
			return null;
		}
		int r = arr[0];
		for (int i : arr) {
			if (r > i) {
				i = r;
			}
		}
		return r;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
