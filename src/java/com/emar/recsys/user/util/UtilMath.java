package com.emar.recsys.user.util;

public class UtilMath {
	
	public static boolean m_Debug;
	/** Compute machine precision */
	public static double m_Epsilon, m_Zero;
	static {
		m_Epsilon = 1.0;
		while (1.0 + m_Epsilon > 1.0) {
			m_Epsilon /= 2.0;
		}
		m_Epsilon *= 2.0;
		m_Zero = Math.sqrt(m_Epsilon);
		if (m_Debug)
			System.err.print("Machine precision is " + m_Epsilon
					+ " and zero set to " + m_Zero);
	}

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
	
	/** 将Object 对象转换为 double 类型 */
	public static double getNumbers(Object obj) throws Exception {
		double score;
		if (obj instanceof String) 
			score = Double.parseDouble((String)obj);
		else if (obj instanceof Integer)
			score = (Integer)obj;
		else if (obj instanceof Float) 
			score = (Float)obj;
		else 
			score = (Double) obj;
		return score;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
