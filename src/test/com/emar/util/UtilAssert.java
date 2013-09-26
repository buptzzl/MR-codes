package com.emar.util;

import org.junit.Assert;
import weka.classifiers.functions.Logistic;

/**
 * 对junit.Test的自定义拓展
 * 
 * @author zhoulm
 * 
 */
public class UtilAssert {
	public static boolean m_Debug;
	/** Compute machine precision */
	protected static double m_Epsilon, m_Zero;
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
	
	public static void assertArrayEquals(double[] pred, double[] pout) {
		if (pred == null && pout == null) 
			return ;
		
		Assert.assertNotNull(pred);
		Assert.assertNotNull(pout);
		Assert.assertEquals(pred.length, pout.length);
		for (int i = 0; i < pred.length; ++i)
			Assert.assertEquals(true, Math.abs(pred[i] - pout[i]) <= m_Epsilon);
	}

}
