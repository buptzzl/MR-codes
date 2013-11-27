package com.emar.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;

import com.emar.recsys.user.action.ActionViewPages;

/**
 * 测试代码不可见等的解决方法。
 * 
 * @author zhoulm
 * 
 */
public class UtilAccess {
	private static Logger log = Logger.getLogger(UtilAccess.class);

	/** 获得对象的指定属性，可直接修改其值 */
	public static Field getField(Class cType, Object instance, String fieldName)
			throws IllegalAccessException, NoSuchFieldException {
		Class c = cType;

		Field field = null;
		try {
			field = c.getDeclaredField(fieldName);
		} catch (NoSuchFieldException e) {
			if (c.getSuperclass() == null)
				throw new NoSuchFieldError(fieldName);
			log.info("recall superclass. " + c.getSuperclass());
			return getField(c.getSuperclass(), instance, fieldName);
		}
		// 参数值为true，禁用访问控制检查，从而使得该变量可以被其他类调用
		field.setAccessible(true);
		return field;
	}

	public static Method getMethod(Class cType, Object instance, String methodName,
			Class[] paraType) throws NoSuchMethodException {
		Method m = null;
		try {
			m = cType.getDeclaredMethod(methodName, paraType);
		} catch (NoSuchMethodException e) {
			if (cType.getSuperclass() == null)
				throw new NoSuchMethodException(methodName);
			log.info("recall superclass: " + cType.getSuperclass());
			return getMethod(cType.getSuperclass(), instance, methodName, paraType);
		}
		m.setAccessible(true);
		return m;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// @UT.
		ActionViewPages tView = null;
		tView = new ActionViewPages();

		Field tFie;
		Method tMet;
		try {
			tFie = getField(ActionViewPages.class, tView, "configure_");
			Object tVal = tFie.get(tView);
			System.out.println("reflect get configure=" + tVal + "\nObject="
					+ Object.class.getSuperclass());

			tFie = getField(ActionViewPages.class, tView, "WordsWhite");
			String[] wlist = (String[]) tFie.get(tView);
			System.out
					.println("reflect get WordsWhite=" + Arrays.asList(wlist));
			wlist[0] = "test";
			System.out
					.println("reflect get WordsWhite=" + Arrays.asList(wlist));
			tFie = getField(ActionViewPages.class, tView, "WordsWhite");
			wlist = (String[]) tFie.get(tView);
			System.out
					.println("reflect get WordsWhite=" + Arrays.asList(wlist));

			Class[] paras = new Class[] { int.class };
			tMet = getMethod(ActionViewPages.class, tView, "Filter", paras);
			System.out.println("invoke method: "
					+ tMet.invoke(tView, new Object[] { new Integer(0) }));

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
