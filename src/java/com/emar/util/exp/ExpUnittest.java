package com.emar.util.exp;

import static org.junit.Assert.*;

import java.util.ArrayList;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

// 一个Junit 4 的单元测试用例执行顺序为：@BeforeClass –> @Before –> @Test –> @After –> @AfterClass;
// 每一个测试方法的调用顺序为：@Before –> @Test –> @After
// @Ref http://blog.csdn.net/piaoliuking/article/details/8485940
public class ExpUnittest {
	@Before  //初始化方法任何一个测试执行之前必须执行的代码;
	public void test1() {
		System.out.println("开始初始化----");
	}
//	@Ignore：忽略的测试方法
	@BeforeClass  // 针对所有测试，只执行一次，且必须为public static void
	public static void test11() {
		System.out.println("所有方法调用前要做的事情");
	}

	@Test
	public void add() {
		assertEquals("不相等", "1", "1");
	}

	@Test
	public void add2() {
		System.out.println("测试用例2");
		assertEquals("不相等", "1", "1");
	}

	@Test(expected = IndexOutOfBoundsException.class)
	public void empty() {
		System.out.println("IndexOutOfBoundsException");
		new ArrayList<Object>().get(0);
		//以最直接的方式比较数组：如果数组长度相同，且每个对应的元素相同，则两个数组相等，否则不相等。数组为空的情况也作了考虑
//		public static void assertEquals(Object[] expected, Object[] actual)
//		public static void assertEquals(String message, Object[] expected, Object[] actual)
	}

	@Test(timeout = 1)
	public void infinity() {
		while (true);
	}

	@After
	public void test2() {
		System.out.println("销毁资源----");
	}

	@AfterClass
	public static void test22() {
		System.out.println("所有方法测试完后要调用的");
	}

}

/*  // 运行测试用例
import junit.framework.JUnit4TestAdapter;  
import org.junit.runner.JUnitCore;  
 
public class MyTest3 {  
    public static void main(String[] args) {  
        JUnitCore.runClasses(ExpUnit.class);  
        System.out.println();  
        //MyTest3.suite();  
    }  
 
    public static junit.framework.Test suite() {   
        return new JUnit4TestAdapter(ExpUnit.class);   
    }  
 
}  
*/