package com.emar.recsys.user.action;

import org.junit.Test;

import junit.framework.Assert;

public class ActionInstancesTest {
	
	private static String []path =  new String[] {
			"../data/test/history_merge.10.view", 
			"../data/test/history_merge.10.view.rest",
			"../data/test/history_instance_in.arff",
	};
	private ActionInstances tObj;
	
	@Test
	public void testinit() {
		Assert.assertEquals(1, tObj.getFeatureSize());
		Assert.assertEquals(1, tObj.getMinActPerUser());
	}
	
	@Test
	public void testBatchFormat() {
		String harffs = "@relation user action\n@attribute ";
		Assert.assertEquals("true", tObj.BatchFormat());
		Assert.assertEquals(tObj.getArffHead().startsWith(harffs), true);
		Assert.assertEquals(tObj.getData(0).startsWith(harffs), true);
		Assert.assertEquals(tObj.getData(0).endsWith("}"), true);
	}
	
	@Test
	public void testformatUserActions() {
		// HOWTO?
	}
	
	@Test
	public void testFilter() {
		// HOWTO
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
