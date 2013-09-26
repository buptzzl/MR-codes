package com.emar.recsys.user.model;

import static org.junit.Assert.assertArrayEquals;

import org.junit.Test;

import weka.core.Utils;

import com.emar.util.UtilAssert;

public class UtilModelTest {

	@Test
	public void testAdjustSexPredict() {
		double[][][] scores = {
				{null, null},
				{{0.1, 0.2, 0.7}, {0.1, 0.2, 0.7}},
				{{0.023, 0.401, 0.576}, {0.023, 0.977, 0}},
				{{0.2, 0.01, 0.79}, {0.99, 0.01, 0}} 
		};
		
		for (int i = 0; i < scores.length; ++i)
			UtilAssert.assertArrayEquals(scores[i][1], UtilModel.adjustSexPredict(scores[i][0]));
	}
	
	@Test
	public void testAdjustSexPredictArray() {
		double[][][] scores = {
				{null, null, null},
				{null, {0.1, 0.9}, null},
				{{ 0.1, 0.9 }, { 0.8, 0.2}, { 0.7, 0.3}},
				{{ 0.9, 0.1 }, { 0.06, 0.94}, { 0.4, 0.6}}
		};
		for (int i = 0; i < scores.length; ++i)
			UtilAssert.assertArrayEquals(scores[i][2], 
					UtilModel.adjustSexPredict(scores[i][0], scores[i][1]));
	}
	
}
