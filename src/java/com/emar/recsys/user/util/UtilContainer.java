package com.emar.recsys.user.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;

/**
 * 对通用容器（数组、Map、Set）的常用操作
 * @author zhoulm
 *
 */
public class UtilContainer {
	

	
	// 返回最大的K个分值, 基于优先队列实现，不保证TOP有序
	public static <C extends Comparable> List<C> TopK(List<C> inidx, int k) {
		if(inidx == null || inidx.size() == 0) {
			return null;
		} else if(inidx.size() <= k) {
			return inidx;
		}
		
		int iidx = inidx.size() - 1;
			Collections.sort(inidx);
			List<C> res = new ArrayList<C>(k);
			for(int i = 0; i < k; ++i) {
				res.add(inidx.get(iidx - i));
			}
			return res;
	}


	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
