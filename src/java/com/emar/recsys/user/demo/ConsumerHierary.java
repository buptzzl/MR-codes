package com.emar.recsys.user.demo;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import com.emar.recsys.user.util.UtilMath;

/**
 * 将数据集按大小分为4层.（1 用于用户消费能力分层）
 * 
 * @author zhoulm
 * @Done UT, 
 */
public class ConsumerHierary implements IHierarchy {
	private TreeSet prices;
	private int L_Max; // 最大层次数
	private final float[] P_TOP = new float[]{ 0.1f, 0.25f, 0.75f, 0.92f, 1.0f};  

	public ConsumerHierary() {
		prices = new TreeSet<Float>();
	}

	@Override
	public boolean update(Object obj) {
		if (obj == null)
			return false;
		boolean flag = false;
		try {
			Float score = (float) UtilMath.getNumbers(obj);
			if (!prices.contains(score))
				flag = prices.add((float)score);  // 支持 字符串形式的小数 
		} catch (Exception  e) {
			flag = false;
		}
		return flag;
	}

	@Override
	public boolean setMaxLayer(Object obj) {
//		if (obj == null) 
		return false;  // 不支持修改。
		/*
		boolean flag = false;
		try {
			int lmax = (Integer) obj;
			if (lmax >= L_Max) {
				flag = true;
				L_Max = lmax;
			}
		} catch (ClassCastException e) {
			flag = false;
		}
		*/
//		return flag;
	}

	@Override
	public int getMaxLayer() {
		return P_TOP.length;
	}

	@Override
	public int getLayer(Object obj) {
		// 小于10个原子数据时直接返回0(不分层). 否则按
		int level = -1;
		if (obj == null) 
			return level;
		
		try {
			float s_threld;
			double score = UtilMath.getNumbers(obj);
			
			int sz = prices.size();
			if (sz <= 10) 
				return 0;
			
			int threld, idx = 0, N = 0;
			threld = (int) (sz * P_TOP[idx]);
			Iterator itr = prices.iterator();
			while (itr.hasNext()) {
				s_threld = (Float)itr.next();
				N ++;
				if (threld < N) {
					idx ++;
					threld = (int) (sz * P_TOP[idx]);
				}
				
				if (score <= s_threld || N == sz) {
					level = idx + 1;
					break;
				}
			}
		} catch (Exception e) {
			level = -1;
		}
		
		return level;
	}

	@Override
	public int getDataSize() {
		return prices.size();
	}

	@Override
	public boolean delete(Object obj) {
		if (obj == null)
			return false;
		boolean flag = false;
		try {
			Float score = (float) UtilMath.getNumbers(obj);
			if (prices.contains(score)) {
				flag = prices.remove(score);  // 支持 字符串形式的小数 
				flag = true;
			}
		} catch (Exception  e) {
		}
		return flag;
	}
	
	@Override
	public String toString() {
		return String.format("ConsumerHierary[%s]",	this.prices.toString());
	}

}
