package com.emar.recsys.user.item;

import java.util.*;

import com.emar.recsys.user.feature.FeatureType;

/**
 * s_browse_prod_log中的信息抽取
 * @author zhoulm
 *
 */
public class ItemFeature {
	
	/**
	 * @desc  价格特征抽取： 有无小数点， 小数点后|前的数字是否为8|9, 第一个数字，小数点前的位数，后面的位数
	 */
	public static boolean extractFPrice(String price, List<String> flist) {
		// 暂无价格分段[5,10,20,30..100,200,300,500,1000,more]
		try {
			price = price.trim();
			Float.parseFloat(price);
		} catch (Exception e) {
			return false;
		}
		
		final char CHAR = '.'; 
		int pidx = price.indexOf(CHAR);
		flist.add(FeatureType.concat(FeatureType.PRICE, FeatureType.SEG, 
				FeatureType.LEN, FeatureType.SEG, pidx));
		flist.add(FeatureType.concat(FeatureType.PRICE, FeatureType.SEG, 
				FeatureType.LEN, FeatureType.SEG, pidx));
		flist.add(FeatureType.concat(FeatureType.PRICE, FeatureType.SEG,
				price.charAt(0))); // 第一位数字
		if(pidx != -1) {
			flist.add(FeatureType.concat(FeatureType.PRICE, FeatureType.SEG, 
					FeatureType.POINT));
			if(pidx != (price.length() - 1) && price.charAt(pidx+1) > '7') 
				flist.add(FeatureType.concat(FeatureType.PRICE, FeatureType.SEG,
						FeatureType.AFTER, FeatureType.SEG, FeatureType.P89));
			if(pidx != 0 && price.charAt(pidx - 1) > '7') 
				flist.add(FeatureType.concat(FeatureType.PRICE, FeatureType.SEG,
						FeatureType.BEFORE, FeatureType.SEG, FeatureType.P89));
		}
		return true;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
