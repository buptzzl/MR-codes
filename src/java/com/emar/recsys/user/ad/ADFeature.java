package com.emar.recsys.user.ad;

import java.text.ParseException;
import java.util.List;

import com.emar.recsys.user.feature.FeatureType;
import com.emar.util.MateridGWK;

public class ADFeature extends AdCombineExtract {
	private MateridGWK midgwk = MateridGWK.getInstance();
	
	public ADFeature(String ad, List<String> flist) throws ParseException {
		super(ad, flist);
		this.BasicExtract();
	}
	
	private void BasicExtract() {
		flist.add(FeatureType.concat(FeatureType.AD, FeatureType.SEG, this.ad));
		String[] adinfo = midgwk.find(Integer.parseInt(this.ad));
		if(adinfo != null) {
			flist.add(FeatureType.concat(FeatureType.AD, FeatureType.SEG, 
					adinfo[midgwk.idxWide]+adinfo[midgwk.idxHigh]));
			flist.add(FeatureType.concat(FeatureType.AD, FeatureType.SEG,
					adinfo[midgwk.idxKeyword]));
		}
	}
	
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
