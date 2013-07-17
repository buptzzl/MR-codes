package com.emar.recsys.user.ad;

import java.text.ParseException;
import java.util.*;

import com.emar.recsys.user.feature.FeatureType;


public class AdCombineExtract extends ADBase {
	
	public AdCombineExtract(String url, List<String> flist) throws ParseException {
		super(url, flist);
	}
	
	public void CombADUrl(String host, String level, String type) {
		if (host != null) {
			this.flist.add(FeatureType
					.concat(FeatureType.COMB, FeatureType.SEG, FeatureType.AD,
							FeatureType.SEG, this.ad, FeatureType.SEG,
							FeatureType.MEDHOST, FeatureType.SEG, host));
		}
		if (level != null) {
			this.flist.add(FeatureType.concat(FeatureType.COMB,
					FeatureType.SEG, FeatureType.AD, FeatureType.SEG, this.ad,
					FeatureType.SEG, FeatureType.MEDHOST, FeatureType.SEG,
					level));
		}
		if (type != null) {
			this.flist.add(FeatureType.concat(FeatureType.COMB,
					FeatureType.SEG, FeatureType.AD, FeatureType.SEG, this.ad,
					FeatureType.SEG, FeatureType.MEDHOST, FeatureType.SEG,
					type));
		}
	}

	public void CombADTime(List<String> ftime) {
		if (ftime == null) {
			return;
		}
		for (String s : ftime) {
			this.flist
					.add(FeatureType.concat(FeatureType.COMB, FeatureType.SEG,
							FeatureType.AD, FeatureType.SEG, this.ad,
							FeatureType.SEG, FeatureType.CDATETIME,
							FeatureType.SEG, s));
		}
	}

	public void CombADPos(String posid) {
		if (posid == null) {
			return;
		}
		this.flist.add(FeatureType.concat(FeatureType.COMB, FeatureType.SEG,
				FeatureType.AD, FeatureType.SEG, this.ad, FeatureType.SEG,
				FeatureType.POS, FeatureType.SEG, posid));
	}

	public void CombADZone(List<String> fzone) {
		if (fzone == null) {
			return;
		}
		for (String s : fzone) {
			this.flist.add(FeatureType.concat(FeatureType.COMB,
					FeatureType.SEG, FeatureType.AD, FeatureType.SEG, this.ad,
					FeatureType.SEG, FeatureType.IP, s));
		}
	}

	public void CombADDevice(List<String> fdevice) {
		if (fdevice == null) {
			return;
		}
		for (String s : fdevice) {
			this.flist.add(FeatureType.concat(FeatureType.COMB,
					FeatureType.SEG, FeatureType.AD, FeatureType.SEG, this.ad,
					FeatureType.SEG, FeatureType.GDEVICE, s));
		}
	}

	public void CombADSPONSORPos(String posid) {
		// TODO
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
