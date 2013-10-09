package com.emar.recsys.user.log;

import java.text.ParseException;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class LogNameParseTest {
	
	private static LogNameParse t_obj;
	private static String[][] t_path = new String[][] {
		{ null, "false", "null", "null", null },
		{"/data/stg/s_sw_pv_log/20130826/i_chuchuang_pv_20130826_22.dat",
			"true", "pv", "chuchuang", "22"// plat, type, hour, status
		}, 
		{"/data/stg/x_s_ad_click_log/20130808/1/20/i_yiqifa_click_20130808_20.dat",
			"true", "click", "yiqifa", "20"
		},
		{"/data/stg/x_s_browse_prod_log/20130808/3/20/i_emarbox_goodsview_20130808_20.dat",
			"true", "goodsview", "emarbox", "20"
		},
		{"/data/stg/x_s_adx_request_log/20130908/20/i_rtb_bidreq_20_20130908.dat",
			"true", "bidreq", "rtb", "20"
		}
	};
	
	@BeforeClass
	public static void Init() throws ParseException {
//		t_obj;
	}

	@AfterClass
	public static void Destroy() {
	}
	
	@Test
	public void testfill() {
		for (int i = 0; i < t_path.length; ++i) {
			t_obj = new LogNameParse(t_path[i][0]);
			Assert.assertEquals( t_obj.status+"", t_path[i][1]);
			Assert.assertEquals(t_obj.type+"", t_path[i][2]);
			Assert.assertEquals(t_obj.plat+"", t_path[i][3]);
			Assert.assertEquals(t_obj.hour, t_path[i][4]);
			t_obj.reset();
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
