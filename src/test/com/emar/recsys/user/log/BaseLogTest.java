package com.emar.recsys.user.log;

import java.text.ParseException;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class BaseLogTest {
	
	private static BaseLog t_obj;
	private static String[] t_fields = new String[]{
		"user_id", "plat_user_id", "user_cookieid", "ip"
	};
	private static String[][] t_data = new String[][] {
		{null, null, null, null, null, null},
		{
				"/data/stg/s_sw_pv_log/20130826/i_chuchuang_pv_20130826_22.dat",
				"BE9X3ApQoCCzqRzTQCTR1QPPyFmJCUBe\u0001Q0dp095QoCCzKT3oKqzzWWlJ03kI1tiz\u0001http://www.shanghaidz.com/dazhe/shenghuo/shishangjiepai/\u0001http://showad.gouwuke.com/windowControler.do?flag=sw&oid=447322&gsid=650535&sn=yigaotongpei.gouwuke.com&winid=26931&modelName=728-90ZY&euid=emar\u0001Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; JianKongBao Monitor 1.1)\u0001106.49.80.3\u000120130826224952\u000126931\u000146866,46868\u0001254,254",
				null, "Q0dp095QoCCzKT3oKqzzWWlJ03kI1tiz", "Q0dp095QoCCzKT3oKqzzWWlJ03kI1tiz", "106.49.80.3"},
		{
				"/data/stg/s_sw_click_log/20130626/i_chuchuang_click_20130626_22.dat",
				"FE8F80F83CD24F9D97A689BE11B41217\u0001g4P3mGzQoCCzq2CqCQ3o7NPC1RCI4Thk\u0001l6iNrGdQoCCzq2CqCQTqbZ1qXA5acVJN\u0001http://p.yigao.com/iframe.jsp?ytc=&ycc=&ybc=&ybco=D9D9D9&ytb=0&ycb=0&ytu=0&ycu=0&yw=300&yh=250&ys=25e86bedba2a2240&ym=78246d4bad93e56c&yu=63699&yz=114514&yp=6&yt=1&ya=1&yc=1&yf=miaozhen&yhc=86366\u0001http://showad.gouwuke.com/windowControler.do?flag=sw&oid=453186&gsid=660256&sn=ccxinmin2.gouwuke.com&winid=10800&modelName=300-250Y&euid=emar\u0001Mozilla/5.0 (iPad; U; CPU OS 3_2_2 like Mac OS X; zh-cn) AppleWebKit/531.21.10 (KHTML, like Gecko) Version/4.0.4 Mobile/7B500 Safari/531.21.10\u0001220.166.219.109\u000120130826220555\u000110800\u000146386\u0001254\u0001http://list.jd.com/1316-1388-5151.html",
				null, "l6iNrGdQoCCzq2CqCQTqbZ1qXA5acVJN", "l6iNrGdQoCCzq2CqCQTqbZ1qXA5acVJN", "220.166.219.109"},
		// plat = 2
		{
				"/data/stg/s_ad_pv_log/20130522/2/i_yigao_pv_20130522_00.dat",
				"2CDDD0A05CCCDE38E0107A48D1F8D0A1\u0001013ad57a-68a1-347f-9e79-0e4f2e3f5504\u0001\u000120130826200000\u0001http://bt.58.com/ershoufang/14616906118018x.shtml\u0001http://bt.58.com/ershoufang/0/e2i19/\u0001Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.31 (KHTML, like Gecko) Chrome/26.0.1410.43 BIDUBrowser/2.x Safari/537.31\u00011\u0001142025\u000186936\u0001128884\u00011377309951096353108495",
				"1377309951096353108495", "013ad57a-68a1-347f-9e79-0e4f2e3f5504", null, ""},
		// plat=1,2 order 中无 2
		{
				"/data/stg/x_s_ad_click_log/20130808/1/20/i_yiqifa_click_20130808_20.dat",
				"5c8b4d0c-7511-4eaf-b39c-f5bfccbee680\u0001\u0001218.21.40.26\u000113759596003735015414\u000120130808195959\u0001254\u0001160\u00011\u0001\u0001\u00011375959604006552536361",
				"1375959604006552536361", "13759596003735015414", null, "218.21.40.26"},
		{
				"/data/stg/x_s_order_log/20130908/1/20/i_yiqifa_order_20130908_20.dat",
				"456c9ee6193911e3849dbc305beee448\u00011378717895452598625141\u0001E00011136786\u00012599790\u0001高中数学(选修系列1合订本1-1\u0001-2人教A版直通高考版)/倍速学习法\u000120.8\u00012\u0001OHG\u0001高考\u000189.4\u0001\u000120130909172021\u0001bookuu.com\u00011378717895452598625141",
				"1378717895452598625141", "1378717895452598625141", null, null},
		// plat = 3. new path model.
		{
				"/data/stg/x_s_browse_log/20130808/3/23/i_emarbox_browse_20130808_23.dat",
				"1375972618015891201674\u0001220.166.216.160\u000120130808231222\u0001http://www.gaojie.com/product/pjchfjqbcbhdcm\u0001http://www.gaojie.com/home/gcceyhffb\u0001高街，gaojie，打折，品牌折扣，限时抢购\u0001simida黑边玫红吊带睡裙39元 （0.9折）- 高街网\u0001立即访问高街网，抢购该商品以及更多低至1折起的知名品牌打折特卖。\u0001Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)\u0001\u0001-1472000\u00011375972618015891201674",
				"1375972618015891201674", "1375972618015891201674", null, "220.166.216.160"},
		{
				"/data/stg/x_s_browse_prod_log/20130808/3/20/i_emarbox_goodsview_20130808_20.dat",
				"1375963111632918045409\u0001183.190.68.219\u000120130808200131\u0001\u0001\u0001PH2011040400162\u0001正品ZIPPO 铁血纪念版 打火机\u0001280.00\u0001PC2012112300016\u0001烟盒打火机\u00010\u00011375963111632918045409",
				"1375963111632918045409", "1375963111632918045409", null, "183.190.68.219"},
		// plat=RTB
		{
				"/data/stg/x_s_adx_request_log/20130908/20/i_rtb_bidreq_20_20130908.dat",
				"5020\u00010AF608780000522C66BF20DC00136593\u0001180.160.46.98\u0001kY4iMGzTZ3E=\u0001kY4iMGzTZ3E=\u0001http://v.youku.com/v_show/id_XMzA4MDM4NDg4.html\u00013:1.000000\u0001\u0001\u0001255",
				null, "kY4iMGzTZ3E=", null, "180.160.46.98"},
				// plat=4
		{
				"/data/stg/x_s_search_log/20130908/4/20/i_gouwuke_search_20130908_20.dat",
				"13786417334541315278\u0001125.58.168.224\u000120130908200213\u0001康帝星\u0001\u0001\u0001\u0001\u0001\u00011378641736617931328882",
				"1378641736617931328882", "13786417334541315278", null, "125.58.168.224"},			
//		{
//				"/data/stg/s_camp_succ_log/20130606/i_dsp_monitor_22_20130606.dat",
//				"522C66BF000BAB760AE2A447AA0003EC\u0001137189894495864279147\u00015010\u00012\u00011\u0001130\u0001\u00011942\u000120544\u00015021\u00013020\u000120130908195959\u0001137189894495864279147\u0001119.36.38.119\u0001Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727)\u0001http://g.hd.baofeng.com/movie/761/list-cateid-1019-ishot-1-sid-1-p-4.shtml\u000114077854\u00010.017857\u0001\u0001\u000120130908200000\u000117857.143000\u00010.100000\u00011\u0001g.hd.baofeng.com\u0001\u00011.0\u0001250X250X2\u00010\u0001null\u0001 ",
//				""} 
	};
	
	@BeforeClass
	public static void Init() throws ParseException {
		t_obj = new BaseLog();
	}
	@AfterClass
	public static void Destroy() {
	}

	@Test
	public void testfill(){}
	
	@Test
	public void testreset() {
		t_obj.reset();
		Assert.assertEquals(t_obj.status, false);
		Assert.assertEquals(t_obj.user_id, null);
		Assert.assertEquals(t_obj.ip, null);
		Assert.assertEquals(t_obj.page_url, null);
	}
	
	@Test
	public void testgetField() {
		Assert.assertEquals(t_obj.getField("user_id"), null);
		Assert.assertEquals(t_obj.getField("isdebug"), false);
	}
	
	@Test (expected=ParseException.class)
	public void testChuPVParse() throws ParseException {
		t_obj.ChuPVParse(t_data[0][1]);
//		for (int i = 2; i < t_fields.length; ++i) 
//			Assert.assertEquals(t_obj.getField(t_fields[i]), t_data[0][i+2]);
		t_obj.reset();
		t_obj.ChuPVParse(t_data[1][1]);
		for (int i = 2; i < t_fields.length; ++i)
			Assert.assertEquals(t_obj.getField(t_fields[i]), t_data[3][i+2]);
		t_obj.reset();
	}
	
	@Test
	public void testChuClickParse() throws ParseException {
		t_obj.ChuClickParse(t_data[2][1]);
		for (int i = 0; i < t_fields.length; ++i)
			Assert.assertEquals(t_obj.getField(t_fields[i]), t_data[2][i+2]);
		t_obj.reset();
	}
	
	@Test
	public void testPVParse() throws ParseException {
		t_obj.PVParse(t_data[3][1]);
		for (int i = 0; i < t_fields.length; ++i)
			Assert.assertEquals(t_obj.getField(t_fields[i]), t_data[3][i+2]);
		t_obj.reset();
	}
	
	@Test
	public void testClickParse() throws ParseException {
		t_obj.ClickParse(t_data[4][1]);
		for (int i = 0; i < t_fields.length; ++i)
			Assert.assertEquals(t_obj.getField(t_fields[i]), t_data[4][i+2]);
		t_obj.reset();
	}
	
	@Test
	public void testOrderParse() throws ParseException {
		t_obj.OrderParse(t_data[5][1]);
		for (int i = 0; i < t_fields.length; ++i)
			Assert.assertEquals(t_obj.getField(t_fields[i]), t_data[5][i+2]);
		t_obj.reset();
	}
	
	@Test
	public void testEmarboxBrowse() throws ParseException {
		t_obj.EmarboxBrowse(t_data[6][1]);
		for (int i = 0; i < t_fields.length; ++i)
			Assert.assertEquals(t_obj.getField(t_fields[i]), t_data[6][i+2]);
		t_obj.reset();
	}
	
	@Test
	public void testEmarboxBProd() throws ParseException {
		t_obj.EmarboxBProd(t_data[7][1]);
		for (int i = 0; i < t_fields.length; ++i)
			Assert.assertEquals(t_obj.getField(t_fields[i]), t_data[7][i+2]);
		t_obj.reset();
	}
	
	@Test
	public void testBidRequest() throws ParseException {
		t_obj.BidRequest(t_data[8][1]);
		for (int i = 0; i < t_fields.length; ++i)
			Assert.assertEquals(t_obj.getField(t_fields[i]), t_data[8][i+2]);
		t_obj.reset();
	}
	
	@Test
	public void testSearch() throws ParseException {
		t_obj.Search(t_data[9][1]);
		for (int i = 0; i < t_fields.length; ++i)
			Assert.assertEquals(t_obj.getField(t_fields[i]), t_data[9][i+2]);
		t_obj.reset();
	}
	
}
