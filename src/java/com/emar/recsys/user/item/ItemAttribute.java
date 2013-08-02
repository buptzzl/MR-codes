package com.emar.recsys.user.item;

import java.text.ParseException;
import java.util.*;
import java.util.regex.Pattern;

import com.emar.recsys.user.feature.FeatureType;
import com.emar.recsys.user.util.IUserException;
import com.emar.recsys.user.util.UtilStr;

/**
 * TODO 查看TB 商品名挖掘流程， 实现商品名切分 
 * item属性： 属性有对应的优先级别；每识别一个属性，对应的内容即被删除
 * @author zhoulm
 * 
 */
public class ItemAttribute {
	private static final Pattern cpatt = Pattern.compile("\\(|\\)|（|）|\\[|\\]|【|】| |\t|，|、|；|。|！|？|,|;|!");
	private static final String FPrefix = String.format("%s%s%s", FeatureType.AD, FeatureType.SEG,FeatureType.TITLE);
	
//	private final List<String> wseg = new ArrayList<String>(10);
	private List<String> sword = new ArrayList<String>(100);
	private String item;
	private List<String> attributes;
	
	public ItemAttribute(String item) throws ParseException {
		if (item == null || item.trim().length() == 0) {
			throw new ParseException("initial ItemAttribute with NULL", IUserException.ErrADItemNull);
		}
		this.item = item;
	}
	
	public List<String> getAttribute() {
		this.ItemSegment();
		
		this.ItemShop();
		this.ItemCategory();
		this.ItemPattern();
		this.ItemProducer();
		this.ItemSize();
		this.ItemBaseInfo();
		return this.attributes;
	}
	
	private void ItemSegment() {
		//TODO 商品标题粗切分, 并分词
		// 分隔符为： 空格、成对的 括号、非数字与英文  
		// 两个字、 三个字中末尾为 装 等不再分词，
		String[] atom = cpatt.split(this.item);
//		wseg.clear();
		
		if(atom != null) {
			this.attributes.add(FeatureType.concat(FPrefix, FeatureType.SEG, FeatureType.BRACKET));
			this.attributes.add(FeatureType.concat(FPrefix, FeatureType.SEG, FeatureType.BRACKET, FeatureType.SEG, atom.length/2));
			for(String s: atom) {
				if(s.length() > 2) {
					// 1个汉字后缀 or 无汉字
					char c;
					int i = 0;
					while(i < s.length() - 1) {
						c = s.charAt(i++);
						if(UtilStr.isChinese(c)) {
							break;
						}
					}
//					if(i != (s.length() - 1))
//						wseg.add(s);  // 待分词
				}
			}
		}
	}
	
	private boolean ItemCategory() {
		// TODO 产品类目  根据商品分类器识别  
		// BOOK 书名： 出版、标准|手册|书|著|词|考|、修订、最新版、N版、N册、编、阅读、人文、教、辅
		return true;
	}
	
	private boolean ItemProducer() {
		// TODO 厂家， 品牌PINPAI 根据商品分类器识别  
		// 空格分隔开的全英文 为品牌
		return true;
	}
	
	private boolean ItemPattern() {
		// TODO sex,time,功效,产地,制材,颜色,外观特征
		// 家用 
		
		return true;
	}

	
	private boolean ItemShop() {
		// TODO 商家：优惠、资质、活动、服务 
		// 日销千件， 送耳机，冲三冠， 积分，换购；
		// 大陆行货, 进口
		
		return true;
	}	
	
	private boolean ItemShipper() {
		// TODO 邮递信息
		// 顺丰8折
		return true;
	}
	
	private boolean ItemNumber() {
		// 货号
		return true;
	}
	
	private boolean ItemBaseInfo() {
		// TODO 宣传词： 款式、质量、价格
		return true;
	}
	
	private boolean ItemSize() {
		// TODO 容量、体积、重量、价格、尺码
		// 16升
		String[] size = { "罐", "桶", "袋", "箱", "盒" }; /// sufix: 装 2gram
		String[] sunion = {"g", "kg", "l", "ml", "克", "千克", "升", "毫升", "米"}; 
		
		return true;
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
