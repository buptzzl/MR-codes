package com.emar.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;

import com.chenlb.mmseg4j.ComplexSeg;
import com.chenlb.mmseg4j.Dictionary;
import com.chenlb.mmseg4j.MMSeg;
import com.chenlb.mmseg4j.MaxWordSeg;
import com.chenlb.mmseg4j.Seg;
import com.chenlb.mmseg4j.Word;

import com.webssky.jcseg.core.ADictionary;
import com.webssky.jcseg.core.DictionaryFactory;
import com.webssky.jcseg.core.ISegment;
import com.webssky.jcseg.core.IWord;
import com.webssky.jcseg.core.JcsegException;
import com.webssky.jcseg.core.SegmentFactory;
import com.webssky.jcseg.core.JcsegTaskConfig;

public class TestWS {
	
	public static class Complex {

		protected Dictionary dic;
		
		public Complex() {
			dic = Dictionary.getInstance();
			System.out.println("[Info] Complex::init DICT-PATH=" + dic.getDicPath());
		}

		protected Seg getSeg() {
			return new ComplexSeg(dic);
		}
		
		public String segWords(Reader input, String wordSpilt) throws IOException {
			StringBuilder sb = new StringBuilder();
			Seg seg = getSeg();	//取得不同的分词具体算法
			MMSeg mmSeg = new MMSeg(input, seg);
			Word word = null;
			boolean first = true;
			while((word=mmSeg.next())!=null) {
				if(!first) {
					sb.append(wordSpilt);
				}
				String w = word.getString();
				sb.append(w);
				first = false;
				
			}
			return sb.toString();
		}
		
		public String segWords(String txt, String wordSpilt) throws IOException {
			return segWords(new StringReader(txt), wordSpilt);
		}
		
		private void printlnHelp() {
			System.out.println("\n\t-- 说明: 输入 QUIT 或 EXIT 退出");
	        System.out.print("\nmmseg4j-"+this.getClass().getSimpleName().toLowerCase()+">");
		}
		
		protected void run(String[] args) throws IOException {
			String txt = "京华时报２００８年1月23日报道 昨天，受一股来自中西伯利亚的强冷空气影响，本市出现大风降温天气，白天最高气温只有零下7摄氏度，同时伴有6到7级的偏北风。";
			
			if(args.length > 0) {
				txt = args[0];
			}
			
			System.out.println(segWords(txt, " | "));
			printlnHelp();
			String inputStr = null;
			System.out.println(segWords(txt, " | "));    //分词
			System.out.print("\n<mmseg4j-"+this.getClass().getSimpleName().toLowerCase()+">");
			/*
	        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
	        while((inputStr = br.readLine()) != null) {
	            if(inputStr.equals("QUIT") || inputStr.equals("EXIT")) {
	                System.exit(0);
	            } else if("".equals(inputStr)) {
	            	printlnHelp();
	            } else {
	            	//System.out.println(inputStr);
	            	System.out.println(segWords(inputStr, " | "));    //分词
	            	System.out.print("\nmmseg4j-"+this.getClass().getSimpleName().toLowerCase()+">");
	            }
	        }
	        */
		}
		
		public static void main(String[] args) throws IOException {
			
			new Complex().run(args);
		}

	}

	public static class MaxWord extends Complex {

		protected Seg getSeg() {

			return new MaxWordSeg(dic);
		}

		public static void main(String[] args) throws IOException {
			new MaxWord().run(args);
		}
	}

	public static class WordSegmentJcseg {
		
		public ISegment seg = null;
		
		public WordSegmentJcseg() throws JcsegException, IOException {
			
			JcsegTaskConfig config = new JcsegTaskConfig();
			// 指定文件的路径
			//JcsegTaskConfig config = new JcsegTaskConfig("/java/JavaSE/jcseg/jcseg.properties"); 
			//JcsegTaskConfig config = new JcsegTaskConfig(null);
			//reset the options from a property file.
			//config.resetFromPropertyFile("/java/JavaSE/jcseg/jcseg.properties");
			
			// 基于配置文件创建词典对象， isAutoload()决定是否同步更新词库
			ADictionary dic = DictionaryFactory.createDefaultDictionary(config);
			//two ways to load lexicons
			//dic.loadFromLexiconDirectory(config.getLexiconPath());
			//dic.loadFromLexiconFile("/java/lex-main.lex");
			
			// SIMPLE_MODE 与 COMPLEX_MODE,
			seg = SegmentFactory
					.createJcseg(JcsegTaskConfig.COMPLEX_MODE, new Object[]{config, dic});
			
			//append pinyin
			//config.setAppendCJKPinyin(true);

		}
		
		public void segment(String str) throws IOException {
			
			StringBuffer sb = new StringBuffer();
			//seg.setLastRule(null);
			IWord word = null;
			long _start = System.nanoTime();
			boolean isFirst = true;
			int counter = 0;
			seg.reset(new StringReader(str));  // 包装成流
			while ( (word = seg.next()) != null ) {
				word.getFrequency(); word.getLength(); 
				//word.getPartSpeech(); 词性 POS word.getSyn(); 同义词
				//word.getType(); 词的类型【普通词、单位、姓氏等】 word.getPinyin(); word.getPosition(); 词的起点位置
				if ( isFirst ) {
					sb.append(word.getValue());
					isFirst = false;
				} else {
					String[] pos = word.getPartSpeech();
					sb.append("\t");
					sb.append(word.getValue() + " " +word.getFrequency()
							+ " " + (pos != null ? pos[0]: "#"));
				}
				//clear the allocations of the word.
				word = null;
				counter++;
			}
			long e = System.nanoTime() - _start;
			System.out.println("Jcsegtest 分词结果：\n" + sb.toString() + "\ntime-token(ms):" + e/1000);
		}
		
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws JcsegException 
	 */
	public static void main(String[] args) throws IOException, JcsegException {
		String[] txt = {
				"享我买价28元的雀巢威化(奶香味 盒装480g)1份！包装精美，随身携带的精巧美味，潮流达人的必备零食！美味丝滑的雀巢牛奶巧克力，包裹着层层酥脆威化，美味让你停不了口！",
				"南极人高档竹纤维无痕女式内裤 纯色平角女式舒适女内裤买3送一",
				"京华时报２００８年1月23日报道 昨天，受一股来自中西伯利亚的强冷空气影响，本市出现大风降温天气，白天最高气温只有零下7摄氏度，同时伴有6到7级的偏北风。",
				"智利进口红提 2kg/箱,越南火龙果 3.5kg/箱,越南火龙果1只装,蓝莓125g/盒*2,玉菇甜瓜3-4只装(约4kg),珍珠虾仁250g,新西兰绿色奇异果6粒装,野生小黄鱼450g,品裕农庄西兰花250g,南翔烧卖160g,法式咖啡（摩卡）250ML,品裕农庄牛心菜500g"
		};
		
		WordSegmentJcseg jcsDemo = new WordSegmentJcseg();
		jcsDemo.segment(txt[1]);
		
		
		new Complex().run(txt);
		txt[0] = txt[1];
		new MaxWord().main(txt);
		
	}

}
