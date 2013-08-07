package com.emar.recsys.user.util;

import java.io.IOException;
import java.io.StringReader;

import com.webssky.jcseg.core.ADictionary;
import com.webssky.jcseg.core.DictionaryFactory;
import com.webssky.jcseg.core.ISegment;
import com.webssky.jcseg.core.IWord;
import com.webssky.jcseg.core.JcsegException;
import com.webssky.jcseg.core.JcsegTaskConfig;
import com.webssky.jcseg.core.SegmentFactory;

public class WordSegment {

		public ISegment Jcseg = null;
		public static WordSegment wordsegment = null;
		
		public static WordSegment getInstance() throws JcsegException, IOException {
			if(wordsegment == null) {
				wordsegment = new WordSegment();
			}
			return wordsegment;
		}
		private WordSegment() throws JcsegException, IOException {
			
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
			Jcseg = SegmentFactory
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
			Jcseg.reset(new StringReader(str));  // 包装成流
			while ( (word = Jcseg.next()) != null ) {
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
		
		public static void main(String[] args) {
			String s = "去 六段5段 600ml*2+400ml（ 智利进口红提 2kg/箱 2千克,蒙曼说隋:隋文帝杨坚(下)(附DVD光盘1张)   乐扣乐扣(lock&lock)普通型彩色保鲜盒4件套HPL827MPS4G （盒装 瓶装 ）    黄飞红麻辣花生铁罐装180g        OPPLE欧普照明MX450-Y40-5-语薇 吸顶灯-4000K";
			WordSegment ws;
			try {
				ws = WordSegment.getInstance();
				ws.segment(s);
			} catch (Exception e) {
				e.printStackTrace();
			}
			
		}
}
