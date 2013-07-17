package com.emar.recsys.user.util.itemclassify;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;

public class Parameter {

	public Parameter() {
	}

	public String ChangeBig2Small(String inputstr) {
		String str = "";
		for (int i = 0; i < inputstr.length(); i++) {
			String newstr = inputstr.substring(i, i + 1);
			if (isBigABC(newstr)) {
				str += Big2Small(newstr);
			} else
				str += newstr;
		}
		return str;
	}

	public boolean isNumeric(String str) {
		for (int i = str.length(); --i >= 0;) {

			if (!Character.isDigit(str.charAt(i))) {
				return false;
			}
		}
		return true;
	}

	public boolean isABC(String str) {
		for (int i = str.length(); --i >= 0;) {

			if (Character.isUpperCase(str.charAt(i))) {
				return true;
			}
			if (Character.isLowerCase(str.charAt(i))) {
				return true;
			}

		}
		return false;
	}

	public boolean isBigABC(String str) {
		for (int i = str.length(); --i >= 0;) {

			if (Character.isUpperCase(str.charAt(i))) {
				return true;
			}
		}
		return false;
	}

	public String Big2Small(String str) {
		char c = str.charAt(0);
		if (c < 97) {
			c += 32;
		}
		StringBuffer sb = new StringBuffer();
		sb.append(c);
		String ar = sb.toString();
		return ar;
	}

	public boolean isSmallabc(String str) {
		for (int i = str.length(); --i >= 0;) {
			if (Character.isLowerCase(str.charAt(i))) {
				return true;
			}

		}
		return false;
	}

	public static int subNumpos(String str, int inputpos) {

		int i = inputpos;
		for (; i < str.length(); i++) {
			if (Character.isDigit(str.charAt(i))) {
				continue;
			} else
				break;

		}
		return i;

	}

	public int subABCpos(String str, int inputpos) {

		int i = inputpos;
		for (; i < str.length(); i++) {

			if (Character.isUpperCase(str.charAt(i))) {
				continue;
			} else if (Character.isLowerCase(str.charAt(i))) {
				continue;
			} else
				break;

		}
		return i;

	}

	public String ReplaceSomeStr(String inputstr) {
		String sentence = inputstr;
		int pos0 = -1;
		pos0 = sentence.indexOf("（");
		if (pos0 != -1) {
			sentence = sentence.replaceAll("（", "("); // 清蒸黄骨鱼(两只)
		}
		pos0 = -1;
		pos0 = inputstr.indexOf("）");
		if (pos0 != -1) {
			sentence = sentence.replaceAll("）", ")");
		}

		pos0 = -1;
		pos0 = inputstr.indexOf("：");
		if (pos0 != -1) {
			sentence = sentence.replaceAll("：", ":");
		}
		pos0 = -1;
		pos0 = inputstr.indexOf("；");
		if (pos0 != -1) {
			sentence = sentence.replaceAll("；", ";");
		}
		pos0 = -1;
		pos0 = inputstr.indexOf(" ，");
		if (pos0 != -1) {
			sentence = sentence.replaceAll(" ，", ",");
		}
		pos0 = -1;
		pos0 = inputstr.indexOf("，");
		if (pos0 != -1) {
			sentence = sentence.replaceAll("，", ",");
		}
		pos0 = -1;
		pos0 = inputstr.indexOf("？");
		if (pos0 != -1) {
			sentence = sentence.replaceAll("？", "?");
		}
		pos0 = -1;
		pos0 = inputstr.indexOf("！");
		if (pos0 != -1) {
			sentence = sentence.replaceAll("！", "!");
		}
		pos0 = -1;
		pos0 = inputstr.indexOf("“");
		if (pos0 != -1) {
			sentence = sentence.replaceAll("“", "\"");
		}
		pos0 = -1;
		pos0 = inputstr.indexOf("”");
		if (pos0 != -1) {
			sentence = sentence.replaceAll("”", "\"");
		}

		pos0 = -1;
		pos0 = inputstr.indexOf("＋");
		if (pos0 != -1) {
			sentence = sentence.replaceAll("＋", "+");
		}
		pos0 = -1;
		pos0 = inputstr.indexOf("{");
		if (pos0 != -1) {
			sentence = sentence.replace("{", ",(");
		}
		pos0 = -1;
		pos0 = inputstr.indexOf("}");
		if (pos0 != -1) {
			sentence = sentence.replace("}", "),");
		}

		int pos = -1;
		pos = sentence.indexOf("【全国包邮】");
		if (pos != -1) {
			sentence = sentence.replaceAll("【全国包邮】", "");
		}
		pos = -1;
		pos = sentence.indexOf("【全国】");
		if (pos != -1) {
			sentence = sentence.replaceAll("【全国】", "");
		}

		pos = -1;
		pos = sentence.indexOf("！");
		if (pos != -1) {
			sentence = sentence.replaceAll("！", "");
		}
		pos = -1;
		pos = sentence.indexOf("+++");
		if (pos != -1) {
			sentence = sentence.replace("+++", "");
		}
		pos = -1;
		pos = sentence.indexOf("++");
		if (pos != -1) {
			sentence = sentence.replace("++", "");
		}
		return sentence;
	}

	public String CutSenBySpecSymbol(String sentence, String leftstr,
			String rightstr, Vector<String> vec_posword) {

		int pos = 0;
		int pos0 = -1;
		int pos1 = -1;
		String lastsen = "";

		String presym = leftstr; // "("; //������
		String lastsym = rightstr; // ������

		pos0 = sentence.indexOf(presym, pos);
		while (pos0 != -1) // �ҵ���������
		{
			String newstr = sentence.substring(pos, pos0);
			lastsen += newstr; // ������֮ǰ������ȡ����

			pos1 = sentence.indexOf(lastsym, pos0 + presym.length()); // ������Եĵ�һ��������
			if (pos1 != -1) // �ҵ�����Ե������ţ�����������֮�������ȡ����
			{
				String subsen = sentence
						.substring(pos0 + presym.length(), pos1); // ȡ���������ż������
				vec_posword.add(subsen); // �����ݱ�������
				sentence = sentence.substring(pos1 + lastsym.length());
				pos = 0;// ���µ�λ�ÿ�ʼ����

				pos0 = sentence.indexOf(presym, pos);
			} else
				break;
		}
		lastsen += sentence;
		return lastsen;

	}

	public void EraseZengSong(Vector<String> vec_inputstr,
			Vector<String> vec_result) {
		Vector<String> vec_midstr = new Vector<String>();
		;
		int pos = -1;
		for (int i = 0; i < vec_inputstr.size(); i++) {
			vec_midstr.clear();
			String line = vec_inputstr.elementAt(i);
			pos = -1;
			pos = line.lastIndexOf("+");
			while (pos != -1) {
				String newstr = line.substring(pos);
				if (!IsContainZengSongWord(newstr)) {
					vec_midstr.add(newstr);// ��"+"�ſ�ʼ����β�Ĳ��ֱ�������

				}

				line = line.substring(0, pos); // ��ͷ��ʼ��"+"���м�Ĳ��ֱ�������
				pos = -1;
				pos = line.lastIndexOf("+");

			}
			if (!vec_midstr.isEmpty()) {
				for (int j = vec_midstr.size() - 1; j >= 0; j--) {
					line += vec_midstr.elementAt(j);
				}
			}

			vec_result.add(line);
		}

	}

	public static int JudgePrePos(String inputstr) {

		for (int i = inputstr.length() - 1; i > 0; i--) {

			String str = inputstr.substring(i - 1, i);
			System.out.println("i-1 =" + (i - 1) + " str = " + str);
			if (str.equals(",") || str.equals("!") || str.equals("?")
					|| str.equals(":") || str.equals(";") || str.equals("。")
					|| str.equals(")") || str.equals("、") || str.equals("\"")
					|| str.equals("}")) {
				return i;
			}
		}
		return -1;
	}

	public static int JudgeLastPos(String inputstr, int begpos) {
		for (int i = begpos; i <= inputstr.length() - 1; i++) {

			String str = inputstr.substring(i, i + 1);
			System.out.println("ls   str =" + str);
			if (str.equals(",") || str.equals("!") || str.equals("?")
					|| str.equals(":") || str.equals(";") || str.equals("。")
					|| str.equals("(") || str.equals("、") || str.equals("\"")
					|| str.equals("{")) {
				return i;
			}
		}
		return -1;
	}

	public static String SplitByJiaHao(String inputstr,
			Vector<String> vec_midstr) {

		String reststr = ""; // 提取出”+“号部分后剩下的那部分的内容
		String finalstr = "";
		int pos = -1;
		pos = inputstr.indexOf("+");

		if (pos != -1) // 至少包含一个 加号
		{
			// //找到从"+"前的第一个符号
			String prestr = inputstr.substring(0, pos); // 将从头开始到"+"号之间的内容取出来

			int prepos = JudgePrePos(prestr);
			String newstr = "";

			if (prepos != -1) // 如果加号前面包含有用符号分开的内容，则保存加号前面从头开始到这个符号之间的内容，
			{
				reststr += inputstr.substring(0, prepos); // 将加号之前的非加号好部分保存起来

				newstr = inputstr.substring(prepos); // 后面只要处理从加号前面的那个符号开始到结尾的部分
			} else {
				newstr = inputstr;
			}

			int lastpos = -1;
			int lastpos2 = -1;
			lastpos = newstr.lastIndexOf("+"); // 找到句子中最后一个加号位置

			if (lastpos != -1) // 找到从"+"开始后的第一个符号
			{
				lastpos2 = JudgeLastPos(newstr, lastpos + 1);

				if (lastpos2 != -1) {
					reststr += newstr.substring(lastpos2);

					String midstr = newstr.substring(0, lastpos2);

					vec_midstr.add(midstr);
				} else {

					vec_midstr.add(newstr);
				}
				finalstr = reststr;
			} else
				finalstr = newstr;

		} else {
			finalstr = inputstr; // 一个加号都没有，就返回原来的字符串
		}

		return finalstr;

	}

	public String SearchMaxValue(HashMap<String, Double> hsCateProb) {
		double maxsum = 0;
		String maxcate = "";
		Iterator iter = hsCateProb.keySet().iterator();
		while (iter.hasNext()) {
			Object key = iter.next();
			Object value = hsCateProb.get(key);
			double maxvalue = Double.parseDouble(value.toString());
			if (maxvalue > maxsum) {
				maxsum = maxvalue;
				maxcate = key.toString();
			}
		}

		return maxcate;
	}

	public String CutSentenceBySymbol(String sentence,
			Vector<String> vec_posword) {
		// 将"()"内的句子单独划分出来，这样句子可能有2个部分，也可能有2个以上的部分

		sentence = sentence.replaceAll("（", "(");
		sentence = sentence.replaceAll("）", ")");
		int pos = 0;
		int pos0 = -1;
		int pos1 = -1;
		String lastsen = "";

		String presym = "("; // 左括号
		String lastsym = ")"; // 右括号

		pos0 = sentence.indexOf(presym, pos);
		while (pos0 != -1) // 找到了左括号
		{
			String newstr = sentence.substring(pos, pos0);
			lastsen += newstr; // 左括号之前的内容取出来
			pos1 = sentence.indexOf(lastsym, pos0 + presym.length()); // 再找配对的第一个右括号
			if (pos1 != -1) // 找到了配对的右括号，将两个括号之间的内容取出来
			{

				String subsen = sentence
						.substring(pos0 + presym.length(), pos1); // 取出两个括号间的内容
				vec_posword.add(subsen); // 将内容保存起来
				sentence = sentence.substring(pos1 + lastsym.length());
				pos = 0;// 从新的位置开始查找

				pos0 = sentence.indexOf(presym, pos);
			} else
				break;
		}
		lastsen += sentence;
		return lastsen;

	}

	public static void SplitSentence(String inputstr, Vector<String> vec_result) {
		Vector<String> vec_con = new Vector<String>();
		SplitByStr(inputstr, "。", vec_con);
		int i = 0;
		Vector<String> vec_con2 = new Vector<String>();
		for (i = 0; i < vec_con.size(); i++) {
			SplitByStr(vec_con.elementAt(i), "?", vec_con2);
		}
		Vector<String> vec_con3 = new Vector<String>();
		for (i = 0; i < vec_con2.size(); i++) {
			SplitByStr(vec_con2.elementAt(i), "!", vec_con3);
		}
		Vector<String> vec_con4 = new Vector<String>();
		for (i = 0; i < vec_con3.size(); i++) {
			SplitByStr(vec_con3.elementAt(i), ";", vec_con4);
		}
		Vector<String> vec_con5 = new Vector<String>();
		for (i = 0; i < vec_con4.size(); i++) {
			SplitByStr(vec_con4.elementAt(i), ",", vec_con5);
		}
		for (i = 0; i < vec_con5.size(); i++) {
			SplitByStr(vec_con5.elementAt(i), ":", vec_result);
		}

	}

	public Vector<String> splitsentence(String[] sens) {
		Vector<String> Arraysentence = new Vector<String>();
		String sentence = "";
		int j = 0;
		for (int i = 0; i < sens.length; i++) {
			if (sens[i].length() == 0)
				continue;
			if (sens[i].equals("  "))
				continue;

			if (sens[i].equals("”") || sens[i].equals("\"")) // 如果是引号
			{// 看看前面有没有标点符号
				if (i - 1 > 0
						&& (sens[i - 1].equals("。") || sens[i - 1].equals("！")
								|| sens[i - 1].equals("!")
								|| sens[i - 1].equals("?")
								|| sens[i - 1].equals("﹖") || sens[i - 1]
								.equals("？"))) {
					sentence += sens[i].replaceAll("  ", "");
					Arraysentence.add(sentence);
					sentence = "";

				}
			} else if ((sens[i].equals("。") || sens[i].equals("！")
					|| sens[i].equals("!") || sens[i].equals("?")
					|| sens[i].equals("﹖") || sens[i].equals("？")
					|| sens[i].equals("......") || sens[i].equals("……")
					|| sens[i].equals("，") || sens[i].equals(",")
					|| sens[i].equals("……") || sens[i].equals(";"))
					&& (i + 1 < sens.length && (!sens[i].equals("”") || (!sens[i]
							.equals("\""))))) {
				sentence += sens[i];
				Arraysentence.add(sentence);
				sentence = "";
			} else {
				sentence += sens[i].replaceAll("  ", "");
			}
		}
		if (!sentence.equals("") && (!sentence.equals("  "))) {
			Arraysentence.add(sentence);
		}
		return Arraysentence;
	}

	public static void SplitByStr(String inputstr, String sepstr,
			Vector<String> vec_con) {
		// Vector<String> vec_con = new Vector<String>();
		int pos = 0;
		int pos0 = -1;
		pos0 = inputstr.indexOf(sepstr, pos);
		while (pos0 != -1) {
			String newstr = inputstr.substring(pos, pos0 + sepstr.length());
			vec_con.add(newstr);
			pos = pos0 + sepstr.length();
			pos0 = inputstr.indexOf(sepstr, pos);
		}
		String newstr0 = inputstr.substring(pos);
		vec_con.add(newstr0);
	}

	public boolean isQuanJiao(String str) {
		if (str.getBytes().length == str.length()) {

			return false;
		}
		if (str.getBytes().length == str.length() * 2) {
			return true;

		}
		return false;

	}

	public static boolean isOrderWord(String str) {
		if (str.equals("第"))
			return true;
		return false;
	}

	public static int subNumpos2(String str, int inputpos) {

		int i = inputpos;
		for (; i < str.length(); i++) {
			if (Character.isDigit(str.charAt(i))) {
				continue;
			} else {
				if (i + 1 < str.length()) {
					String newstr = str.substring(i, i + 1);
					if (newstr.equals("%") || newstr.equals("％")) {
						return i + 1;
					} else
						break;

				}

			}

		}
		return i;
	}

	public Boolean IsContainZengSongWord(String inputstr) {
		int pos = -1;
		pos = inputstr.indexOf("免费");
		int pos1 = -1;
		pos1 = inputstr.indexOf("赠送");
		int pos2 = -1;
		pos2 = inputstr.indexOf("独立");
		if (pos != -1 || pos1 != -1 || pos2 != -1)
			return true;
		return false;

	}

}
