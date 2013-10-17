package com.emar.recsys.user.util.mr;

/**
 * MR 中通用的采用数组格式表示的 Counter.
 * @author Administrator
 * @Done Unit Test.
 */
public class CounterArray {
	private static final int N_ENUM = 100;

	/** 最多100个索引+一个异常  枚举值的伪Counter数组 */
	public enum EArray{
		
		E_A_00,  E_A_01,  E_A_02,  E_A_03,  E_A_04,  E_A_05,  E_A_06,  E_A_07,  E_A_08,  E_A_09,  E_A_10,  E_A_11,  E_A_12,  E_A_13,  E_A_14,  E_A_15,  E_A_16,  E_A_17,  E_A_18,  E_A_19,  E_A_20,  E_A_21,  E_A_22,  E_A_23,  E_A_24,  E_A_25,  E_A_26,  E_A_27,  E_A_28,  E_A_29,  E_A_30,  E_A_31,  E_A_32,  E_A_33,  E_A_34,  E_A_35,  E_A_36,  E_A_37,  E_A_38,  E_A_39,  E_A_40,  E_A_41,  E_A_42,  E_A_43,  E_A_44,  E_A_45,  E_A_46,  E_A_47,  E_A_48,  E_A_49,  E_A_50,  E_A_51,  E_A_52,  E_A_53,  E_A_54,  E_A_55,  E_A_56,  E_A_57,  E_A_58,  E_A_59,  E_A_60,  E_A_61,  E_A_62,  E_A_63,  E_A_64,  E_A_65,  E_A_66,  E_A_67,  E_A_68,  E_A_69,  E_A_70,  E_A_71,  E_A_72,  E_A_73,  E_A_74,  E_A_75,  E_A_76,  E_A_77,  E_A_78,  E_A_79,  E_A_80,  E_A_81,  E_A_82,  E_A_83,  E_A_84,  E_A_85,  E_A_86,  E_A_87,  E_A_88,  E_A_89,  E_A_90,  E_A_91,  E_A_92,  E_A_93,  E_A_94,  E_A_95,  E_A_96,  E_A_97,  E_A_98,  E_A_99,
		E_A_BAD_INDEX;
		private static int N_MAX = N_ENUM;
		
		public static void setMax(int i) {
			if (0 < i && i < N_ENUM)
				N_MAX = i;
		}
		
		public static EArray getElement(int i) {
			if (i < 0 || N_MAX <= i)
				return EArray.E_A_BAD_INDEX;
			else 
				return EArray.values()[i];
		}
		
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		int [] arr = new int[]{-1, 0, 50, 100, 101};  // unit test.
//		for (int i = 0; i < arr.length; ++i) {
		for (int i = -1; i < 102; ++i) {
			System.out.print(EArray.getElement(i) + " ");
			// OUT: E_A_BAD_INDEX E_A_0 E_A_50 E_A_BAD_INDEX E_A_BAD_INDEX 
			if (i == 100) 
				System.out.println();
		}
	}

}
