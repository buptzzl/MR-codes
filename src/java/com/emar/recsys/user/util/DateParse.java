package com.emar.recsys.user.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import junit.framework.Assert;

public class DateParse {

	public static final int SUN_ID = 7, SAT_ID = 1; // week id.
	public static final int WEEKEND = 0, WEEKDAY = 1;

	public static enum TimeSlice {
		MORN("morning", 8, 12), AFTER("afternoon", 12, 18), NIGHT("night", 18,
				23), MNIGHT("midnight", 23, -1);

		private String name;
		private int index;
		private int last;

		private TimeSlice(String name, int index, int last) {
			this.name = name;
			this.index = index;
			this.last = last;
		}

		public static Integer getTimeSlice(int index) {
			if (index < 0 || 23 < index) {
				return null;
			}
			for (TimeSlice b : TimeSlice.values()) {
				if (b.index <= index && index < b.last) {
					return b.getIndex();
				}
			}
			return TimeSlice.MNIGHT.getIndex();
		}

		public static String getName(int index) {
			for (TimeSlice b : TimeSlice.values()) {
				if (b.getIndex() == index) {
					return b.name;
				}
			}
			return null;
		}

		public String getName() {
			return name;
		}

		public void setName() {
			this.name = name;
		}

		public int getIndex() {
			return index;
		}

		public void setIndex() {
			this.index = index;
		}

		public String toString() {
			return this.index + "_" + this.name;
		}
	};

	/**
	 * 返回时间的 {week,hour} 
	 */
	public static int[] getWeekDayHour(String dstr, String dfmt) {
		if (dstr == null) {
			return null;
		}

		Calendar c = Calendar.getInstance();
		SimpleDateFormat dateformat = new SimpleDateFormat(dfmt); // ("yyyyMMdd HH:mm:ss");
		int week, hour; // ��Ч��Ĭ��ֵ\
		int[] res = null;
		Date d;
		try {
			d = dateformat.parse(dstr);
			c.setTime(d);
			week = c.get(Calendar.DAY_OF_WEEK);
			hour = c.get(Calendar.HOUR_OF_DAY);
			// hour = TimeSlice.getTimeSlice(c.get(Calendar.HOUR_OF_DAY));

			res = new int[] { week, hour };
		} catch (ParseException e) {
		} // DateTime
		return res;
	}

	/**
	 * merge day in [SAT, SUN] and hour[]
	 * 
	 * @param dstr
	 * @param dfmt
	 * @return
	 */
	public static int[] getWeekHour(String dstr, String dfmt) {
		int[] weekdayHour = DateParse.getWeekDayHour(dstr, dfmt);
		if (weekdayHour == null) {
			return null;
		}

		if (weekdayHour[0] == DateParse.SUN_ID
				|| weekdayHour[0] == DateParse.SAT_ID) {
			weekdayHour[0] = DateParse.WEEKEND; // weekend.
		} else {
			weekdayHour[0] = DateParse.WEEKDAY;
		}

		// merge hour.
		weekdayHour[1] = TimeSlice.getTimeSlice(weekdayHour[1]);
		return weekdayHour;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String[] t = { "20130616 17:12:12", "yyyyMMdd HH:mm:ss" };
		String[] t2 = { "20130616", "yyyyMMdd" };
		int[] wd_info = DateParse.getWeekHour(t2[0], t2[1]);
		if (wd_info != null) {
			System.out.print("[test] weekend=" + wd_info[0] + "\thour="
					+ wd_info[1]);
		}
		Assert.assertEquals(DateParse.getWeekHour("20130", "yyyy"), null);
//		Assert.assertEquals(DateParse.getWeekHour("20130616", "yyyyMMdd"), null);
		Assert.assertEquals(DateParse.getWeekHour("20130616 17:12:12", "yyyyMMdd HH:mm:ss"), 
				new int[]{0,12});
		
	}

}
