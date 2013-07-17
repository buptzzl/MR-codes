package com.emar.recsys.user.feature;

public class FeatureType {

	public static final String SEG = "_";

    /**
     * strong compound type
     */
    public static final String USER = "UID";

    public static final String IP = "IP";

    public static final String AD = "AD";

    public static final String QUERY = "QUERY";

    public static final String URL = "URL";

    public static final String TITLE = "TITLE";
    public static final String TOPIC = "TOPIC";
    public static final String KEYWORD = "KEYWORD";
    public static final String TERM = "TERM";

    public static final String HIT = "HIT";

    public static final String UNKNOWN = "UNKNOWN";
    public static final String LEN = "LEN";

    public static final String CHI = "CHI";
    public static final String ENG = "ENG";

    public static final String COMCN = "COMCN";
    public static final String COM = "COM";
    public static final String CN = "CN";
    public static final String NET = "NET";
    public static final String ORG = "ORG";
    
    public static final String MEDHOST = "MEDHOST";  // 媒体前缀
    public static final String MEDURLMID = "MEDURLMID";
    public static final String MEDURLLEAF = "MEDURLLEAF";
    public static final int MEDULRMID_SZ = 2;
    public static final int HOSTLEN = 3; 
    public static final int HOSTDOTMX = 5;
    public static final int DOMAINCHILDLEN = 3;  /// 最多三级类目
    
    public static final String GEN = "GEN"; // 通用特征前缀
    public static final String GBROWSE = "BROWSE";
    public static final String GOS = "OS";
    public static final String GDEVICE = "DEVICE";
    public static final String GWEEK = "WEEK";
    public static final String GHOUR = "HOUR";
    public static final String GIP = "GIP";
    
    public static final int HOSTLEAFSTEP = 4;
    public static final int HOSTLEAFSTEPMX = 5;
    
    public static final String COMB = "COMB";
    public static final String CDATETIME = "DTIME";
    /**
     * punctuation
     */
    public static final String EXCLAM = "!";
    public static final String COMMA = ",";
    public static final String QUESTION = "?";
    public static final String DASH = "DASH";
    public static final String NUMBER = "NUM";
    public static final String BOOK = "BOOK";
    public static final String QUOTE = "QUOTE";
    public static final String BLANK = "BLANK";
    public static final String PARENTH = "()";
    public static final String BRACKET = "[]";
    public static final String PHONE = "PHONE";
    public static final String MONEY = "$";

    /**
     * All below are weak compound types
     */
    public static final String KEYFROM = "KEYFROM";

    public static final String INTERCEPT = "INTERCEPT";

    public static final String ADTYPE = "ADTYPE";
    public static final String ADSIGN = "ADSIGN";
    public static final String ADVARIANT = "VID";
    public static final String ADGROUP = "GID";
    public static final String SPONSOR = "SPID";
    public static final String INDUSTRY = "IID";

    public static final String WILDCARD = "{关键词}";

    public static final String CPC = "CPC";

    public static final String SYNDICATION = "SYNDID";

    public static final String POS = "POS";
    public static final String HPOS = "HPOS";
    public static final String VPOS = "VPOS";

    public static final String LOCATION = "LOCATION";
    public static final String ABROAD = "ABROAD";
    public static final String PROVINCE = "PROVINCE";
    public static final String CITY = "CITY";
    public static final String IP_EDU = "IPEDU";

    /**
     * time
     */
    public static final String MONTH = "MONTH";
    public static final String DATE = "DATE";
    public static final String WEEKDAY = "WEEKDAY";
    public static final String WEEKEND = "WEEKEND";
    public static final String FESTIVAL = "FESTIVAL";
    public static final String HOUR = "HOUR";

    public static String concat(Object... objects) {
        StringBuilder builder = new StringBuilder();
        for (Object obj: objects) {
            builder.append(obj);
        }
        return builder.toString();
    }
    
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
