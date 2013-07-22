/**
 * CopyRight (C) 2008-2009 yeyong
 */
package com.emar.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.security.Principal;
import java.util.*;

//import javax.servlet.RequestDispatcher;
//import javax.servlet.ServletInputStream;
//import javax.servlet.http.Cookie;
//import javax.servlet.http.HttpServletRequest;
//import javax.servlet.http.HttpSession;

/**
 * HttpRequestParser工具类, from http://yy629.iteye.com/blog/420432
 * @author yy
 * @date Jun 21, 2009 2:13:25 PM
 * 有删除
 */
public class HttpRequestParser {

  public static Map<String, String[]> getParamsMap(String queryString, String enc) {
    Map<String, String[]> paramsMap = new HashMap<String, String[]>();
    if (queryString != null && queryString.length() > 0) {
      int ampersandIndex, lastAmpersandIndex = 0;
      String subStr, param, value;
      String[] paramPair, values, newValues;
      do {
        ampersandIndex = queryString.indexOf('&', lastAmpersandIndex) + 1;
        if (ampersandIndex > 0) {
          subStr = queryString.substring(lastAmpersandIndex, ampersandIndex - 1);
          lastAmpersandIndex = ampersandIndex;
        } else {
          subStr = queryString.substring(lastAmpersandIndex);
        }
        paramPair = subStr.split("=");
        param = paramPair[0];
        value = paramPair.length == 1 ? "" : paramPair[1];
        try {
          value = URLDecoder.decode(value, enc);
        } catch (UnsupportedEncodingException ignored) {
        }
        if (paramsMap.containsKey(param)) {
          values = paramsMap.get(param);
          int len = values.length;
          newValues = new String[len + 1];
          System.arraycopy(values, 0, newValues, 0, len);
          newValues[len] = value;
        } else {
          newValues = new String[] { value };
        }
        paramsMap.put(param, newValues);
      } while (ampersandIndex > 0);
    }
    return paramsMap;
  }

  public static void main(String[] args) {
	  String s = "http://p.yigao.com/imprImg.jsp?tc=&cc=&bgc=&bc=D9D9D9&tb=0&cb=0&tu=0&cu=0&uid=76060&zid=140012&pid=2&w=728&h=90&t=1&a=1&c=1&sid=9ac3c5bf44420789&ua=Mozilla/4.0%20%28compatible%3B%20MSIE%206.0%3B%20Windows%20NT%205.1%3B%20Trident/5.0%3B%20SLCC2%3B%20.NET%20CLR%202.0.50727%3B%20.NET%20CLR%203.5.30729%3B%20.NET%20CLR%203.0.30729%3B%20Media%20Center%20PC%206.0%3B%20BOIE9%3BZHCN%3B%20SE%202.x%29&n=Microsoft%20Internet%20Explorer&p=http:&v=4.0%20%28compatible%3B%20MSIE%206.0%3B%20Windows%20NT%205.1%3B%20Trident/5.0%3B%20SLCC2%3B%20.NET%20CLR%202.0.50727%3B%20.NET%20CLR%203.5.30729%3B%20.NET%20CLR%203.0.30729%3B%20Media%20Center%20PC%206.0%3B%20BOIE9%3BZHCN%3B%20SE%202.x%29&r=&ho=www.beijingdz.com&l=http%3A//www.beijingdz.com/a/gouwu/quanqiugouwu/&ax=0&ay=0&rx=0&ry=0&os=WinXP&scr=1280_960&ck=true&s=1&ww=1213&wh=824&ym=&fs=1&pvid=888e9ab2ed459ca798c2c31efc72df0b&yhc=&msid=4227d5f89edbe44";
	  String spara = "tc=&cc=&bgc=&bc=D9D9D9&tb=0&cb=0&tu=0&cu=0&uid=76060&zid=140012&pid=2&w=728&h=90&t=1&a=1&c=1&sid=9ac3c5bf44420789&ua=Mozilla/4.0%20%28compatible%3B%20MSIE%206.0%3B%20Windows%20NT%205.1%3B%20Trident/5.0%3B%20SLCC2%3B%20.NET%20CLR%202.0.50727%3B%20.NET%20CLR%203.5.30729%3B%20.NET%20CLR%203.0.30729%3B%20Media%20Center%20PC%206.0%3B%20BOIE9%3BZHCN%3B%20SE%202.x%29&n=Microsoft%20Internet%20Explorer&p=http:&v=4.0%20%28compatible%3B%20MSIE%206.0%3B%20Windows%20NT%205.1%3B%20Trident/5.0%3B%20SLCC2%3B%20.NET%20CLR%202.0.50727%3B%20.NET%20CLR%203.5.30729%3B%20.NET%20CLR%203.0.30729%3B%20Media%20Center%20PC%206.0%3B%20BOIE9%3BZHCN%3B%20SE%202.x%29&r=&ho=www.beijingdz.com&l=http%3A//www.beijingdz.com/a/gouwu/quanqiugouwu/&ax=0&ay=0&rx=0&ry=0&os=WinXP&scr=1280_960&ck=true&s=1&ww=1213&wh=824&ym=&fs=1&pvid=888e9ab2ed459ca798c2c31efc72df0b&yhc=&msid=4227d5f89edbe44";
//	  Request req = HttpRequestParser.parse(s);
//	  System.out.println(req.toString());
  }
}
