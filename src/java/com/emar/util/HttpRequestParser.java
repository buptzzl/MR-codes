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

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletInputStream;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

/**
 * HttpRequestParser工具类, from 网络
 * @author yy
 * @date Jun 21, 2009 2:13:25 PM
 */
public class HttpRequestParser {
  /**
   * 分析url字符串,采用utf-8解码
   * @param urlString
   * @return
   */
  public static Request parse(String urlString) {
    return parse(urlString, "utf-8");
  }

  /**
   * 分析url字符串,指定字符集进行解码
   * @param urlString
   * @param enc
   * @return
   */
  public static Request parse(String urlString, String enc) {
    if (urlString == null || urlString.length() == 0) {
      return new Request();
    }
    int questIndex = urlString.indexOf('?');
    if (questIndex == -1) {
      return new Request(urlString);
    }
    String url = urlString.substring(0, questIndex);
    String queryString = urlString.substring(questIndex + 1, urlString.length());
    return new Request(url, getParamsMap(queryString, enc));
  }

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

  /**
   * 请求对象
   * @author yy
   * @date Jun 21, 2009 2:17:31 PM
   */
  public static class Request implements HttpServletRequest {
    private String requestURI;
    private Map<String, String[]> parameterMap;

    Request() {
      this("");
    }

    Request(String requestURI) {
      this.requestURI = requestURI;
      parameterMap = new HashMap<String, String[]>();
    }

    Request(String requestURI, Map<String, String[]> parameterMap) {
      this.requestURI = requestURI;
      this.parameterMap = parameterMap;
    }

    /**
     * 获得指定名称的参数
     * @param name
     * @return
     */
    public String getParameter(String name) {
      String[] values = parameterMap.get(name);
      if (values != null && values.length > 0) {
        return values[0];
      }
      return null;
    }

    /**
     * 获得所有的参数名称
     * @return
     */
    public Enumeration<String> getParameterNames() {
      return Collections.enumeration(parameterMap.keySet());
    }

    /**
     * 获得指定名称的参数值(多个)
     * @param name
     * @return
     */
    public String[] getParameterValues(String name) {
      return parameterMap.get(name);
    }

    /**
     * 获得请求的url地址
     * @return
     */
    public String getRequestURI() {
      return requestURI;
    }

    /**
     * 获得 参数-值Map
     * @return
     */
    public Map<String, String[]> getParameterMap() {
      return parameterMap;
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder();
      buf.append("{");
      buf.append("\n  url = ").append(this.requestURI);
      buf.append("\n  paramsMap = {");
      if (this.parameterMap.size() > 0) {
        for (Map.Entry<String, String[]> e : this.parameterMap.entrySet()) {
          buf.append(e.getKey()).append("=").append(Arrays.toString(e.getValue())).append(",");
        }
        buf.deleteCharAt(buf.length() - 1);
      }
      buf.append("}\n}");
      return buf.toString();
    }
   //  剩下的函数,自己根据需求实现了,一般返回0或者null即可
   //  这里就不贴了,HttpServletRequest的接口方法也太多了

	@Override
	public Object getAttribute(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Enumeration getAttributeNames() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getCharacterEncoding() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getContentLength() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getContentType() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ServletInputStream getInputStream() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getLocalAddr() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getLocalName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getLocalPort() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Locale getLocale() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Enumeration getLocales() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getProtocol() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BufferedReader getReader() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getRealPath(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getRemoteAddr() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getRemoteHost() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getRemotePort() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public RequestDispatcher getRequestDispatcher(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getScheme() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getServerName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getServerPort() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean isSecure() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void removeAttribute(String arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setAttribute(String arg0, Object arg1) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setCharacterEncoding(String arg0)
			throws UnsupportedEncodingException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String getAuthType() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getContextPath() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Cookie[] getCookies() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getDateHeader(String arg0) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getHeader(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Enumeration getHeaderNames() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Enumeration getHeaders(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getIntHeader(String arg0) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getMethod() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getPathInfo() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getPathTranslated() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getQueryString() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getRemoteUser() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public StringBuffer getRequestURL() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getRequestedSessionId() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getServletPath() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public HttpSession getSession() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public HttpSession getSession(boolean arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Principal getUserPrincipal() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isRequestedSessionIdFromCookie() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isRequestedSessionIdFromURL() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isRequestedSessionIdFromUrl() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isRequestedSessionIdValid() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isUserInRole(String arg0) {
		// TODO Auto-generated method stub
		return false;
	}
  }
  
  public static void main(String[] args) {
	  String s = "http://p.yigao.com/imprImg.jsp?tc=&cc=&bgc=&bc=D9D9D9&tb=0&cb=0&tu=0&cu=0&uid=76060&zid=140012&pid=2&w=728&h=90&t=1&a=1&c=1&sid=9ac3c5bf44420789&ua=Mozilla/4.0%20%28compatible%3B%20MSIE%206.0%3B%20Windows%20NT%205.1%3B%20Trident/5.0%3B%20SLCC2%3B%20.NET%20CLR%202.0.50727%3B%20.NET%20CLR%203.5.30729%3B%20.NET%20CLR%203.0.30729%3B%20Media%20Center%20PC%206.0%3B%20BOIE9%3BZHCN%3B%20SE%202.x%29&n=Microsoft%20Internet%20Explorer&p=http:&v=4.0%20%28compatible%3B%20MSIE%206.0%3B%20Windows%20NT%205.1%3B%20Trident/5.0%3B%20SLCC2%3B%20.NET%20CLR%202.0.50727%3B%20.NET%20CLR%203.5.30729%3B%20.NET%20CLR%203.0.30729%3B%20Media%20Center%20PC%206.0%3B%20BOIE9%3BZHCN%3B%20SE%202.x%29&r=&ho=www.beijingdz.com&l=http%3A//www.beijingdz.com/a/gouwu/quanqiugouwu/&ax=0&ay=0&rx=0&ry=0&os=WinXP&scr=1280_960&ck=true&s=1&ww=1213&wh=824&ym=&fs=1&pvid=888e9ab2ed459ca798c2c31efc72df0b&yhc=&msid=4227d5f89edbe44";
	  String spara = "tc=&cc=&bgc=&bc=D9D9D9&tb=0&cb=0&tu=0&cu=0&uid=76060&zid=140012&pid=2&w=728&h=90&t=1&a=1&c=1&sid=9ac3c5bf44420789&ua=Mozilla/4.0%20%28compatible%3B%20MSIE%206.0%3B%20Windows%20NT%205.1%3B%20Trident/5.0%3B%20SLCC2%3B%20.NET%20CLR%202.0.50727%3B%20.NET%20CLR%203.5.30729%3B%20.NET%20CLR%203.0.30729%3B%20Media%20Center%20PC%206.0%3B%20BOIE9%3BZHCN%3B%20SE%202.x%29&n=Microsoft%20Internet%20Explorer&p=http:&v=4.0%20%28compatible%3B%20MSIE%206.0%3B%20Windows%20NT%205.1%3B%20Trident/5.0%3B%20SLCC2%3B%20.NET%20CLR%202.0.50727%3B%20.NET%20CLR%203.5.30729%3B%20.NET%20CLR%203.0.30729%3B%20Media%20Center%20PC%206.0%3B%20BOIE9%3BZHCN%3B%20SE%202.x%29&r=&ho=www.beijingdz.com&l=http%3A//www.beijingdz.com/a/gouwu/quanqiugouwu/&ax=0&ay=0&rx=0&ry=0&os=WinXP&scr=1280_960&ck=true&s=1&ww=1213&wh=824&ym=&fs=1&pvid=888e9ab2ed459ca798c2c31efc72df0b&yhc=&msid=4227d5f89edbe44";
	  Request req = HttpRequestParser.parse(s);
	  System.out.println(req.toString());
  }
}
