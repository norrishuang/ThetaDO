package com.thetado.crawler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.List;
import java.util.Map;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.thetado.crawler.parser.UrlExtractParser;

public class WebAccess {
	
	/*
	 * 通过URL 获取网页内容
	 */
	public static String sendGet(String url) {  
        String result = "";  
        BufferedReader in = null;  
        try {  
            String urlName = url;  
            URL realUrl = new URL(urlName);  
            //打开和URL之间的连接  
//            URLConnection conn = realUrl.openConnection();  
            HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();  
            //设置通用的请求属性  
            conn.setRequestProperty("accept", "text/html, application/xhtml+xml, */*");  
            conn.setRequestProperty("connection", "Keep-Alive");  
            conn.setRequestProperty("user-agent",  
                "User-Agent	Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko");  
            conn.setRequestProperty("Accept-Encoding","gzip, deflate");
            conn.setRequestProperty("Cookie","YF-Page-G0=091b90e49b7b3ab2860004fba404a078; SUB=_2AkMuczPWf8NxqwJRmP8czWPlboV3wwnEieKYL8INJRMxHRl-yT83qmgCtRCrsrIQacxbDYXfvInut7pHGktuRA..; SUBP=0033WrSXqPxfM72-Ws9jqgMF55529P9D9WWMHNKnOb_fSuTueq5YRQ6j");
            
            //建立实际的连接  
            conn.connect();  
            
            //获取所有响应头字段  
            Map < String, List < String >> map = conn.getHeaderFields();  
            //遍历所有的响应头字段  
            for (String key: map.keySet()) {  
                System.out.println(key + "--->" + map.get(key));  
            }  
            //定义BufferedReader输入流来读取URL的响应  
            in = new BufferedReader(new InputStreamReader(conn.getInputStream(),"UTF-8"));  
            String line;  
            while ((line = in .readLine()) != null) {
            	//解析到行的时候，开始处理需要提取的信息
                result += "\r\n" + line;  
                
                //1:提取需要的URL
//                UrlExtractParser urlExt = new UrlExtractParser();
//                urlExt.ExtractUrl(line);
            }  
        } catch (Exception e) {  
            System.out.println("发送GET请求出现异常！" + e);  
            e.printStackTrace();  
        }  
        //使用finally块来关闭输入流  
        finally {  
            try {  
                if ( in != null) { in .close();  
                }  
            } catch (IOException ex) {  
                ex.printStackTrace();  
            }  
        }  
        return result;  
    }  
	
	public void JsoupHtml(String html) {
		Document doc = Jsoup.parse(html);
		Element content = doc.getElementById("u");
		Elements links = content.getElementsByTag("a");
		for (Element link : links) {
		  String linkHref = link.attr("href");
		  String linkText = link.text();
		}
	}

}
