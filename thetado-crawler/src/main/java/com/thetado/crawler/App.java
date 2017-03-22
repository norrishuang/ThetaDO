package com.thetado.crawler;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
//    	String url = "https://login.sina.com.cn/visitor/visitor?a=crossdomain&cb=return_back&s=_2AkMuczPWf8NxqwJRmP8czWPlboV3wwnEieKYL8INJRMxHRl-yT83qmgCtRCrsrIQacxbDYXfvInut7pHGktuRA..&sp=0033WrSXqPxfM72-Ws9jqgMF55529P9D9WWMHNKnOb_fSuTueq5YRQ6j&from=weibo&_rand=0.06790710857792748";
    	String url = "http://weibo.com/huangxiao227";
        String webContent = WebAccess.sendGet(url);
//        System.out.println(webContent);
    	WebAccess webAcc = new WebAccess();
    	webAcc.JsoupHtml(webContent);
    }
}
