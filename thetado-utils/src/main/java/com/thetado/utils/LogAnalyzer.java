package com.thetado.utils;

import java.io.InputStream;


/**
 * 日志分析
 * @author Administrator
 *
 */
public abstract interface LogAnalyzer
{
	public abstract void init()
    	throws LogAnalyzerException;

	public abstract SqlldrResult analysis(String paramString)
    	throws LogAnalyzerException;

	public abstract SqlldrResult analysis(InputStream paramInputStream)
    	throws LogAnalyzerException;

	public abstract void destory();
}
    