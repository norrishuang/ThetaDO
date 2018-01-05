package com.thetado.core.parser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import com.thetado.core.taskmanage.TaskInfo;
import com.thetado.core.taskmanage.TaskMgr;
import com.thetado.core.template.LineTempletP;
import com.thetado.core.transform.LineTransform;
import com.thetado.utils.ConstDef;
import com.thetado.utils.Util;



public class LineParser extends AbstractParser {
	private String remainingData = "";

	private int m_ParseTime = 0;

	

	public LineParser(TaskInfo collectInfo)
  	{
		super(collectInfo,new LineTransform(collectInfo));
  	}


	/**
	 * 数据解析
	 */
  	@SuppressWarnings("resource")
	public boolean parseData() throws Exception
  	{
  		//FileReader reader = null;
  		FileInputStream fis = null;
  		try
  		{
  			String logStr = this + ": starting parse file : " + this.fileName;
  			this.log.debug(logStr);
  			this.collectObjInfo.log("解析", logStr);

  			//行解析模版
  			LineTempletP templet = (LineTempletP)this.collectObjInfo.getParseTemplet();
  			
  			File fs = new File(this.fileName);
      
  			fis = new FileInputStream(fs);
      
  			BufferedReader br = null;
  			if(templet.m_strEncode.isEmpty())
  			{
  				br = new BufferedReader(new InputStreamReader(fis));
  			}
  			else
  			{
  				br = new BufferedReader(new InputStreamReader(fis,templet.m_strEncode));
  			}
      
  			char[] buff = new char[65536];

  			
  			int iLen = 0;
  			while ((iLen = br.read(buff)) > 0)
  			{
  				BuildData(buff, iLen);
  			}

  			String strEnd = "\n**FILEEND**";
  			BuildData(strEnd.toCharArray(), strEnd.length());
  		}
  		catch (Exception ex)
		{
  			errorlog.error("LineParser Error",ex);
		}
  		finally
  		{
  			try
  			{
  				if (fis != null) {
  					fis.close();
  				}
  			}
  			catch (Exception localException)
  			{
  			}
  		}
  		return true;
  	}

  	public boolean BuildData(char[] chData, int iLen)
  	{
  		boolean bReturn = true;

  		this.remainingData += new String(chData, 0, iLen);

  		String logStr = null;

  		if (++this.m_ParseTime % 100 == 0)
  		{
  			logStr = this + ": " + this.collectObjInfo.getDescribe() + 
  			" parse time:" + this.m_ParseTime;
  			this.log.debug(logStr);
  			this.collectObjInfo.log("解析", logStr);
  		}
  		boolean bLastCharN = false;
  		if (this.remainingData.charAt(this.remainingData.length() - 1) == '\n') {
  			bLastCharN = true;
  		}

  		String[] strzRowData = this.remainingData.split("\n");

  		if (strzRowData.length == 0) {
  			return true;
  		}

  		int nRowCount = strzRowData.length - 1;
  		this.remainingData = strzRowData[nRowCount];
  		if (this.remainingData.equals("**FILEEND**")) {
  			this.remainingData = "";
  		}

  		if (bLastCharN) {
  			this.remainingData += "\n";
  		}

  		try
  		{
  			for (int i = 0; i < nRowCount; i++)
  			{
  				if (Util.isNull(strzRowData[i]))
  					continue;
  				//ParseLineData(strzRowData[i]);
  				Transform.ParserData(strzRowData[i]);
  			}
  		}
  		catch (Exception e)
  		{
  			bReturn = false;
  			logStr = this + ": Cause:";
  			this.errorlog.error(logStr, e);
  			this.collectObjInfo.log("解析", logStr, e);
  		}

  		return bReturn;
  	}


	  public String toString()
	  {
	    String strTaskID = "Line-Parser-" + (
	      this.collectObjInfo == null ? "NULL" : Integer.valueOf(this.collectObjInfo.getTaskID()));
	    return strTaskID;
	  }

	@Override
	public void Stop() {
		// TODO Auto-generated method stub
		
	}
}

    