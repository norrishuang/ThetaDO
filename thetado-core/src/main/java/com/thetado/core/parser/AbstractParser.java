package com.thetado.core.parser;


import java.util.ArrayList;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.thetado.core.distribute.AbstractDistribute;
import com.thetado.core.distribute.Distribute;
import com.thetado.core.taskmanage.TaskInfo;
import com.thetado.core.transform.AbstractTransform;


/**
 * 解析抽象类
 * @author Administrator
 *
 */
public abstract class AbstractParser
{
	protected TaskInfo collectObjInfo;
	public AbstractDistribute distribute;
	public AbstractTransform Transform;
	protected String fileName = "";
	protected String dsConfigName = null;

	protected Logger log = Logger.getLogger(AbstractParser.class);
	protected Logger errorlog = Logger.getLogger(AbstractParser.class);

	public AbstractParser() {
		
	}
	
	public AbstractParser(TaskInfo obj,AbstractTransform transform)
	{
		this.collectObjInfo = obj;
		this.distribute = new Distribute(obj);
		this.Transform = transform;
	}


	public String getFileName()
	{
		return this.fileName;
	}

	public void setFileName(String strFileName)
	{
		this.fileName = strFileName;
	}

	public String getDsConfigName()
	{
		return this.dsConfigName;
	}

	public void setDsConfigName(String dsConfigName)
	{
		this.dsConfigName = dsConfigName;
	}

	public void init(TaskInfo obj)
	{
		this.collectObjInfo = obj;
	}

	/**
	 * 解析数据
	 * @return
	 * @throws Exception
	 */
	public abstract boolean parseData()
		throws Exception;

	public TaskInfo getCollectObjInfo()
	{
		return this.collectObjInfo;
	}

	public void setCollectObjInfo(TaskInfo collectObjInfo)
	{
		this.collectObjInfo = collectObjInfo;
	}

	public AbstractDistribute getDistribute()
	{
		return this.distribute;
	}

	public void setDistribute(AbstractDistribute distribute)
	{
		this.distribute = distribute;
	}
	
	public String GetDistributeFile()
	{
		return "";
	}
	
	
	
	
	
	
	public abstract void Stop();
}