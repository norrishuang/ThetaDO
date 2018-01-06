package com.thetado.core.access;


import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Date;

import org.apache.log4j.Logger;

import com.thetado.core.distribute.AbstractDistribute;
import com.thetado.core.distribute.DistributeTemplet;
import com.thetado.core.distribute.TableItem;
import com.thetado.core.parser.AbstractParser;
import com.thetado.core.taskmanage.TaskInfo;
import com.thetado.core.template.ITempletBase;
import com.thetado.utils.Task;
import com.thetado.utils.Util;


/**
 * ETL 接入层 抽象类，多种接入方式DB/Ftp/Telnet/HTTP
 * @author Administrator 
 *
 */
public abstract class AbstractAccessor extends Task
  implements Accessor
{
	protected Logger log = Logger.getLogger(AbstractAccessor.class);
	protected Logger errorlog = Logger.getLogger(AbstractAccessor.class);
	
	protected TaskInfo taskInfo;
	protected AbstractParser parser;
	protected AbstractDistribute distributor;
	protected boolean runFlag = true;

	private boolean accessSucc = false;
	private int taskID;
	protected String strLastGatherTime;
	private GenericDataConfig dataSourceConfig;
	protected String name;

	public abstract void configure()
    	throws Exception;

	public abstract boolean access()
    	throws Exception;

	/*
	 * 任务执行
	   (non-Javadoc)
	 * @see com.thetado.utils.Task#run()
	 */
	public void run()
  	{
  		String logStr = null;
  		long lastCollectTime = -1L;
  		try { 
  			configure();

  			boolean b = validate();
  			if (!b) return;
  			do { 

  				doReady();
  				doStart();
  				
  				b = doBeforeAccess(); 
  			} while (!b);

  			lastCollectTime = this.taskInfo.getLastCollectTime().getTime();
  			this.accessSucc = (b = access());
  			logStr = this.name + ": 数据(" + this.strLastGatherTime + ")接入完成. 接入结果=" + 
  				this.accessSucc;
  			this.log.info(logStr);
  			this.taskInfo.log("开始", logStr);

  			b = doAfterAccess();

  			doFinishedAccess();

  			distributor.DataLoad();
//  			doSqlLoad();

  			doFinished();

  			this.accessSucc = true;

  			Thread.sleep(3000L);
  		}
  		catch (InterruptedException ie)
    	{
    		this.log.error(this.name + ": 任务被外界终止");
    		this.taskInfo.log("结束", this.name + ": 任务被外界终止", ie);
    	}
    	catch (Exception e)
    	{
    		logStr = this.name + ": 数据(" + this.strLastGatherTime + ")接入异常,原因：";
    		this.errorlog.error(logStr, e);
    		this.taskInfo.log("结束", logStr, e);
    	}
    	finally
    	{
    		dispose(lastCollectTime);
    	}
  	}
	
	public void shutdown()
	{
	}

	public void dispose(long lastCollectTime)
	{
		closeFiles();

		this.runFlag = false;
		this.taskInfo.setUsedFlag(false);

		String logStr = this.name + ": remove from active-task-map. " + 
			this.strLastGatherTime;
		this.log.debug(logStr);
		this.taskInfo.log("dispose", logStr);
		if(this.taskInfo.getCollectType() != 10)
		{
//			TaskMgr.getInstance().delActiveTask(this.taskInfo.getKeyID(), this.taskInfo instanceof RegatherObjInfo);
		}
		else
		{
			//socket 实时采集接口
			this.log.info("Sokect Start ID:" + this.taskInfo.getKeyID());
		}

//		TaskMgr.getInstance().commitRegather(this.taskInfo, lastCollectTime);
		
	}

	/**
	 * 
	 * 验证	 
	 * */
	public boolean validate()
	{
		boolean b = true;
		
		if(distributor == null) {
			log.error("Distributor Object is NULL");
			return false;
		}

		if (this.taskInfo == null) {
			return false;
		}

//		if (((this.dataSourceConfig == null) || (this.dataSourceConfig.getDatas() == null)) && 
//				(this.taskInfo.getCollectType() != 9))
//		{
//			this.log.error("taskId-" + this.taskInfo.getTaskID() + 
//					":不是有效任务，原因，collect_path为空");
//			return false;
//		}

		try
		{
			this.strLastGatherTime = Util.getDateString(this.taskInfo.getLastCollectTime());
		}
		catch (Exception e)
		{
			this.errorlog.error(this.name + "> 时间格式错误,原因:", e);
			this.taskInfo.log("开始", this.name + "> 时间格式错误,原因:", e);
			b = false;
		}

		return b;
	}

	/**
	 *
	 */
	public void doReady()
    	throws Exception
    {
		this.taskInfo.setUsedFlag(true);

		this.taskInfo.setStartTime(new Timestamp(new Date().getTime()));

		int sleepTime = this.taskInfo.getThreadSleepTime();
		if (sleepTime > 0)
		{
			this.log.debug(this.name + " sleep " + sleepTime + " (s)");
			this.taskInfo.log("开始", this.name + " sleep " + sleepTime + 
				" (s)");
			Thread.sleep(sleepTime * 1000);
		}
    }

	/**
	 *
	 */
	public void doStart()
    	throws Exception
    {
		
		
		this.log.info(this.name + ": 开始采集时间点为 " + this.strLastGatherTime + " 的数据.");
		this.taskInfo.log("开始", this.name + ": 开始采集时间点为 " + 
				this.strLastGatherTime + " 的数据.");
    }

	public boolean doBeforeAccess()
    	throws Exception
    {
		return true;
    }

	/**
	 *
	 */
	public void parse(char[] chData, int iLen)
    	throws Exception
    {
    }

	public boolean doAfterAccess()
      	throws Exception
    {
		return true;
    }

    public void doFinishedAccess()
    	throws Exception
    {
    }

  	

  	
  	/**
  	 *
  	 */
  	private void closeFiles()
  	{
  		ITempletBase distmp = this.taskInfo.getDistributeTemplet();
//  		if (!(distmp instanceof GenericSectionHeadD))
  		{
  			for (int i = 0; i < ((DistributeTemplet)distmp).tableTemplets.size(); i++)
  			{
  				TableItem tableItem = (TableItem)((DistributeTemplet)distmp).tableItems.get(Integer.valueOf(i));
  				if (tableItem == null) {
  					continue;
  				}
  				FileOutputStream fw = tableItem.fileWriter;
  				if (fw == null)
  					continue;
  				try
  				{
  					fw.close();
  				}
  				catch (IOException localIOException)
  				{
  				}
  			}
  		}
  	}

  	public TaskInfo getTaskInfo()
  	{
  		return this.taskInfo;
  	}

  	public void setTaskInfo(TaskInfo obj)
  	{
  		this.taskInfo = obj;
  		this.taskID = obj.getTaskID();
//  		this.dataSourceConfig = GenericDataConfig.wrap(obj.getCollectPath());
    
  		int id = obj.getTaskID();
//  		if ((obj instanceof RegatherObjInfo))
//  			id = obj.getKeyID() - 10000000;
  		this.name = (obj.getTaskID() + "-" + id);
  	}

  	public AbstractParser getParser()
  	{
  		return this.parser;
  	}

  	public void setParser(AbstractParser parser)
  	{
  		this.parser = parser;
  	}

  	public AbstractDistribute getDistributor()
  	{
  		return this.distributor;
  	}

  	public void setDistributor(AbstractDistribute distributor)
  	{
  		this.distributor = distributor;
  	}

  	public int getTaskID()
  	{
  		return this.taskID;
  	}

  	public void setTaskID(int taskID)
  	{
  		this.taskID = taskID;
  	}

  	public GenericDataConfig getDataSourceConfig()
  	{
  		return this.dataSourceConfig;
  	}

  	public void setDataSourceConfig(GenericDataConfig dataSourceConfig)
  	{
  		this.dataSourceConfig = dataSourceConfig;
  	}

  	public String getStrLastGatherTime()
  	{
  		return this.strLastGatherTime;
  	}

  	public void setStrLastGatherTime(String strLastGatherTime)
  	{
  		this.strLastGatherTime = strLastGatherTime;
  	}	

  	public boolean isAccessSucc()
  	{
  		return this.accessSucc;
  	}

  	public void setAccessSucc(boolean accessSucc)
  	{
  		this.accessSucc = accessSucc;
  	}

  	public String getMyName()
  	{
  		return this.name;
  	}
  	
  	public String toString()
  	{
  		return "Thread:"+taskInfo.getDescribe();
  	}
}