package com.thetado.core.distribute;

import java.util.Map;

import com.thetado.core.taskmanage.TaskInfo;

public abstract class AbstractDistribute {

	protected TaskInfo collectInfo;
	protected Map<Integer, TableItem> tableItems = null;
	protected DistributeTemplet disTmp;
	
	public AbstractDistribute(TaskInfo TaskInfo)
	{
		try
		{
			this.collectInfo = TaskInfo;
			this.disTmp = ((DistributeTemplet)TaskInfo.getDistributeTemplet());
			init();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void init(TaskInfo TaskInfo)
	{
		try
		{
			this.collectInfo = TaskInfo;
			this.disTmp = ((DistributeTemplet)TaskInfo.getDistributeTemplet());
			init();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	
	protected abstract void init();
	
	public abstract void DataLoad();
	
	public void mySleep(long ms)
  	{
  		try
  		{
  			Thread.sleep(ms);
  		}
  		catch (InterruptedException localInterruptedException)
  		{
  		}
  	}
}

    