package com.thetado.core.distribute;

import java.util.Map;

import org.apache.log4j.Logger;

import com.thetado.core.taskmanage.TaskInfo;

public abstract class AbstractDistribute {

	protected TaskInfo collectInfo;
	protected Map<Integer, TableItem> tableItems = null;
	protected DistributeTemplet disTmp;
	protected Logger log = Logger.getLogger(AbstractDistribute.class);
	
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
	
	public abstract boolean DistributeData(byte[] bData, int tableIndex);
	
	/**
  	 * 写入分发数据
  	 * @param bData
  	 * @param tableIndex
  	 * @return
  	 */
//  	public boolean DistributeData(byte[] bData, int tableIndex)
//  	{
//  		boolean bReturn = true;
//
//  		if ((this.collectInfo == null) || (this.disTmp == null))
//  		{
//  			this.log.error("DistributeData: task 为 null. 数据分发失败.");
//  			this.collectInfo.log("入库", "DistributeData: task 为 null. 数据分发失败.");
//  			return false;
//  		}
//
////  		this.collectInfo.m_nAllRecordCount += 1;
//
//  		switch (this.disTmp.stockStyle)
//  		{
//	  		case 0://不做分发写文件
//	  			return true;
//  			case 1://数据库INSERT
//  				Distribute_Insert(bData, tableIndex);
//  				break;
//  			case 2:
//  			case 3://数据库工具导入 例如 sqlldr
//  				bReturn = Distribute_Sqlldr(bData, tableIndex);
//  				break;
//  			case 4://文件分发，FTP上传
//  				Distribute_File(bData, tableIndex);
//  				break;
//  		}
//
//  		return bReturn;
//  	}
	
	/**
 	 * 获取分发模版
 	 * @return
 	 */
 	public DistributeTemplet getDisTemplet()
 	{
 		return this.disTmp;
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

    