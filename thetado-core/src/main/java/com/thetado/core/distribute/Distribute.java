package com.thetado.core.distribute;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;

import com.thetado.core.config.SystemConfig;
import com.thetado.core.distribute.DistributeTemplet.FieldTemplet;
import com.thetado.core.distribute.DistributeTemplet.TableTemplet;
import com.thetado.core.taskmanage.TaskInfo;
import com.thetado.utils.Util;

/**
 * 分发
 * @author Administrator
 *
 */
public class Distribute extends AbstractDistribute
{
	public Distribute(TaskInfo TaskInfo) {
		super(TaskInfo);  
	}

	protected TaskInfo collectInfo;
	private Map<Integer, TableItem> tableItems = null;
	protected DistributeTemplet disTmp;
	private DistributeSqlLdr sqlldr = null;
	

	protected Logger log = Logger.getLogger(Distribute.class);

	String fileName = "";
	

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

	/**
	 * 初始化分发模版
	 */
	public void init()
	{
		TableItem tableItem = null;
		this.tableItems = this.disTmp.tableItems;
		Map<Integer, TableTemplet> tables = this.disTmp.tableTemplets;

		switch (this.disTmp.stockStyle)
		{
			case 1://SQL INSERT
				for (int i = 0; i < tables.size(); i++)
				{
					tableItem = new TableItem();
					DistributeTemplet.TableTemplet tableTmp = (DistributeTemplet.TableTemplet)tables.get(Integer.valueOf(i));
					Map<Integer, FieldTemplet> fields = tableTmp.fields;
					StringBuffer buffer = new StringBuffer();
					buffer.append("insert into " + tableTmp.tableName + "  values(");

					for (int j = 0; j < fields.size(); j++)
					{
						DistributeTemplet.FieldTemplet FieldInfo = (DistributeTemplet.FieldTemplet)fields.get(Integer.valueOf(j));
						if (FieldInfo.m_nDataType == 3)
						{
							buffer.append("to_date(?,'" + 
									FieldInfo.m_strDataTimeFormat + "'),");
						}
						else if (FieldInfo.m_nDataType == 5)
						{
							buffer.append("to_date(?,'" + 
									FieldInfo.m_strDataTimeFormat + "'),");
						}
						else
						{
							buffer.append("?,");
						}
					}
					String strSQL = buffer.substring(0, buffer.toString().length() - 1);
					strSQL = strSQL + ")";
					tableItem.tableIndex = i;
					tableItem.sql = strSQL;
					this.tableItems.put(Integer.valueOf(tableTmp.tableIndex), tableItem);
				}
				break;
			case 2:
			case 3:
			case 4://file
				String currentPath = SystemConfig.getInstance().getCurrentPath();
				for (int i = 0; i < tables.size(); i++)
				{
					tableItem = new TableItem();

					DistributeTemplet.TableTemplet TableInfo = (DistributeTemplet.TableTemplet)tables.get(Integer.valueOf(i));

					tableItem.tableIndex = TableInfo.tableIndex;

					Date now = new Date(this.collectInfo.getLastCollectTime().getTime());

					String strTime = Util.getDateString_yyyyMMddHHmmss(now);

					Random random = new Random(System.currentTimeMillis());
					int nFileID = Math.abs(random.nextInt(100000));//随机文件ID
					    
					String strFileName =  this.collectInfo.getTaskID() + "_" + strTime + "_" + i + "_" + nFileID;

					tableItem.fileName = strFileName;

					String strTmpFileName = currentPath + File.separatorChar + 
						strFileName + ".txt";
					tableItem.outputFileName = strTmpFileName;
					buildFileHead(strTmpFileName, tableItem, TableInfo);

					this.tableItems.put(Integer.valueOf(TableInfo.tableIndex), tableItem);
					
					try {
						Thread.sleep(10L);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
		}
	}

	public void distribute(Object wParam, Object lParam)
    	throws Exception
    {
    }

 	public void commit()
 	{
 	}

 	/**
 	 * 获取分发模版
 	 * @return
 	 */
 	public DistributeTemplet getDisTemplet()
 	{
 		return this.disTmp;
 	}
  
  	/**
  	 * 写入分发数据
  	 * @param bData
  	 * @param tableIndex
  	 * @return
  	 */
  	public boolean DistributeData(byte[] bData, int tableIndex)
  	{
  		boolean bReturn = true;

  		if ((this.collectInfo == null) || (this.disTmp == null))
  		{
  			this.log.error("DistributeData: task 为 null. 数据分发失败.");
  			this.collectInfo.log("入库", "DistributeData: task 为 null. 数据分发失败.");
  			return false;
  		}

  		this.collectInfo.setAllRecordCount(1);

  		switch (this.disTmp.stockStyle)
  		{
	  		case 0://不做分发写文件
	  			return true;
  			case 1://数据库INSERT
  				Distribute_Insert(bData, tableIndex);
  				break;
  			case 2:
  			case 3://数据库工具导入 例如 sqlldr
//  				bReturn = Distribute_Sqlldr(bData, tableIndex);
  				break;
  			case 4://文件分发，FTP上传
  				Distribute_File(bData, tableIndex);
  				break;
  		}

  		return bReturn;
  	}

  	
  	/**
  	 * 文件方式
  	 * @param bData
  	 * @param tableIndex
  	 */
  	private void Distribute_File(byte[] bData,int tableIndex)
  	{
  		int TmpType = tableIndex;
  		String logStr = null;
  		try
  		{
  			TableItem tableItem = (TableItem)this.tableItems.get(Integer.valueOf(TmpType));

  			tableItem.recordCounts += 1;

  			FileOutputStream fw = tableItem.fileWriter;

  			if (fw != null)
  			{
  				fw.write(bData);
  				fw.flush();
  			}
  			/*else if(!fw.getFD().valid())
  			{
  				int count = 0;
  				while(count < 5)
  				{
  					count++;
  					if(fw.getFD().valid())
  					{
  						fw.write(bData);
  		  				fw.flush();
  		  				break;
  					}
  					Thread.sleep(3000);
  				}
  			}*/
  			else
  			{
  				logStr = this.collectInfo.getDescribe() + 
  					": distribute error, no file create! ";
  				this.log.error(logStr);
  				this.collectInfo.log("入库", logStr);
  			}
  			
  			int nOnceShockCount = this.disTmp.onceStockCount;
  			
  			if ((nOnceShockCount != -1) && 
  					(tableItem.recordCounts >= nOnceShockCount))
  			{  				
  				
  				DistributeTemplet distmp = (DistributeTemplet)this.collectInfo.getDistributeTemplet();
  				DistributeTemplet.TableTemplet table = (DistributeTemplet.TableTemplet)distmp.tableTemplets.get(Integer.valueOf(tableIndex));
  				String strOldFileName = tableItem.fileName;
  				
  				log.debug("commit file,rows>onceshockcount:" + tableItem.recordCounts + "," + nOnceShockCount + " File:" + tableItem.outputFileName);
  				
  				DistributeFile outputfile = new DistributeFile(this.collectInfo);
  				fw.close();
  				
  				//调用SQL LOAD入库
  				outputfile.BulidFileUpoadFtpThread(table.tableIndex, strOldFileName);

  				log.debug("old file upload:" + strOldFileName);
  				tableItem.recordCounts = 0;
  				String strCurrentPath = SystemConfig.getInstance().getCurrentPath();

  				Date now = new Date(this.collectInfo.getLastCollectTime().getTime());
  				SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
  				String strTime = formatter.format(now);
  				
  				Random random = new Random(System.currentTimeMillis());
				int nFileID = Math.abs(random.nextInt(100000));//随机文件ID
				    
				String strNewFileName = this.collectInfo.getTaskID() + "_" + strTime 
						+ "_" + String.valueOf(tableIndex) +"_" + nFileID;

				
  				tableItem.fileName = strNewFileName;

  				String strTmpFileName = strCurrentPath + File.separatorChar + 
  				strNewFileName + ".txt";
  				tableItem.outputFileName = strTmpFileName;
  				buildFileHead(strTmpFileName, tableItem, table);
  				
  				log.debug("create new file:" + strNewFileName);
		 		
  				
  				
  				
  			}
  			
  			
  		}
  		catch (Exception ex)
  		{
  			logStr = this.collectInfo.getDescribe() + ": Distribute_File error.";
  			this.log.error(logStr, ex);
  			this.collectInfo.log("入库", logStr, ex);
  			try
  			{
  				Thread.sleep(5000L);
  			}
  			catch (InterruptedException localInterruptedException)
  			{
  			}
  		}
  	}

  	/**
  	 * 以SQL方式入库
  	 * @param bData
  	 * @param tableIndex
  	 */
  	public void Distribute_Insert(byte[] bData, int tableIndex)
  	{
  		PreparedStatement stmt = null;
  		Connection con = null;
  		try
  		{
//  			con = CommonDB.getConnection();

  			TableItem tableItem = (TableItem)this.tableItems.get(Integer.valueOf(tableIndex));
  			String strSQL = tableItem.sql;

  			stmt = con.prepareStatement(strSQL);
  			String m_strData = new String(bData);

  			String[] strRowData = m_strData.split("\n");

  			for (int i = 0; i < strRowData.length; i++)
  			{
  				String[] strColData = strRowData[i].split(";");
  				for (int j = 0; j < strColData.length; j++)
  				{
  					stmt.setString(j + 1, strColData[j]);
  				}
  				stmt.addBatch();
  			}
  			stmt.executeBatch();
  			con.commit();
  		}
  		catch (Exception e)
  		{
  			this.log.error("Distribute_Insert error.", e);
  			this.collectInfo.log("入库", "Distribute_Insert error.");
  		}
  		finally
  		{
//  			CommonDB.close(null, stmt, con);
  		}	
  	}

  	private List<Integer> findDuplicateCol(List<String> cols, String col)
  	{
  		List<Integer> index = new ArrayList<Integer>();
  		for (int i = 0; i < cols.size(); i++)
  		{
  			if (!((String)cols.get(i)).equalsIgnoreCase(col))
  				continue;
  			index.add(Integer.valueOf(i));
  		}

  		return index.size() > 1 ? index : null;
  	}

  
	

  	/**
  	 * 创建文件头
  	 * @param strFileName
  	 * @param tableItem
  	 * @param table
  	 * @return
  	 */
	private boolean buildFileHead(String strFileName, TableItem tableItem, DistributeTemplet.TableTemplet table)
  	{
  		//如果解析模版为-1 即为空解析模版，不做分发处理，直接返回。
  		if(this.collectInfo.getParesTmpType() == -1)
  			return true;
  		
  		if ((strFileName == null) || (strFileName.equals(""))) 
  		{
  			return false;
  		}
  		String logStr = null;

  		
  		
  		boolean bReturn = true;

  		FileOutputStream fw = null;
  		try
  		{
  			fw = new FileOutputStream(strFileName);
  		}
  		catch (IOException e)
  		{
  			logStr = this.collectInfo.getDescribe() + 
  				": error when building file head. ";
  			this.log.error(logStr, e);
  			this.collectInfo.log("入库", logStr, e);
  			return false;
  		}

  		tableItem.fileWriter = fw;

  		if (table.isFillTitle)
  		{
  			try
  			{
  				int len = table.fields.size();
  				for (int k = 0; k < len - 1; k++)
  				{
  					DistributeTemplet.FieldTemplet field = (DistributeTemplet.FieldTemplet)table.fields.get(Integer.valueOf(k));

  					fw.write(String.format("%s;", field.m_strFieldName).getBytes());
  				}
  				fw.write(((DistributeTemplet.FieldTemplet)table.fields.get(Integer.valueOf(len - 1))).m_strFieldName.getBytes());

  				fw.write("\n".getBytes());
  				fw.flush();
  			}
  			catch (IOException e)
  			{
  				logStr = this.collectInfo.getDescribe() + ": error when building file head. ";
  				this.log.error(logStr, e);
  				this.collectInfo.log("入库", logStr, e);
  				bReturn = false;
  			}
  		}
  		return bReturn;
  	}

  	

  	public TaskInfo getCollectInfo()
  	{
  		return this.collectInfo;
  	}

  	public void setCollectInfo(TaskInfo collectInfo)
  	{
  		this.collectInfo = collectInfo;
  	}
  	
  	private String filePath = "";
  	public String getFilePath()
  	{
  		return filePath;
  	}
  	
  	public void setFilePath(String filepath)
  	{
  		filePath = filepath;
  	}

	@Override
	public void DataLoad() {
	}
}