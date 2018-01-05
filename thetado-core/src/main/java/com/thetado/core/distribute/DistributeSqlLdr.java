package com.thetado.core.distribute;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.sql.Timestamp;
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
import com.thetado.utils.SqlLdrLogAnalyzer;
import com.thetado.utils.SqlldrResult;
import com.thetado.utils.Util;
import com.thetado.utils.string.LevenshteinDistance;

/**
 * 
     *@Title SQLLOAD 数据分发方法 
     *@Description 
     *@Author Turk
     *@Since 2017年11月15日
     *@Version 1.1.0
 */
public class DistributeSqlLdr extends AbstractDistribute{

	private Logger log = Logger.getLogger(DistributeSqlLdr.class);
	
	public DistributeSqlLdr(TaskInfo TaskInfo) {
		super(TaskInfo);  
	}

	@Override
	protected void init() {
		TableItem tableItem = null;
		this.tableItems = this.disTmp.tableItems;
		Map<Integer, TableTemplet> tables = this.disTmp.tableTemplets;
		
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

	@Override
	public void DataLoad() {
		doSqlLoad();
	}
	
	public boolean Distribute_Sqlldr(byte[] bData, int tableIndex)
  	{
  		if(this.collectInfo.getParesTmpType() == -1)
  			return true;
  		
  		boolean bReturn = true;
  		String logStr = null;
  		try
  		{
  			if (this.tableItems == null)
  			{
  				logStr = this.collectInfo.getDescribe() + 
  					": Distribute_Sqlldr: m_hFile 为null,数据分发失败. 请检查模板配置.";
  				this.log.error(logStr);
  				this.collectInfo.log("结束", logStr);
  				return false;
  			}

  			TableItem tableItem = (TableItem)this.tableItems.get(Integer.valueOf(tableIndex));
  			if (tableItem == null)
  			{
  				logStr = this.collectInfo.getDescribe() + 
  					": Distribute_Sqlldr: tableItem 为null,数据分发失败. 请检查模板配置.";
  				this.log.error(logStr);
  				this.collectInfo.log("结束", logStr);
  				return false;
  			}

  			tableItem.recordCounts += 1;

  			FileOutputStream fw = tableItem.fileWriter;

  			File txt = new File(SystemConfig.getInstance().getCurrentPath(), tableItem.fileName + 
  				".txt");
  			List<String> raws = null;
  			Map<String, List<Integer>> colToIndex = new HashMap<String, List<Integer>>();
  			List<Object> dels = new ArrayList<Object>();
  			String splitSign = ";";
  			if ((tableItem.head == null) && (txt.length() > 0L))
  			{
  				BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(txt)));
  				String firstLine = br.readLine();
  				br.close();
  				try
  				{
  					String DeviceID = String.valueOf(this.collectInfo.getDevInfo().getDevID());
  					splitSign = firstLine.substring(firstLine.indexOf(DeviceID) + 
  							DeviceID.length(), firstLine.indexOf(DeviceID) + 
  							DeviceID.length() + 1);
  				}
  				catch (Exception e)
  				{	
  					logStr = "get splitSign error:" + e.getMessage();
  					this.log.error(logStr);
  					this.collectInfo.log("入库", logStr, e);
  				}
  				String[] items = firstLine.split(";");
  				raws = new ArrayList<String>();
  				for (String s : items)
  				{
  					if (!Util.isNotNull(s))
  						continue;
  					raws.add(s.trim());
  				}

  				tableItem.head = raws;
  				for (int i = 0; i < raws.size(); i++)
  				{
  					String r = (String)raws.get(i);
  					if (colToIndex.containsKey(r))
  						continue;
  					List<Integer> index = findDuplicateCol(raws, r);
  					if (index == null)
  						continue;
  					colToIndex.put(r, index);
  				}

  				Collection<List<Integer>> c = colToIndex.values();
  				for (Object index = c.iterator(); ((Iterator<?>)index).hasNext(); ) 
  				{ 
  					List<?> list = (List<?>)((Iterator<?>)index).next();

  					for (Iterator<?> localIterator = list.iterator(); localIterator.hasNext(); ) 
  					{ 
  						Object i = (Integer)localIterator.next();
  						
  						dels.add(i);
  					}
  				}	
  			}

  			if (fw != null)
  			{
  				String data = new String(bData);
  				if (raws != null)
  				{
  					String[] items = data.split(splitSign);

  					StringBuilder sb = new StringBuilder();
  					for (int i = 0; i < items.length; i++)
  					{
  						boolean flag = false;
  						for (Object ii = dels.iterator(); ((Iterator<?>)ii).hasNext(); ) 
  						{ 
  							Integer del = (Integer)((Iterator<?>)ii).next();

  							if (i != del.intValue())
  								continue;
  							flag = true;
  						}

  						if (flag)
  							continue;
  						sb.append(items[i]).append(splitSign);
  					}

  					if(sb.length() > 0)
  						sb.deleteCharAt(sb.length() - 1);
  					data = sb.toString();
  				}

  				File f = new File(SystemConfig.getInstance().getCurrentPath(), tableItem.fileName + 
  					".txt");
  				if (f.length() > 0L)
  				{
  					BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(txt)));
  					String s = br.readLine();
  					br.close();
  					Object rs = new ArrayList<Object>();
  					String[] ss = s.split(splitSign);
  					for (String str : ss)
  					{
  						if (!Util.isNotNull(str))
  							continue;
  						((List<String>)rs).add(str.trim());
  					}

  					if ((((List<?>)rs).size() > 3) && 
  							(data.split(splitSign).length > 3) && 
  							(((String)((List<?>)rs).get(3)).equalsIgnoreCase(data.split(splitSign)[3])))
  					{
  						data = "";
  					}
  				}

  				fw.write(data.getBytes());

  				fw.flush();
  			}
  			else
  			{
  				this.log.debug("FileWriter ID" + tableIndex);
  				this.log.debug("FileWriter fw " + fw);
  				this.log.debug("containsKey" + this.tableItems.containsKey(Integer.valueOf(tableIndex)));
  				this.log.debug("m_hFile " + this.tableItems.size());

  				mySleep(5000L);
  			}
  			int nOnceShockCount = this.disTmp.onceStockCount;

  			if ((nOnceShockCount != -1) && 
  					(tableItem.recordCounts >= nOnceShockCount))
  			{
//  				if (this.sqlldr == null)
//  					this.sqlldr = new DistributeSqlLdr(this.collectInfo);
  				DistributeTemplet distmp = (DistributeTemplet)this.collectInfo.getDistributeTemplet();
  				DistributeTemplet.TableTemplet table = (DistributeTemplet.TableTemplet)distmp.tableTemplets.get(Integer.valueOf(tableIndex));
  				String strOldFileName = tableItem.fileName;

  				fw.close();
  				
  				//调用SQL LOAD入库
  				buildSqlLdr(table.tableIndex, strOldFileName);

  				tableItem.recordCounts = 0;
  				String strCurrentPath = SystemConfig.getInstance().getCurrentPath();

  				Date now = new Date(this.collectInfo.getLastCollectTime().getTime());
  				SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
  				String strTime = formatter.format(now);

  				String strNewFileName = this.collectInfo.getGroupID() + "_" + 
  				this.collectInfo.getTaskID() + "_" + strTime + "_" + 
  				String.valueOf(tableIndex);

  				tableItem.fileName = strNewFileName;

  				String strTmpFileName = strCurrentPath + File.separatorChar + 
  				strNewFileName + ".txt";
  				buildFileHead(strTmpFileName, tableItem, table);
  			}
  		}
  		catch (Exception e)
  		{
  			bReturn = false;
  			this.log.error("Distribute_Sqlldr error.", e);
  			this.collectInfo.log("入库", "Distribute_Sqlldr error.", e);
  			mySleep(5000L);
  		}

  		return bReturn;
  	}

	private void doSqlLoad() {
	    String logStr = null;

//	    if (this.accessSucc)
	    {
	    	//DistributeTemplet disTmp = ((DistributeTemplet)taskInfo.getDistributeTemplet());
	    
	    	int parseType = this.collectInfo.getParesTmpType();
	    		//this.taskInfo.get
	    		if ((parseType != 3) && 
	    				(parseType != 0) && 
	    				(this.collectInfo.getParserID() != 35) && parseType != -1)
	    		{
	    			File ldrlogDirectory = new File(SystemConfig.getInstance().getCurrentPath() + 
	    					File.separator + "ldrlog");
	    			if (!ldrlogDirectory.exists())
	    			{
	    				ldrlogDirectory.mkdir();
	    			}

	    			logStr = "SQL LOAD: " + " Load data start.";
	    			this.log.info(logStr);
	    			this.collectInfo.log("分发", logStr);

	    			runSqlldr(true);
	    		}
	    	

	    		logStr = "SQL LOAD: " + " Load data end.";
	    		this.log.info(logStr);
	    		this.collectInfo.log("结束", logStr);

	    		logStr = this.collectInfo.getTaskID() + ": " + this.collectInfo.getDescribe() + " import " 
	    				+ " finish 任务开始时间:" +	this.collectInfo.getStartTime() + " " 
	    				+ this.collectInfo.getAllRecordCount();
	    		this.log.info(logStr);
	    		this.collectInfo.log("结束", logStr);
	    	}
	    }

	 
	
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
	
	private void runSqlldr(boolean isAll)
  	{
  		boolean isRedoFlag = false;
//  		isRedoFlag = TaskMgr.getInstance().isReAdoptObj(this.taskInfo);

  		DistributeSqlLdr sqlldr = new DistributeSqlLdr(this.collectInfo);
  		DistributeFile outputfile = new DistributeFile(this.collectInfo);
  		
  		DistributeTemplet distmp = (DistributeTemplet)this.collectInfo.getDistributeTemplet();

  		if ((distmp == null) || (distmp.tableTemplets == null)) {
  			return;
  		}
  		for (int i = 0; i < distmp.tableTemplets.size(); i++)
  		{
  			if ((!isAll) && 
  					(this.collectInfo.getActiveTableIndex() != i)) {
  				continue;
  			}
  			TableItem tableItem = (TableItem)distmp.tableItems.get(Integer.valueOf(i));

  			DistributeTemplet.TableTemplet table = (DistributeTemplet.TableTemplet)distmp.tableTemplets.get(Integer.valueOf(i));

  			String strOldFileName = tableItem.fileName;
  			FileOutputStream fw = tableItem.fileWriter;
  			try
  			{
  				if (fw == null) continue;
  				fw.close();
  			}
  			catch (IOException e)
  			{
  				this.log.error(this + ": runSqlldr", e);
  				this.collectInfo.log("分发", this + ": runSqlldr", e);
  			}

//  			if (isAll)
//  			{
//  				if (isRedoFlag)
//  				{
//  					RegatherObjInfo rTask = (RegatherObjInfo)this.taskInfo;
//  					if ((!rTask.isEmptyTableIndexes()) && 
//  							(!((RegatherObjInfo)this.taskInfo).existsInTableIndexes(i)))
//  					{
//  						continue;
//  					}
//
//  				}
//  				
//  			}

  			switch(distmp.stockStyle)
  			{
  				case 0:
  					break;
	  			case 1://Insert Sql
	  				break;
	  			case 2://SQLLOAD 
	  				sqlldr.buildSqlLdr(table.tableIndex, strOldFileName);
	  			case 3:
	  				break;
	  			case 4: //FILE
	  				outputfile.BuildFileUploadFtp(table.tableIndex, strOldFileName);
	  				break;
  			}
  			

  			tableItem.recordCounts = 0;

  			if (isAll)
  				continue;
  			String strOracleCurrentPath = SystemConfig.getInstance().getCurrentPath();

  			Date now = new Date(this.collectInfo.getLastCollectTime().getTime());
  			SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
  			String strTime = formatter.format(now);
  			String strNewFileName = this.collectInfo.getGroupID() + "_" + 
  				this.collectInfo.getTaskID() + "_" + strTime + "_" + 
  				String.valueOf(i);
  			tableItem.fileName = strNewFileName;
  			try
  			{
  				fw = new FileOutputStream(strOracleCurrentPath + 
  						File.separatorChar + strNewFileName + ".txt");
  				tableItem.fileWriter = fw;
        
  				if (!table.isFillTitle)
  					continue;
  				try
  				{
  					for (int k1 = 0; k1 < table.fields.size(); k1++)
  					{
  						DistributeTemplet.FieldTemplet field = (DistributeTemplet.FieldTemplet)table.fields.get(Integer.valueOf(k1));

  						if (k1 < table.fields.size() - 1)
  							fw.write(String.format("%s;", field.m_strFieldName).getBytes());
  						else
  							fw.write(field.m_strFieldName.getBytes());
  					}
  					fw.write("\n".getBytes());
  					fw.flush();
  				}
  				catch (IOException e)
  				{
  					this.log.error("Name : runSqlldr", e);
  					this.collectInfo.log("分发", "name" + 
  							": runSqlldr", e);
  				}

  			}
  			catch (Exception e)
  			{	
  				this.log.error("name" + ": runSqlldr", e);
  				this.collectInfo.log("分发", "name" + ": runSqlldr", e);
  			}
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
	 * 创建SQLLDR必要条件
	 * @param tableIndex
	 * @param tempFile
	 */
	@SuppressWarnings({ "resource", "unchecked" })
	public void buildSqlLdr(int tableIndex, String tempFile)
	{
		String logStr = null;
		DistributeTemplet.TableTemplet tableInfo = (DistributeTemplet.TableTemplet)this.disTmp.tableTemplets.get(Integer.valueOf(tableIndex));
    
		String currentPath = SystemConfig.getInstance().getCurrentPath();
		String charSet = SystemConfig.getInstance().getSqlldrCharset();

		int retCode = -1;

		File txttempfile = new File(currentPath, tempFile + ".txt");
		if (!txttempfile.exists())
		{
			logStr = this.collectInfo.getDescribe() + ": " + 
				txttempfile.getAbsolutePath() + " 不存在.";
			this.log.debug(logStr);
			this.collectInfo.log("入库", logStr);
			return;
		}

		if (txttempfile.length() == 0L)
		{
			logStr = this.collectInfo.getDescribe() + ": " + 
				txttempfile.getAbsolutePath() + " 内容为空.";
			this.log.debug(logStr);
			this.collectInfo.log("入库", logStr);
			txttempfile.delete();
			return;
		}

		try
		{
			BufferedWriter bw = new BufferedWriter(
					new FileWriter(currentPath + 
							File.separatorChar + tempFile + ".ctl", false));

			File txtFile = new File(currentPath + File.separatorChar + tempFile + ".txt");
			InputStream in = new FileInputStream(txtFile);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String splitSign = ";";
			String firstLine = br.readLine();
			try
			{
				String DeviceID = String.valueOf(this.collectInfo.getDevInfo().getDevID());
				splitSign = firstLine.substring(firstLine.indexOf(DeviceID) + 
						DeviceID.length(), firstLine.indexOf(DeviceID) + 
						DeviceID.length() + 1);
			}
			catch (Exception e)
			{
				logStr = "get splitSign error:" + e.getMessage();
				this.log.error(logStr);
				this.collectInfo.log("入库", logStr, e);
			}
			if (br != null)
			{
				br.close();
			}
			if (in != null)
			{
				in.close();
			}
			List<String> raws = new ArrayList<String>();
			Map<String, List<?>> colnameToIndex = new HashMap<String, List<?>>();
			if (firstLine != null)
			{
				String[] items = firstLine.split(";");
				for (String s : items)
				{
					if (!Util.isNotNull(s))
						continue;
					raws.add(s.trim());
				}

				for (String s : raws)
				{
					if (colnameToIndex.containsKey(s))
						continue;
					List<?> index = findDuplicateCol(raws, s);
					if (index == null)
						continue;
					colnameToIndex.put(s, index);
				}
			}

//			if (Util.isOracle())
			{
				bw.write("load data\r\n");

				if ((charSet != null) && (charSet.length() > 0))
				{
					bw.write("CHARACTERSET " + charSet + " \r\n");
				}
				else 
				{
					bw.write("CHARACTERSET AL32UTF8 \r\n");
				}
				
				bw.write("infile '" + currentPath + File.separatorChar + 
						tempFile + ".txt' ");
				bw.write("append into table " + tableInfo.tableName + " \r\n");
				bw.write("FIELDS TERMINATED BY \";\"\r\n");
				bw.write("TRAILING NULLCOLS\r\n");
				bw.write("(");
				Object strFieldMap;
				if (this.disTmp.stockStyle == 3)
				{
					String m_RawColumnsList = ReadFileFirstLine(tempFile);
					if (m_RawColumnsList == null) {
						return;
					}

					String[] FieldMappingList = m_RawColumnsList.split(";");
					String StrNewFieldList = "";
					for (int i = 0; i < FieldMappingList.length; i++)
					{
						strFieldMap = FieldMappingList[i].trim();
						if (((String)strFieldMap).trim() == "") 
						{
							continue;
						}
						for (int j = 0; j < tableInfo.fields.size(); j++)
						{
							DistributeTemplet.FieldTemplet field = (DistributeTemplet.FieldTemplet)tableInfo.fields.get(Integer.valueOf(j));
							if (!field.m_strFieldMapping.equals(strFieldMap)) 
							{
								continue;
							}
							switch (field.m_nDataType)
							{
								case 1:
									StrNewFieldList = StrNewFieldList + field.m_strFieldName + ",";
									break;
								case 2:
									StrNewFieldList = StrNewFieldList + field.m_strFieldName + ",";
									break;
								case 3:
									StrNewFieldList = StrNewFieldList + field.m_strFieldName + 
										" Date 'YYYY-MM-DD HH24:MI:SS',";
									break;
								case 4:
									StrNewFieldList = StrNewFieldList + field.m_strFieldName + 
									" LOBFILE(LOBF_00006) TERMINATED BY EOF,";
									break;
								case 5://时间格式（精确到毫秒）
									StrNewFieldList = StrNewFieldList + field.m_strFieldName + 
									" TIMESTAMP '" + field.m_strDataTimeFormat + "'";
									break;
								default:
									break;
							}
						}
					}

					if (StrNewFieldList.length() >= 1)
						StrNewFieldList = StrNewFieldList.substring(0, StrNewFieldList.length() - 1);
					bw.write(StrNewFieldList);
				}
				else if (isCreateCtlByRawName(tableInfo.fields))
				{
					//2013-01-11 OMCID -> DEVICEID Modified by turk
					StringBuilder tmp = new StringBuilder();
					tmp.append("DEVICEID,COLLECTTIME Date 'YYYY-MM-DD HH24:MI:SS',STAMPTIME Date 'YYYY-MM-DD HH24:MI:SS',");
					int i = -1;
					Object nullList = new ArrayList<Object>();
					for (strFieldMap = raws.iterator(); ((Iterator<?>)strFieldMap).hasNext(); ) 
					{ 
						String rawName = (String)((Iterator<?>)strFieldMap).next();
						boolean flag = false;
						i++;
						if (colnameToIndex.containsKey(rawName))
						{
							List<?> index = (List<?>)colnameToIndex.get(rawName);
							for (Iterator<?> localIterator2 = index.iterator(); localIterator2.hasNext(); ) 
							{ 
								int ix = ((Integer)localIterator2.next()).intValue();
								if (i != ix)
									continue;
								flag = true;
								break;
							}
						}

						if (flag)
						{
							continue;
						}

						DistributeTemplet.FieldTemplet field = findFieldTemplet(tableInfo.fields, rawName);
						if (field == null)
						{
							if (i <= 2)
								continue;
							((List<Integer>)nullList).add(Integer.valueOf(i));
						}
						else
						{
							switch (field.m_nDataType)
							{
								case 1:
									tmp.append(field.m_strFieldName);
									break;
								case 2:
									tmp.append(field.m_strFieldName + " CHAR(" + 
											field.m_strDataTimeFormat + ")");
									break;
								case 3:
									if (field.m_strFieldName.equals("COLLECTTIME"))
									{
										tmp.append("COLLECT_TIME Date 'YYYY-MM-DD HH24:MI:SS'");
									}
									else
									{
										tmp.append(field.m_strFieldName + 
											" Date 'YYYY-MM-DD HH24:MI:SS'");
									}
									break;
								case 4:
									tmp.append(field.m_strFieldName + 
									" LOBFILE(LOBF_00006) TERMINATED BY EOF ");
									break;
								case 5:
									tmp.append(field.m_strFieldName + 
									" TIMESTAMP '" + field.m_strDataTimeFormat + "'");
									break;
							}

							if (field.m_bIsDefault)
							{
								bw.write(" " + field.m_strDefaultValue);
							}
							if(field.m_DataLength > 0)
							{
								tmp.append(" CHAR(" + field.m_DataLength + ")");
							}
							tmp.append(",");
						}
					}

					if (((List<?>)nullList).size() > 0)
					{
						String txtName = currentPath + File.separatorChar + 
								tempFile + ".txt";
						File txt = new File(txtName);
						InputStream is = new FileInputStream(txt);
						BufferedReader reader = new BufferedReader(new InputStreamReader(is));
						String tmpFile = txtName + ".tmp";
						PrintWriter pw = new PrintWriter(tmpFile);
						String str = null;
						while ((str = reader.readLine()) != null)
						{
							pw.println(str);
							pw.flush();
						}
						pw.close();
						is.close();
						reader.close();
						txt.delete();

						is = new FileInputStream(tmpFile);
						reader = new BufferedReader(new InputStreamReader(is));
						pw = new PrintWriter(txtName);
						while ((str = reader.readLine()) != null)
						{
							String[] items = str.split(splitSign);
							StringBuilder sb = new StringBuilder();
							for (int j = 0; j < items.length; j++)
							{
								boolean flag = false;
								for (Integer index : (List<Integer>)nullList)
								{
									if (j != index.intValue())
										continue;
									flag = true;
								}

								if (flag)
									continue;
								sb.append(items[j]).append(splitSign);
							}

							sb.deleteCharAt(sb.length() - 1);
							pw.println(sb);
							pw.flush();
						}
						pw.close();
						is.close();
						reader.close();
						new File(tmpFile).delete();
					}

					if (tmp.charAt(tmp.length() - 1) == ',')
					{
						tmp.deleteCharAt(tmp.length() - 1);
					}
					bw.write(tmp.toString());
				}
				else
				{
					for (int i = 0; i < tableInfo.fields.size(); i++)
					{
						DistributeTemplet.FieldTemplet field = (DistributeTemplet.FieldTemplet)tableInfo.fields.get(Integer.valueOf(i));
						switch (field.m_nDataType)
						{
							case 1:
								bw.write(field.m_strFieldName);
								break;
							case 2:
								bw.write(field.m_strFieldName + " CHAR(" + 
										field.m_strDataTimeFormat + ")");
								break;
							case 3: //时间格式，精确到秒
								bw.write(field.m_strFieldName + 
								" Date 'YYYY-MM-DD HH24:MI:SS'");
								break;
							case 4:
								bw.write(field.m_strFieldName + 
								" LOBFILE(LOBF_00006) TERMINATED BY EOF ");
								
							case 5://时间格式（精确到毫秒）
								bw.write(field.m_strFieldName + 
										" TIMESTAMP '" + field.m_strDataTimeFormat + "' ");
								break;
						}

						if (field.m_bIsDefault)
						{
							bw.write(" " + field.m_strDefaultValue);
						}
						//Turk Add 字段长度
						if(field.m_DataLength > 0)
						{
							bw.write(" CHAR(" + field.m_DataLength + ")");
						}
						if (i < tableInfo.fields.size() - 1) {
							bw.write(",");
						}
					}
				}

				bw.write(")\r\n");
				bw.close();
        
				Date now = new Date();
//				this.collectInfo.setSqlldrTime(new Timestamp(now.getTime()));
				retCode = RunSqlldr(tableIndex, tempFile);
			}
//			else if ((Util.isSybase()) || (Util.isSqlServer()))
//			{
//				Map<?, ?> columns = CommonDB.GetTableColumns(tableInfo.tableName);
//				bw.write("10.0\r\n");
//				int nField = tableInfo.fields.size();
//				bw.write(String.valueOf(nField) + "\r\n");
//
//				for (int i = 1; i <= nField; i++)
//				{
//					bw.write(i + "\tSYBCHAR\t0\t128\t");
//					if (i < nField)
//					{
//						bw.write("\";\"");
//					}
//					else
//					{
//						bw.write("\"\n\"");
//					}
//
//					String strField = ((DistributeTemplet.FieldTemplet)tableInfo.fields.get(Integer.valueOf(i - 1))).m_strFieldName;
//					int j = 0;
//					for (; j < columns.size(); j++)
//					{
//						if (strField.equalsIgnoreCase((String)columns.get(Integer.valueOf(j))))
//						{
//							break;
//						}
//					}
//					bw.write("\t" + (j + 1) + "\t" + strField + "\r\n");
//				}
//				bw.close();
//				Date now = new Date();
//				this.collectInfo.setSqlldrTime(new Timestamp(now.getTime()));
//				RunBcp(tableInfo.tableName, tempFile);
//			}
//			else if(Util.isMySQL())
//			{
//				RunMySQLLoadData(tableIndex, tableInfo.tableName, tempFile);
//			}
				
			if (SystemConfig.getInstance().isDeleteLog())
			{
				File ctlfile = new File(currentPath, tempFile + ".ctl");
				if (ctlfile.exists()) {
					ctlfile.delete();
				}

				String strTxt = currentPath + File.separatorChar + tempFile + ".txt";
				File txtfile = new File(strTxt);
				if (txtfile.exists())
				{
					if (txtfile.delete())
					{
						logStr = this.collectInfo.getDescribe() + ": " + strTxt + "删除成功....";
						this.log.debug(logStr);
						this.collectInfo.log("入库", logStr);
					}
					else
					{
						logStr = this.collectInfo.getDescribe() + ": " + strTxt + "删除失败";
						this.log.warn(logStr);
						this.collectInfo.log("入库", logStr);
						
						if(txtfile.getAbsoluteFile().delete())
						{
							logStr = this.collectInfo.getDescribe() + ": " + strTxt + "重新删除成功....";
							this.log.debug(logStr);
							this.collectInfo.log("入库", logStr);
						}
						else
						{
							logStr = this.collectInfo.getDescribe() + ": " + strTxt + "重新删除失败";
							this.log.warn(logStr);
							this.collectInfo.log("入库", logStr);
						}
					}
				}
				else
				{
					logStr = this.collectInfo.getDescribe() + ": " + strTxt + 
						"未找到，无法删除";
					this.log.debug(logStr);
					this.collectInfo.log("入库", logStr);
				}

				if (retCode == 0)
				{
					File txtlog = new File(currentPath + File.separatorChar + 
							"ldrlog", tempFile + ".log");
					if (txtlog.exists())
						txtlog.delete();
				}
			}
		}
		catch (Exception e)
		{
			this.log.error("BuildSqlLdr", e);
			this.collectInfo.log("入库", "BuildSqlLdr", e);
		}
	}

	private boolean isCreateCtlByRawName(Map<Integer, DistributeTemplet.FieldTemplet> fields)
	{
		Collection<DistributeTemplet.FieldTemplet> fs = fields.values();
		for (DistributeTemplet.FieldTemplet f : fs)
		{
			if ((Util.isNull(f.rawName)) && 
					(!f.m_strFieldName.equalsIgnoreCase("omcid")) && 
					(!f.m_strFieldName.equalsIgnoreCase("collecttime")) && 
					(!f.m_strFieldName.equalsIgnoreCase("stamptime"))) 
				return false;
		}
		return true;
	}

	@SuppressWarnings("rawtypes")
	private DistributeTemplet.FieldTemplet findFieldTemplet(Map<Integer, DistributeTemplet.FieldTemplet> fields, String rawName)
	{
		Map<FieldTemplet, Float> tmp = new HashMap<FieldTemplet, Float>();
		Collection<DistributeTemplet.FieldTemplet> fs = fields.values();
		for (DistributeTemplet.FieldTemplet f : fs)
		{	
			float dis = LevenshteinDistance.similarity(rawName, f.rawName);
			if (dis < SystemConfig.getInstance().getFieldMatch())
				continue;
			tmp.put(f, Float.valueOf(dis));
		}

		Iterator<?> it = tmp.entrySet().iterator();
		Map.Entry max = null;
		while (it.hasNext())
		{
			Map.Entry et = (Map.Entry)it.next();
			if ((max != null) && (((Float)et.getValue()).floatValue() <= ((Float)max.getValue()).floatValue()))
				continue;
			max = et;
		}

		return max == null ? null : (DistributeTemplet.FieldTemplet)max.getKey();
	}

	public String ReadFileFirstLine(String TempFile)
	{
		String strCurrentPath = SystemConfig.getInstance().getCurrentPath();

		String strLine = "";
		try
		{
			FileReader reader = new FileReader(strCurrentPath + "/" + TempFile + ".txt");
			BufferedReader br = new BufferedReader(reader);
			strLine = br.readLine();
			br.close();
			reader.close();
		}
		catch (Exception e)
		{
			this.log.error(this.collectInfo.getDescribe() + " : ReadFileFirstLine", e);
		}

		return strLine;
	}

	public void RunSqlldr(String strCtlName)
	{
		RunSqlldr(0, strCtlName);
	}

	/**
	 * Oracle 入库方式
	 * @param tableIndex
	 * @param strCtlName
	 * @return
	 */
	public int RunSqlldr(int tableIndex, String strCtlName)
	{
		String logStr = null;
		String strCurrentPath = SystemConfig.getInstance().getCurrentPath();
		
		String strOracleBase = SystemConfig.getInstance().getDbService();
		String strOracleUserName = SystemConfig.getInstance().getDbUserName();
		String strOraclePassword = SystemConfig.getInstance().getDbPassword();
		
//		String strOracleBase = collectInfo.getInDBServerConfig().getInDBServer();
//		if(strOracleBase.isEmpty())
//		{
//			strOracleBase = SystemConfig.getInstance().getDbService();
//		}
//		
//		String strOracleUserName = collectInfo.getInDBServerConfig().getInDBUser();
//		if(strOracleUserName.isEmpty())
//		{
//			strOracleUserName = SystemConfig.getInstance().getDbUserName();
//		}
//		
//		String strOraclePassword = collectInfo.getInDBServerConfig().getInDBPassword();
//		if(strOraclePassword.isEmpty())
//		{
//			strOraclePassword = SystemConfig.getInstance().getDbPassword();
//		}
		
		int retCode = -1;
		String cmd = String.format("sqlldr userid=%s/%s@%s skip=1 " +
				"control=%s%s%s.ctl bad=%s%s%s.bad log=%s%sldrlog%s%s.log " +
				"rows=500 "+ SystemConfig.getInstance().getreadsize() + " errors=999999", 
				new Object[] { strOracleUserName, 
			strOraclePassword, 
			strOracleBase, 
			strCurrentPath, 
			Character.valueOf(File.separatorChar), 
			strCtlName, 
			strCurrentPath, 
			Character.valueOf(File.separatorChar), 
			strCtlName, 
			strCurrentPath, 
			Character.valueOf(File.separatorChar), 
			Character.valueOf(File.separatorChar), 
			strCtlName });
		
		
		try
		{
			
			
			BalySqlloadThread sqlthread = new BalySqlloadThread();
			//sqlthread.setM_TaskInfo(this.collectInfo);
			sqlthread.setTableIndex(tableIndex);

			logStr = this.collectInfo.getDescribe() + ": " + 
				cmd.replace(strOracleUserName, "*").replace(strOraclePassword, "*");
			this.log.debug(logStr);
			this.collectInfo.log("入库", logStr);

			retCode = sqlthread.runcmd(cmd);
			if ((retCode == 0) || (retCode == 2))
			{
				logStr = this.collectInfo.getDescribe() + ": sqldr OK. retCode=" + retCode;
				this.log.debug(logStr);
				this.collectInfo.log("入库", logStr);
			}
			else if ((retCode != 0) && (retCode != 2))
			{
				int maxTryTimes = 3;
				int tryTimes = 0;
				long waitTimeout = 30000L;
				while (tryTimes < maxTryTimes)
				{
					retCode = sqlthread.runcmd(cmd);
					if ((retCode == 0) || (retCode == 2))
					{
						break;
					}

					tryTimes++;
					waitTimeout *= 2L;

					logStr = this.collectInfo.getDescribe() + ": 第" + tryTimes + 
						"次Sqlldr尝试入库失败. " + cmd + " retCode=" + retCode;
					this.log.error(logStr);
					this.collectInfo.log("入库", logStr);
					Thread.currentThread(); Thread.sleep(waitTimeout);
				}

				if ((retCode == 0) || (retCode == 2))
				{
					logStr = this.collectInfo.getDescribe() + ": " + tryTimes + 
						"次Sqlldr尝试入库后成功. retCode=" + retCode;
					this.log.info(logStr);
					this.collectInfo.log("入库", logStr);
				}
				else
				{
					logStr = this.collectInfo.getDescribe() + " : " + tryTimes + 
						"次Sqlldr尝试入库失败. " + cmd + " retCode=" + retCode;
					this.log.error(logStr);
					this.collectInfo.log("入库", logStr);

//					AlarmMgr.getInstance().insert(this.collectInfo.getTaskID(),(byte)1, "sqlldr 失败 重试" + 
//							tryTimes + "次", this.collectInfo.getSysName() + 
//							" 返回值=" + retCode, cmd, 30103);
				}
			}
			else
			{
				logStr = this.collectInfo.getDescribe() + ": sqlldr 失败 并且不重试.";
				this.log.error(logStr);
				this.collectInfo.log("入库", logStr);
        
//				AlarmMgr.getInstance().insert(this.collectInfo.getTaskID(),(byte)1, "sqlldr 失败 并且不重试", this.collectInfo.getSysName() + 
//						" 返回值=" + retCode, cmd, 30101);
			}
		}	
		catch (Exception e)
		{
			logStr = this.collectInfo.getDescribe() + ": sqlldr exception. " + cmd;
			this.log.error(logStr, e);
			this.collectInfo.log("入库", logStr, e);

//			AlarmMgr.getInstance().insert(this.collectInfo.getTaskID(),(byte)1, "sqlldr 异常", this.collectInfo.getSysName(), cmd + 
//					e.getMessage(), 30102);
		}

		String logFileName = strCurrentPath + File.separator + "ldrlog" + File.separator + strCtlName + ".log";
		File logFile = new File(logFileName);
		if ((!logFile.exists()) || (!logFile.isFile()))
		{
			logStr = this.collectInfo.getDescribe() + ": " + logFileName + "不存在.";
			this.log.info(logStr);
			this.collectInfo.log("入库", logStr);
			return retCode;
		}
		SqlLdrLogAnalyzer analyzer = new SqlLdrLogAnalyzer();
		try
		{
			SqlldrResult result = analyzer.analysis(new FileInputStream(logFileName));
			if (result == null) {
				return retCode;
			}
			
			//获取入库文件大小
			File file = new File(strCurrentPath+Character.valueOf(File.separatorChar)+strCtlName+".txt");
			double fileBytes = 0;
			if (file.exists()) { 
				FileInputStream fis = null; 
				fis = new FileInputStream(file); 
				fileBytes = fis.available(); 
				fileBytes = (double)fileBytes/(double)1024;
				fileBytes = Util.round(fileBytes, 3, BigDecimal.ROUND_HALF_DOWN);
				fis.close();
			}
			
			logStr = this.collectInfo.getDescribe() + ": SQLLDR日志分析结果: DeviceID=" + 
			this.collectInfo.getDevInfo().getDevID() + " 表名=" + 
			result.getTableName() + " 数据时间=" + 
			Util.getDateString(this.collectInfo.getLastCollectTime()) + 
				" 文件大小=" + fileBytes + "KB 文件入库时长=" + result.getRunTime() + "(s) 入库成功条数=" + result.getLoadSuccCount() + " sqlldr日志=" + 
				logFileName;
			this.log.debug(logStr);
			this.collectInfo.log("入库", logStr);

//			dbLogger.log(this.collectInfo.getDevInfo().getDevID(), 
//					result.getTableName(), this.collectInfo.getLastCollectTime().getTime(), 
//					result.getLoadSuccCount(), this.collectInfo.getTaskID(), 
//					result.getRunTime(),fileBytes);
		}
		catch (Exception e)
		{
			logStr = this.collectInfo.getDescribe() + ": sqlldr日志分析失败，文件名：" + 
				logFileName + "，原因: ";
			this.log.error(logStr, e);
			this.collectInfo.log("入库", logStr, e);
		}	
		return retCode;
	}

	/**
	 * SQLSERVER OR SYSBASE 入库方式
	 * @param strTable
	 * @param strFormat
	 */
	private void RunBcp(String strTable, String strFormat)
	{
		try
		{
			String strCurrentPath = SystemConfig.getInstance().getCurrentPath();
			String strUrl = SystemConfig.getInstance().getDbUrl();
			String strBase = strUrl.substring(strUrl.lastIndexOf("/") + 1);
			String strUserName = SystemConfig.getInstance().getDbUserName();
			String strPassword = SystemConfig.getInstance().getDbPassword();
			String strService = SystemConfig.getInstance().getDbService();
			String strLog = strCurrentPath + File.separatorChar + "ldrlog" + 
				File.separatorChar + strFormat + ".log";
			String strDataFile = strCurrentPath + File.separatorChar + 
				strFormat + ".txt";

			String cmd = String.format("bcp %s..%s in \"%s\" -U%s -P%s -S%s -t; -r\\n -c -e %s", new Object[] { strBase, strTable, strDataFile, strUserName, strPassword, strService, strLog });

			Process ldr = Runtime.getRuntime().exec(cmd);
				ldr.waitFor();
		}
		catch (Exception e)
		{
			this.log.error("BCP Error!", e);
		}
	}

	@Override
	public boolean DistributeData(byte[] bData, int tableIndex) {
		return false;
		    
	}
	
	
	/**
	 * MySql 入库方式
	 * @param strTable
	 * @param strFormat
	 */
//	private void RunMySQLLoadData(int tableIndex,String strTable, String strFormat)
//	{
//		try
//		{
//			String strCurrentPath = SystemConfig.getInstance().getCurrentPath();
//
//			String strHost = SystemConfig.getInstance().getDbService();
//			String strInDB = collectInfo.getInDBServerConfig().getInDBServer();
//			if(strInDB.isEmpty())
//			{
//				strInDB = SystemConfig.getInstance().getDbService();
//			}
//			
//			String strInDBUser = collectInfo.getInDBServerConfig().getInDBUser();
//			if(strInDBUser.isEmpty())
//			{
//				strInDBUser = SystemConfig.getInstance().getDbUserName();
//			}
//			
//			String strInDBPassword = collectInfo.getInDBServerConfig().getInDBPassword();
//			if(strInDBPassword.isEmpty())
//			{
//				strInDBPassword = SystemConfig.getInstance().getDbPassword();
//			}
//
//			String logStr = "";
//			
//			BalySqlloadThread sqlthread = new BalySqlloadThread();
//			sqlthread.setTableIndex(tableIndex);
//
//			String sourceFile = strCurrentPath + File.separatorChar + 
//			strFormat + ".txt";
//			String targetFile = strCurrentPath + File.separatorChar + strTable + ".txt";
//			Util.FileCopy(sourceFile, targetFile);
//			
//			String cmd = String.format("mysqlimport -h %s -L -u %s -p%s --fields-terminated-by=; --ignore-lines=1 %s %s", 
//					new Object[] { strHost, strInDBUser, strInDBPassword, strInDB, targetFile });
//			logStr = this.collectInfo.getSysName() + ": " + 
//				cmd.replace(strInDBUser, "*").replace(strInDBPassword, "*");
//			
//			this.log.debug(logStr);
//			this.collectInfo.log("入库", logStr);
//		
//			int retCode = sqlthread.runcmd(cmd);
//			
//			File delfile = new File(sourceFile);
//			if(delfile.delete())
//			{
//				this.log.debug("删除文件:"+sourceFile+" 成功");
//			}
//			
//			if ((retCode == 0) || (retCode == 2))
//			{
//				logStr = this.collectInfo.getSysName() + ": sqldr OK. retCode=" + retCode;
//				this.log.debug(logStr);
//				this.collectInfo.log("入库", logStr);
//			}
//			else if ((retCode != 0) && (retCode != 2))
//			{
//				int maxTryTimes = 3;
//				int tryTimes = 0;
//				long waitTimeout = 30000L;
//				while (tryTimes < maxTryTimes)
//				{
//					retCode = sqlthread.runcmd(cmd);
//					if ((retCode == 0) || (retCode == 2))
//					{
//						break;
//					}
//
//					tryTimes++;
//					waitTimeout *= 2L;
//
//					logStr = this.collectInfo.getSysName() + ": 第" + tryTimes + 
//						"次Sqlldr尝试入库失败. " + cmd + " retCode=" + retCode;
//					this.log.error(logStr);
//					this.collectInfo.log("入库", logStr);
//					Thread.currentThread(); Thread.sleep(waitTimeout);
//				}
//
//				if ((retCode == 0) || (retCode == 2))
//				{
//					logStr = this.collectInfo.getSysName() + ": " + tryTimes + 
//						"次Sqlldr尝试入库后成功. retCode=" + retCode;
//					this.log.info(logStr);
//					this.collectInfo.log("入库", logStr);
//				}
//				else
//				{
//					logStr = this.collectInfo.getSysName() + " : " + tryTimes + 
//						"次Sqlldr尝试入库失败. " + cmd + " retCode=" + retCode;
//					this.log.error(logStr);
//					this.collectInfo.log("入库", logStr);
//
//					AlarmMgr.getInstance().insert(this.collectInfo.getTaskID(),(byte)1, "sqlldr 失败 重试" + 
//							tryTimes + "次", this.collectInfo.getSysName() + 
//							" 返回值=" + retCode, cmd, 30103);
//				}
//			}
//			else
//			{
//				logStr = this.collectInfo.getSysName() + ": sqlldr 失败 并且不重试.";
//				this.log.error(logStr);
//				this.collectInfo.log("入库", logStr);
//        
//				AlarmMgr.getInstance().insert(this.collectInfo.getTaskID(),(byte)1, "sqlldr 失败 并且不重试", this.collectInfo.getSysName() + 
//						" 返回值=" + retCode, cmd, 30101);
//			}
//			
//			
//			//获取入库文件大小
//			File file = new File(targetFile);
//			double fileBytes = 0;
//			if (file.exists()) { 
//				FileInputStream fis = null; 
//				fis = new FileInputStream(file); 
//				fileBytes = fis.available(); 
//				fileBytes = (double)fileBytes/(double)1024;
//				fileBytes = Util.round(fileBytes, 3, BigDecimal.ROUND_HALF_DOWN);
//				fis.close();
//			}
//			
//			logStr = this.collectInfo.getSysName() + ": SQLLDR日志分析结果: DeviceID=" + 
//			this.collectInfo.getDevInfo().getDevID() + " 表名=" + 
//			strTable.toUpperCase() + " 数据时间=" + 
//			Util.getDateString(this.collectInfo.getLastCollectTime()) + 
//				" 文件大小=" + fileBytes + "KB 入库成功条数=" + sqlthread.getRecordNum();
//			this.log.debug(logStr);
//			this.collectInfo.log("入库", logStr);
//
//			dbLogger.log(this.collectInfo.getDevInfo().getDevID(), 
//					strTable.toUpperCase(), this.collectInfo.getLastCollectTime().getTime(), 
//					sqlthread.getRecordNum(), this.collectInfo.getTaskID(), 
//					0,fileBytes);
//			
//		}
//		catch (Exception e)
//		{
//			this.log.error("MySQLLoadData Error!", e);
//		}
//	}

}

    