package com.thetado.core.taskmanage;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;

import org.apache.log4j.Logger;

import com.thetado.utils.Task;
import com.thetado.core.access.AbstractAccessor;
import com.thetado.core.datalog.DataLogInfo;
import com.thetado.core.template.AbstractTempletBase;
import com.thetado.core.template.TempletRecord;
import com.thetado.utils.ConstDef;
import com.thetado.utils.ThreadPool;




/**
 * ETL 任务信息对象
 * @author Administrator
 *
 */
public class TaskInfo implements Serializable{

	private static final long serialVersionUID = 2652378647623428865L;
	
	private Logger log = Logger.getLogger(TaskInfo.class);
	private Task threadHandle = null;
	
	private String sysName;
	
	public TaskInfo(int taskID)
  	{
  		this.taskId = taskID;
  		this.keyID = taskID;
  		this.sysName = String.valueOf(this.taskId);
  	}
	
	private TempletRecord parseTmpRecord;
	private TempletRecord distTmpRecord;
	
	private DataLogInfo logInfo = new DataLogInfo();
	
	private int keyID = 0;
	
	private Timestamp startTime;
	
	/**
	 * 任务编号
	 */
	public void setKeyID(int keyid){
		keyID = keyid;
	}
	
	/**
	 * 任务编号
	 */
	public int getKeyID() {
		return keyID;
	}
	
	public void setStartTime(Timestamp starttime) {
		startTime = starttime;
	}
	
	public Timestamp getStartTime() {
		return startTime;
	}


	private int groupId = 0;
	
	/**
	 * 任务组编号
	 */
	public void setGroupID(int groupid) {
		groupId = groupid;
	}
	
	/**
	 * 任务组编号
	 */
	public int getGroupID() {
		return groupId;
	}
	
	
	private String Describe;
	
	public void setDescribe(String describe) {
		describe = Describe;
	}
	
	public String getDescribe() {
		return Describe;
	}
	
	private int devPort;
	
	public void setDevPort(int devport) {
		devPort = devport;
	}
	
	public int getDevPort() {
		return devPort;
	}
	
	private int taskId = 0;
	
	/**
	 * 任务编号，数据库配置表中配置
	 */
	public void setTaskID(int taskid){
		taskId = taskid;
	}
	/**
	 * 任务编号，数据库配置表中配置
	 */
	public int getTaskID() {
		return taskId;
	}
	
	
	private DevInfo devInfo;
	
	/**
	 * 数据源信息配置,配置数据源服务器相关信息
	 */
	public void setDevInfo(DevInfo devinfo){
		devInfo = devinfo;
	}
	
	/**
	 * 数据源信息配置,配置数据源服务器相关信息
	 */
	public DevInfo getDevInfo() {
		return devInfo;
	}
	
	/**
	 * 数据源信息配置(代理)
	 */
	private DevInfo proxyDevInfo;
	private int proxyDevPort = 0;
	
	
	private int collectType = 0;
	
	/**
	 * 数据采集类型
	 */
	public void setCollectType(int collecttype) {
		collectType = collecttype;
	}
	
	/**
	 * 数据采集类型
	 */
	public int getCollectType() {
		return collectType;
	}
	
	
	private int collectTimeOut = 0;
	
	/**
	 * 采集超时时间
	 */
	public void setCollectTimeOUT(int collecttimeout) {
		collectTimeOut = collecttimeout;
	}
	
	/**
	 * 采集超时时间
	 */
	public int getCollectTimeOUT() {
		return collectTimeOut;
	}
	
	
	
	private int collectPeriod = 0;
	
	/**
	 * 采集周期
	 */
	public void setCollectPeriod(int collectperiod) {
		collectPeriod = collectperiod;
	}
	/**
	 * 采集周期
	 */
	public int getCollectPeriod() {
		return collectPeriod;
	}
	
	
	private int collectTime = 0;
	public void setCollectTime(int collecttime) {
		collectTime = collecttime;
	}
	
	public int getCollectTime() {
		return collectTime;
	}
	
	private int collectTimePos = 0;
	
	public void setCollectTimePos(int collecttimepos) {
		collectTimePos = collecttimepos;
	}
	
	public int getCollectTimePos() {
		return collectTimePos;
	}
	
	
	private String collectPath;
	/**
	 * 采集路径，文件路径，如果是数据库采集配置SQL
	 */
	public void setCollectPath(String collectpath) {
		collectPath = collectpath;
	}
	/**
	 * 采集路径，文件路径，如果是数据库采集配置SQL
	 */
	public String getCollectPath() {
		return collectPath;
	}
	
	
	private String shellCmdPrepare = "";
	/**
	 * 执行采集命令前在源数据服务器上执行的命令
	 */
	public void setShellCMDPrepare(String shellcmdprepare) {
		shellCmdPrepare = shellcmdprepare;
	}
	/**
	 * 执行采集命令前在源数据服务器上执行的命令
	 */
	public String getShellCMDPrepare() {
		return shellCmdPrepare;
	}
	
	
	private String shellCmdFinish = "";
	
	/**
	 * 执行采集命令后在源数据服务器上执行的命令
	 */
	public void setShellCMDFinish(String shellcmdfinish) {
		shellCmdFinish = shellcmdfinish;
	}
	
	/**
	 * 执行采集命令后在源数据服务器上执行的命令
	 */
	public String getShellCMDFinish() {
		return shellCmdFinish;
	}
		
	
	private int shellTimeOut;
	/**
	 * 命令执行超时
	 */
	public void setShellTimeOut(int shelltimeout){
		shellTimeOut = shelltimeout;
	}
	/**
	 * 命令执行超时
	 */
	public int getShellTimeOut() {
		return shellTimeOut;
	}
	
	
	private int parseTmpID = 0;
	/**
	 * 解析模板ID
	 */
	public void setParesTmpID(int parsetmpid) {
		parseTmpID = parsetmpid;
	}
	/**
	 * 解析模板ID
	 */
	public int getParesTmpID() {
		return parseTmpID;
	}
	
	
	private int parseTmpType = 0;
	/**
	 * 解析类型
	 */
	public void setParesTmpType(int parsetmptype) {
		parseTmpType = parsetmptype;
	}
	/**
	 * 解析类型
	 */
	public int getParesTmpType() {
		return parseTmpType;
	}
	
	//解析模板
	private AbstractTempletBase parseTemplet;
	/**
	 * 解析模板
	 * @param parsetemplet
	 * @Description:
	 */
	public void setParseTemplet(AbstractTempletBase parsetemplet) {
		parseTemplet = parsetemplet;
	}
	
	/**
	 * 解析模板
	 * @return
	 * @Description:
	 */
	public AbstractTempletBase getParseTemplet() {
		return parseTemplet;
	}
	
	private int disTmpID = 0;
	
	/**
	 * 分发模板ID
	 * @param distmpid
	 * @Description:
	 */
	public void setDistributeTmpID(int distmpid) {
		disTmpID = distmpid;
	}
	/**
	 * 分发模板ID
	 * @return
	 * @Description:
	 */
	public int getDistributeTmpID() {
		return disTmpID;
	}
	
	//分发模板
	private AbstractTempletBase distributeTemplet;
	/**
	 * 分发模板
	 * @param distributetemplet
	 * @Description:
	 */
	public void setDistributeTemplet(AbstractTempletBase distributetemplet) {
		distributeTemplet = distributetemplet;
	}
	/**
	 * 分发模板
	 * @return
	 * @Description:
	 */
	public AbstractTempletBase getDistributeTemplet() {
		return distributeTemplet;
	}
	
	private int redoTimeOffset = 0;
	public void setRedoTimeOffset(int redotimeoffset) {
		redoTimeOffset = redotimeoffset;
	}
	
	public int getRedoTimeOffset() {
		return redoTimeOffset;
	}
	
	private int parserID;
	public void setParserID(int parserid){
		parserID = parserid;
	}
	
	public int getParserID() {
		return parserID;
	}
	
  	private int distributorID;
  	public void setDistributorID(int distributorid) {
  		distributorID = distributorid;
  	}
  	public int getDistributorID() {
  		return distributorID;
  	}
  	
  	
  	private Timestamp lastCollectTime;
  	public void setLastCollectTime(Timestamp lastcollecttime) {
  		lastCollectTime = lastcollecttime;
  	}
  	
  	public Timestamp getLastCollectTime() {
  		return lastCollectTime;
  	}
  	
  	private int lastCollectPos;
  	public void setLastCollectPos(int lastcollectpos) {
  		lastCollectPos = lastcollectpos;
  	}
  	public int getLastCollectPos() {
  		return lastCollectPos;
  	}
  	
  	private boolean usedFlag = false;
  	public void setUsedFlag(boolean usedflag) {
  		usedFlag = usedflag;
  	}
  	
  	public boolean getUsedFlag() {
  		return usedFlag;
  	}
  	
  	private int maxReCollectTime = 0;
  	public void setMaxReCollectTime(int maxrecollecttime) {
  		maxReCollectTime = maxrecollecttime;
  	}
  	
  	public int getMaxReCollectTime() {
  		return maxReCollectTime;
  	}

  	private int activeTableIndex = -1;
  	public void setActiveTableIndex(int activetableindex) {
  		activeTableIndex = activetableindex;
  	}
  	
  	public int getActiveTableIndex() {
  		return activeTableIndex;
  	}

  	private String dbDriver = "";
  	public void setDBDriver(String dbdriver) {
  		dbDriver = dbdriver;
  	}
  	public String getDBDriver() {
  		return dbDriver;
  	}
  	
  	private String dbUrl = "";
  	public void setDBUrl(String dburl) {
  		dbUrl = dburl;
  	}
  	
  	public String getDBUrl() {
  		return dbUrl;
  	}
  	
  	
  	private Timestamp sqlldrTime;
  	public void setSQLldrTime(Timestamp sqlldrtime) {
  		sqlldrTime = sqlldrtime;
  	}
  	public Timestamp getSQLldrTime() {
  		return sqlldrTime;
  	}
  	
  	private int threadSleepTime = 0;
  	public void setThreadSleepTime(int threadsleeptime) {
  		threadSleepTime = threadsleeptime;
  	}
  	public int getThreadSleepTime() {
  		return threadSleepTime;
  	}
  	
  	private int blockedTime;
  	public void setBlockedTime(int blockedtime) {
  		blockedTime = blockedtime;
  	}
  	public int getBlockedTime() {
  		return blockedTime;
  	}
  	
  	private String hostName;
  	public void setHostName(String hostname) {
  		hostName = hostname;
  	}
  	public String getHostName() {
  		return hostName;
  	}
  	
  	private Timestamp endDataTime = null;
  	public void setEndDataTime(Timestamp enddatatime) {
  		endDataTime = enddatatime;
  	}
  	public Timestamp getEndDataTime() {
  		return endDataTime;
  	}
  	
  	private int allRecordCount = 0;
  	
  	public void setAllRecordCount(int allrecordcount) {
  		allRecordCount = allRecordCount + allrecordcount;
  	}
  	
  	public int getAllRecordCount() {
  		return allRecordCount;
  	}
  	
  	/**
  	 * 构建采集任务
  	 * @param rs
  	 * @param scantime
  	 * @throws Exception
  	 */
  	public void buildObj(ResultSet rs, Date scantime) throws Exception
  	{
  		if (TaskMgr.getInstance().isActive(rs.getInt("TASK_ID"), false))
  		{
  			Logger.getLogger(TaskInfo.class).debug(this.sysName + " is active");
  			return;
  		}

  		buildObj(rs);

  		if (checkDataTime())
  		{
  			addTaskItem(scantime);
  		}
  	}
  	
  	/**
  	 * 检查采集任务的时间，是否可以进行采集
  	 * @return
  	 */
  	private boolean checkDataTime()
  	{
  		if (this.endDataTime == null) return true;
  		return this.lastCollectTime.getTime() <= this.endDataTime.getTime();
  	}
  	
  	/**
  	 * 添加采集任务
  	 * @param scantime
  	 */
  	protected void addTaskItem(Date scantime)
  	{
  		Calendar cal = Calendar.getInstance();
  		int minutes = cal.get(12);
  		int hours = cal.get(11);

  		boolean bAdd = false;
  		int time = -1;

  		switch (getCollectPeriod())
  		{
	  		case 1:
	  			bAdd = true;
	  			break;
		  	case 3://小时
		  	case 6:
		  	case 9://一分钟
		  	case 8:
		  		time = minutes;
		  		break;
		  	case 2://天
		  		time = hours;
		  		break;
		  	case 4://半小时
		  		time = minutes % 30;
		  		break;
		  	case 5://15分钟
		  		time = minutes % 15;
		  		break;
		  	case 7://5分钟
		  		time = minutes % 5;
		  		break;
		  	case 10://10分钟
		  		time = minutes % 10;
		  		break;
		  	default:
		  		this.log.debug(this.sysName + " : without period type.");
		  		return;
  		}	

  		if (time != -1) {
  			bAdd = isReady(time, scantime.getTime());
  		}
  		if (bAdd)
  		{
  			startTask();
	  	}
  	}

  	public void startTask()
  	{
  		if (TaskMgr.getInstance().addTask(this.getTaskID(),this.getLastCollectTime().getTime()))
  		{
  			AbstractAccessor accessor = Factory.createAccessor(this);
  			setCollectThread(accessor);
  			//accessor.s.setName("Thread:"+this.getDescribe() + "[" + this.getLastCollectTime() + "]");
  			//accessor.start();
  			accessor.setBeginExceuteTime(new Date());
  			this.startTime = new Timestamp(new Date().getTime());
  			accessor.setSubmitTime(new Date());
  			
  			ThreadPool.getInstance().addTask(accessor);
  			
  			//更新下一个采集时间 2013-06-03 Turk
  			long lastCollectTime = getLastCollectTime().getTime();
  			Timestamp timeStamp = new Timestamp(lastCollectTime + getPeriodTime());
  	  		saveLastCollectTime(timeStamp);
  		}
  	}
  	
  	public boolean saveLastCollectTime(Timestamp time)
  	{
//  		if(this.isPersistentTask)//如果是持续性采集任务，不需要更新采集日志
//  			return true;
  		
  		
  		//更新下一个任务表的时间，表示此任务已在任务队列中运行
//  		if(SystemConfig.getInstance().IsTaskUserXML())
//  		{
////  			TaskMgr.getInstance().setLastImportTimePosForXML(getTaskID(), time, 0);
//  		}
//  		else
  		{
  			TaskMgr.getInstance().setLastImportTimePos(getTaskID(), time, 0);
  		}
  		
  		String logStr = this.sysName + ": update stamptime :" + getDescribe() + "  " + time;
  		this.log.debug(logStr);
  		log("结束", logStr);
  		return true;
  	}
  	
	/**
  	 * 该任务的采集线程对象
  	 * @return
  	 */
  	public Task getCollectThread()
  	{
  		return this.threadHandle;
  	}
//
  	public void setCollectThread(Task hThreadHandle)
  	{
  		this.threadHandle = hThreadHandle;
  	}
  	
 	/**
  	 * 判断是否可运行采集，此处控制采集延时和采集具体时间点
  	 * @param unit
  	 * @param scanTime
  	 * @return
  	 */
  	protected boolean isReady(int unit, long scanTime)
  	{
  		boolean bReturn = false;

  		//采集具体启动时间，天采集为小时，小时采集为分钟
  		int collectTime = getCollectTime();
  		collectTime = collectTime > 59 ? 0 : collectTime;
  		
  		//采集启动偏移多少分钟开始采集
  		long startTime = getLastCollectTime().getTime() + 
  			getCollectTimePos() * 60 * 1000;

  		if (scanTime - startTime >= this.getPeriodTime())
  		{
//  			DelayProbeMgr.getTaskEntrys().remove(Integer.valueOf(getTaskID()));
  			bReturn = true;
  		}
  		else if ((unit >= collectTime) && (startTime < scanTime))
  		{
//  			DelayProbeMgr.getTaskEntrys().remove(Integer.valueOf(getTaskID()));
  			bReturn = true;
  		}

  		//if (!bReturn)
  		//{
  		//	TaskMgr.getInstance().tempTasks.add(this);
  		//}
  		return bReturn;
  	}
  	
  	/**
  	 * 构建采集任务
  	 * @param rs
  	 * @throws Exception
  	 */
  	protected void buildObj(ResultSet rs)
  		throws Exception
  	{
  		setGroupID(rs.getInt("GROUP_ID"));
	    setDescribe(rs.getString("Task_Describe"));
	    setTaskID(rs.getInt("TASK_ID"));
	    DevInfo devInfo = new DevInfo();
	    devInfo.setDevID(rs.getInt("DEVICEID"));
	    devInfo.setName(rs.getString("DEV_NAME"));
	    devInfo.setIP(rs.getString("HOST_IP"));
	    devInfo.setHostUser(rs.getString("HOST_USER"));
	    devInfo.setHostPwd(rs.getString("HOST_PWD"));
	    devInfo.setHostSign(rs.getString("HOST_SIGN"));
	    devInfo.setEncode(rs.getString("ENCODE"));
	    devInfo.setDeviceName(rs.getString("DEVICENAME"));
	    devInfo.setCityID(rs.getInt("CITY_ID"));
	    devInfo.setVendor(rs.getString("vendor"));
	    setDBDriver(rs.getString("DBDRIVER"));
	    setDBUrl(rs.getString("DBURL"));
	    setDevInfo(devInfo);
	    setDevPort(rs.getInt("DEV_PORT"));
	    DevInfo proxdevInfo = new DevInfo();
	    proxdevInfo.setDevID(rs.getInt("PROXY_DEV_ID"));
	    proxdevInfo.setName(rs.getString("PROXY_DEV_NAME"));
	    proxdevInfo.setIP(rs.getString("PROXY_HOST_IP"));
	    proxdevInfo.setHostUser(rs.getString("PROXY_HOST_USER"));
	    proxdevInfo.setHostPwd(rs.getString("PROXY_HOST_PWD"));
	    proxdevInfo.setHostSign(rs.getString("PROXY_HOST_SIGN"));
//	    setProxyDevInfo(proxdevInfo);
	    
//	    InDBServer indbserver = new InDBServer();
//	    indbserver.setInDBServer(rs.getString("INDBSERVER"));
//	    indbserver.setInDBUser(rs.getString("INDBUSER"));
//	    indbserver.setInDBPassword(rs.getString("INDBPASSWORD"));
//	    setInDBServerConfig(indbserver);

//	    setProxyDevPort(rs.getInt("PROXY_DEV_PORT"));
	    setCollectType(rs.getInt("COLLECT_TYPE"));
	    setCollectTimeOUT(rs.getInt("CollectTimeOut"));
	    setCollectPeriod(rs.getInt("COLLECT_PERIOD"));
	    setCollectTime(rs.getInt("COLLECT_TIME"));
	    setCollectTimePos(rs.getInt("COLLECT_TIMEPOS"));
	    setShellCMDPrepare(rs.getString("SHELL_CMD_PREPARE"));
	    setShellCMDFinish(rs.getString("SHELL_CMD_FINISH"));

	    setParserID(rs.getInt("PARSERID")); //解析类型，反射调用的解析类
	    setDistributorID(rs.getInt("DISTRIBUTORID"));
	
	    setRedoTimeOffset(rs.getInt("REDO_TIME_OFFSET"));
//	    setProbeTime(rs.getInt("prob_starttime"));
	
	    this.endDataTime = rs.getTimestamp("end_data_time");

//	    if (Util.isOracle())
	    {
	    	String strPath = ConstDef.ClobParse(rs.getClob("COLLECT_PATH"));
	    	setCollectPath(strPath);
	    }
//	    else if (Util.isSybase())
//	    {
//	    	setCollectPath(rs.getString("COLLECT_PATH"));
//	    }
//	    else if (Util.isMySQL())
//	    {
//	    	setCollectPath(rs.getString("COLLECT_PATH"));
//	    }

	    this.setShellTimeOut(rs.getInt("SHELL_TIMEOUT"));
	    this.setParesTmpID(rs.getInt("PARSE_TMPID")); //解析类型
	    
	    this.setParesTmpType(rs.getInt("TMPTYPE_P")); //解析模版类型

	    this.parseTmpRecord = new TempletRecord();
	    this.parseTmpRecord.setId(rs.getInt("PARSE_TMPID")); //解析模版编号
	    this.parseTmpRecord.setType(rs.getInt("TMPTYPE_P")); //解析模版类型
	    this.parseTmpRecord.setName(rs.getString("TMPNAME_P")); 
	    this.parseTmpRecord.setEdition(rs.getString("EDITION_P"));
	    this.parseTmpRecord.setFileName(rs.getString("TEMPFILENAME_P"));

	    //解析模版
	    this.parseTemplet = Factory.createTemplet(this.parseTmpRecord);
	
	    this.setDistributeTmpID(rs.getInt("DISTRBUTE_TMPID"));
	
	    this.distTmpRecord = new TempletRecord();
	    this.distTmpRecord.setId(rs.getInt("DISTRBUTE_TMPID"));
	    this.distTmpRecord.setType(rs.getInt("TMPTYPE_D"));
	    this.distTmpRecord.setName(rs.getString("TMPNAME_D"));
	    this.distTmpRecord.setEdition(rs.getString("EDITION_D"));
	    this.distTmpRecord.setFileName(rs.getString("TEMPFILENAME_D"));

	    this.distributeTemplet = Factory.createTemplet(this.distTmpRecord);
	
	    setLastCollectTime(rs.getTimestamp("SUC_DATA_TIME"));
	
	    setLastCollectPos(rs.getInt("SUC_DATA_POS"));
	
	    setMaxReCollectTime(rs.getInt("MAXCLTTIME"));
	
	    setThreadSleepTime(rs.getInt("THREADSLEEPTIME"));
	
	    setBlockedTime(rs.getInt("BLOCKEDTIME"));
  	}
  	

  	
  	public void log(String taskStatus, String taskDetail, Throwable taskException, String taskResult)
  	{
  		this.logInfo.setTaskId(getTaskID());
	    this.logInfo.setTaskDescription(getDescribe());
//	    this.logInfo.setTaskType((this instanceof RegatherObjInfo) ? "补采任务" : "正常任务");
	    this.logInfo.setTaskStatus(taskStatus);
	    this.logInfo.setTaskDetail(taskDetail);
	    this.logInfo.setTaskException(taskException);
	    this.logInfo.setDataTime(getLastCollectTime());
//	    this.logInfo.setCostTime(this.startTime == null ? 0L : new Date().getTime() - 
//	      this.startTime.getTime());
	    this.logInfo.setTaskResult(taskResult);
	    this.logInfo.addLog();
  	}

  	public void log(String taskStatus, String taskDetail)
  	{
  		log(taskStatus, taskDetail, null);
  	}

  	public void log(String taskStatus, String taskDetail, Throwable taskException)
  	{
  		log(taskStatus, taskDetail, taskException, null);
  	}
  	
	/**
  	 * 根据采集周期控制采集时间点
  	 * @return
  	 */
  	public long getPeriodTime()
  	{
  		long time = 0L;
  		switch (getCollectPeriod())
  		{
  			case 1://
  				time = 60000L;
  				break;
  			case 3:
  				time = 3600000L;
  				break;
  			case 6:
  				time = 14400000L;
  				break;
  			case 2:
  				time = 86400000L;
  				break;
  			case 4:
  				time = 1800000L;
  				break;
  			case 5://15 min
  				time = 900000L;
  				break;
  			case 7://5 min
  				time = 300000L;
  				break;
  			case 8:
  				time = 43200000L;
  				break;
  			case 9:
  				time = 60000L;
  				break;
  			case 10://十分钟粒度
  				time = 10*60*1000L;
  		}

  		return time;
  	}

}
