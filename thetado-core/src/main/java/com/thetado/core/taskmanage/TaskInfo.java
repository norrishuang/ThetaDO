package com.thetado.core.taskmanage;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Date;



import com.thetado.core.datalog.DataLogInfo;
import com.thetado.core.template.AbstractTempletBase;


/**
 * ETL 任务信息对象
 * @author Administrator
 *
 */
public class TaskInfo implements Serializable{

	private static final long serialVersionUID = 2652378647623428865L;
	
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

}
