package com.thetado.manage;

import java.util.Date;

import org.apache.log4j.Logger;

import com.thetado.core.bean.PBeanMgr;
import com.thetado.utils.Util;

public class ThetaDORUN {
	
	private static Logger log = Logger.getLogger(ThetaDORUN.class);

	public static final Date SYS_START_TIME = new Date();

	public static void main(String[] args)
	{
		Start();
	}
	
	

	public static void Start()
	{
		log.info("System Start...");
//		Version ver = Version.getInstance();
//		String strVer = ver.getExpectedVersion();
//		if (!ver.isRightVersion())
//		{
//			log.error("系统退出. 原因:版本号不一致. 内部版本号:" + strVer);
//			return;
//		}
//		log.info("版本号:" + strVer);
//		try
//		{
//			ConsoleMgr.getInstance(SystemConfig.getInstance().getCollectPort()).start();
//		}
//		catch (Exception e)
//		{
//			log.error("采集系统启动失败,原因: 控制台模块启动异常. ", e);
//		}
		
//		SelfTest st = SelfTest.getInstance();
//		if(!st.IsOK())
//		{
//			log.error("系统退出. 原因:程序自检异常");
//			return;
//		}

		Util.printEnvironmentInfo();

		PBeanMgr.getInstance();

//		AlarmMgr.getInstance();
		
		//ResourceManagerment.getInstance().start();

//		ProcessStatus.getInstance().start();
//		
//		DataLifecycleMgr.getInstance().start();

		//启动路测汇总线程
	
		
		
//		specialapp.ScanLog scanlog = specialapp.ScanLog.getInstance();
//		scanlog.startScan();
		
		ScanThread scanThread = ScanThread.getInstance();
		scanThread.startScan();
		
		
		
		//PersistentScanThread PscanThread = PersistentScanThread.getInstance();
		//PscanThread.startScan();		
	}
	

	public String toString()
	{
		return "Capricorn V2";
	}

}

    