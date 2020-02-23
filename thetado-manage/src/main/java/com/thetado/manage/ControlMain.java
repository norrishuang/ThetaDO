package com.thetado.manage;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.thetado.rpc.NettyServer;

/**
 * 
 * "[任务调度管理系统] 服务端  控制主程序 主进程
 * 负责监听消息，处理收到的消息（管理消息，数据消息）
 * 
 * @author  Turk
 * @version  [版本号, 2017年3月22日]
 * @see  [相关类/方法]
 * @since  [产品/模块版本]
 */
public class ControlMain {
	
	private Logger log = Logger.getLogger(ControlMain.class);
	
	private Thread mainThread;
	
	private int PORT = 9527;
	public ControlMain(int port) {
		PORT = port;
	}
	
	
	public void start() throws IOException {
	 		this.mainThread = new Thread(new Runnable()
	 		{
	 			public void run()
	 			{
	 				try {//Netty Server 2016/11/24 by turk
						new NettyServer().bind(PORT);
						
					} catch (Exception e) {
						// TODO Auto-generated catch block
						log.error("Start Netty Sever Error",e);
					}
	 			}
	 		});
	 		this.mainThread.start();
	 		log.info("Start Main Port[" + PORT + "] listener Success!");
	}

}
