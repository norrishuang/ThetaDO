package com.thetado.rpc;

import static org.junit.Assert.assertEquals;


import net.sf.json.JSONObject;

import org.apache.commons.beanutils.PropertyUtils;
import org.apache.log4j.Logger;


import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * Server 端处理
 * @author Administrator
 *
 */
public class NettyServerHandler extends ChannelInboundHandlerAdapter{

	private Logger log = Logger.getLogger(NettyServerHandler.class);
	
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		
		//接收到client消息
		ByteBuf buf = (ByteBuf) msg;
		byte[] req = new byte[buf.readableBytes()];
		buf.readBytes(req);
		String body = new String(req,"UTF-8");
//		
		JSONObject jsonObject = JSONObject.fromObject(body); 
		Object bean = JSONObject.toBean(jsonObject);
		int MsgID = 0;
		try 
		{
			assertEquals(jsonObject.get("msgID"),
					PropertyUtils.getProperty(bean, "msgID"));
			//
			MsgID = Integer.parseInt(PropertyUtils.getProperty(bean, "msgID").toString());
    		  
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.error("Master,Json ERROR",e);
		}
		
//		
//		//
		String strReturn = "";
		switch(MsgID)
		{
			case 110001://主进程选举请求
//				HBaseImportLog obj = new HBaseImportLog();
//				MushroomWarehouse.getInstance().InsertHBaseImportLog(obj.getByJson(body));
				break;
			case 110002://
//				ApolloConfig_Oracle sys = ApolloConfig_Oracle.getInstance();
//				HashSet<String> set = sys.GetIPFilter();
//				String strIPs = "";
//				for(String ip:set)
//				{
//					strIPs = strIPs + ip + "|";
//				}
//				strReturn = strIPs;
//				log.info("Get Import IP");
//				MailUtil.StormTaskUpdateTime = System.currentTimeMillis();
				break;
				
			case 1003://Alarm Log
//				AlarmLog alarmobj = new AlarmLog();
//				MushroomWarehouse.getInstance().AlarmWriteToLog4j(alarmobj.getByJson(body));
				strReturn = "Done";
				break;
				
			case 1004://Get IP Configs
				
//				ApolloConfig_Oracle sys1004 = ApolloConfig_Oracle.getInstance();
//				HashSet<String> set1004 = sys1004.GetIPWhiterList();
//				String strWhiterListIP = "";
//				for(String ip:set1004)
//				{
//					strWhiterListIP = strWhiterListIP + ip + "|";
//					log.info("WhiteList:" + ip);
//				}
//				if(strWhiterListIP.isEmpty())
//					strWhiterListIP = "NONE";
//				strReturn = strWhiterListIP;
				break;
			default:
				break;	  
		 }
		
		//strReturn = strReturn + "Done";
//		ByteBuf buf = (ByteBuf) msg;
//		byte[] req = new byte[buf.readableBytes()];
//		buf.readBytes(req);
//		String body = new String(req,"UTF-8");
//		System.out.println("The Netty server receive order : " + body);
		
//		String currentTime = "QUERY TIME ORDER".equalsIgnoreCase(body)?new java.util.Date(
//				System.currentTimeMillis()).toString() : "BAD ORDER";
		
				
		//消息返回给客户端
		ByteBuf resp = Unpooled.copiedBuffer(strReturn.getBytes());
		ctx.write(resp);
		
	}
	
	public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
		
		//System.out.println(ctx.name());
	    ctx.fireChannelRegistered();
	}
	
	public void channelReadComplete(ChannelHandlerContext ctx) throws Exception{
		ctx.flush();
	}
	
	public void exceptionCaught(ChannelHandlerContext ctx,Throwable cause){
		Logger.getLogger(NettyServerHandler.class).info("Unexpected exception from downstream : " + cause.getMessage());
		ctx.close();
	}
}
