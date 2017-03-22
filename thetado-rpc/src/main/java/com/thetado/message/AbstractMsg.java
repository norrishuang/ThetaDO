package com.thetado.message;

/**
 * 
 * @author Administrator
 *
 */
public abstract class AbstractMsg {

	protected int _msgid;
	
	public void setMsgID(int msgid)
	{
		this._msgid = msgid;
	}
	
	public int getMsgID()
	{
		return this._msgid;
	}
}

