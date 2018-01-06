package com.thetado.core.template;

/**
 * 
 * @author Administrator
 *
 */
public abstract interface ITempletBase
{
	public abstract void buildTmp(int paramInt);

	public abstract void buildTmp(TempletRecord paramTempletRecord);

	public abstract void parseTemp(String paramString)
    	throws Exception;
	

}