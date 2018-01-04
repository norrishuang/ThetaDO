package com.thetado.core.taskmanage;

import org.apache.log4j.Logger;

import com.thetado.core.access.AbstractAccessor;
import com.thetado.core.bean.PBeanMgr;
import com.thetado.core.distribute.AbstractDistribute;
import com.thetado.core.parser.AbstractParser;
import com.thetado.core.template.AbstractTempletBase;
import com.thetado.core.template.TempletBase;
import com.thetado.core.template.TempletRecord;
/**
 * 采集解析对象工厂
 * @author Administrator
 *
 */
public class Factory
{
	private static Logger logger = Logger.getLogger(Factory.class);

	public static AbstractAccessor createAccessor(TaskInfo obj)
	{
		if (obj == null) {
			return null;
		}
		AbstractAccessor accessor = PBeanMgr.getInstance().getAccessorBean(obj.getCollectType());
		if (accessor == null) {
			return null;
		}
		accessor.setTaskInfo(obj);

		AbstractParser parser = createParser(obj);
		if (parser == null)
		{
			logger.error("未找到parserId为" + obj.getParserID() + 
				"的解析器，请查看pbean.xml是否有此parser");
		}

		AbstractDistribute distributor = createDistributor(obj);
		parser.setDistribute(distributor);

		accessor.setParser(parser);
		accessor.setDistributor(distributor);

		return accessor;
	}

	public static AbstractParser createParser(TaskInfo obj)
	{
		if (obj == null) {
			return null;
		}
		int parserID = obj.getParserID();

		AbstractParser p = PBeanMgr.getInstance().getParserBean(parserID);
		if (p == null) {
			return null;
		}

		p.setCollectObjInfo(obj);

		return p;
	}

	public static AbstractDistribute createDistributor(TaskInfo obj)
	{
		if (obj == null) {
			return null;
		}
		int distID = obj.getDistributorID();

		AbstractDistribute d = PBeanMgr.getInstance().getDistributorBean(distID);
		if (d == null)
			return null;
		d.init(obj);

		return d;
	}

	public static TempletBase createTemplet(int tmpType, int tmpID)
	{
		TempletBase templet = PBeanMgr.getInstance().getTemplateBean(tmpType);
		if (templet == null) {
			return null;
		}

		templet.buildTmp(tmpID);

		return templet;
	}

	public static AbstractTempletBase createTemplet(TempletRecord record)
	{
		if (record == null) {
			return null;
		}
		AbstractTempletBase templet = PBeanMgr.getInstance().getTemplateBean(record.getType());
		if (templet == null) {
			return null;
		}
		
		templet.buildTmp(record);

		return templet;
	}
}
    