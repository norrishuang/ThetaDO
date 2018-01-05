package com.thetado.core.transform;

import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.thetado.core.taskmanage.TaskInfo;

public abstract class AbstractTransform {
	
	protected Logger log = Logger.getLogger(AbstractTransform.class);
	protected TaskInfo collectObjInfo;
	public AbstractTransform(TaskInfo info) {
		collectObjInfo = info;
	}

	public abstract void ParserData(String row);
	
	protected String removeNoiseSemicolon(String content)
	{
	    String strValue = content.replaceAll(";", " ");
	    strValue = content.replaceAll(",", " ");
	    return strValue;
	}
	
	protected boolean logicEquals(String shortFileName, String fileName)
	{
		if ((!fileName.contains("*")) && (!fileName.contains("?"))) return shortFileName.equals(fileName);

		String s1 = shortFileName.replaceAll("\\.", "");
		String s2 = fileName.replaceAll("\\.", "");
		s1 = s1.replaceAll("\\+", "");
		s2 = s2.replaceAll("\\+", "");
		s2 = s2.replaceAll("\\*", ".*");
		s2 = s2.replaceAll("\\?", ".");

		return Pattern.matches(s2, s1);
	}
	
}

    