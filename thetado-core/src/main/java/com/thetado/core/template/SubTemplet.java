package com.thetado.core.template;


/**
	 * 解析子模版
	 * @author Administrator
	 *
	 */
	public class SubTemplet
	{
		/**
		 * 文件名
		 */
		public String m_strFileName = "";
		
		/**
		 * 标识，特殊需要使用
		 */
		public String m_tag = "";
		
		/**
		 * 是否需要文件名比较
		 */
		public int m_nFileNameCompare = 0;
		
		/**
		 * 
		 */
		public String m_RawColumnList = "";
		
		/**
		 * 
		 */
		public String m_ColumnListAppend = "";
		
		/**
		 * 每一行头位置标识符
		 */
		public String m_strLineHeadSign = "";
		
		/**
		 * 此处表示为每个子模版的索引编号
		 */
		public int m_nLineHeadType = 0;
		
		/**
		 * 解析字段列数
		 */
		public int m_nColumnCount = 0;
		
		/**
		 * 解析原始文件字段分隔符
		 */
		public String m_strFieldSplitSign = "";
		
		/**
		 * 解析原始文件字段分隔符，特殊字符例如引号中间的逗号
		 */
		public String m_strFieldUpSplitSign = "";
		
		/**
		 * 
		 */
		public boolean m_bEscape = true;
		
		/**
		 * 解析后生成的字段分隔符
		 */
		public String m_strNewFieldSplitSign = "";
		
		/**
		 * 解析类型  对应解析配置文件中  RESERVED.RITEM.PARSETYPE 节
		 * 1：按行间隔符解析
		 * 2: 按位解析，字符串起始位置+字段长度解析
		 * 3：整行解析，只做间隔符替换         
		 */
		public int m_nParseType;
		
		/**
		 * 默认解析行自带默认字段类型
		 * 0：不带默认字段(默认)
		 * 1：带默认字段 DEVICEID,COLLECTTIME,STAMPTIME,
		 * 2：带默认字段 START_TIME 当前采集任务的时间
		 */
		public int m_nDefaultColumnType;
		
		/**
		 * 用于hbase的 rowkey
		 */
		public boolean m_hasRowkey = false;
		
		
		
		/**
		 * 解析字段
		 */
//		public Map<Integer, LineTempletP.FieldTemplet> m_Filed = new HashMap<Integer, FieldTemplet>();

		/**
		 * 当字段为空时，替代字段的符号
		 */
		public String nvl = "0";

		public SubTemplet()
		{
		}
	}

    