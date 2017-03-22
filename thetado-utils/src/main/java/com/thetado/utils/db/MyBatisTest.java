package com.thetado.utils.db;

import org.apache.ibatis.session.SqlSessionFactory;

public class MyBatisTest {

	static SqlSessionFactory sqlSessionFactory = null;
    static {
        sqlSessionFactory = MyBatisUtil.getSqlSessionFactory();
    }

    public static void main(String[] args) {
    	MyBatisImpl test = new MyBatisImpl(sqlSessionFactory);
    	test.getData();
    }

  




	
}
