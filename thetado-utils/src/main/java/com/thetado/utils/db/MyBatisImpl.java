package com.thetado.utils.db;

import java.util.List;

import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;

import com.thetado.utils.db.mapper.MapperTest;

public class MyBatisImpl implements MapperTest{
	SqlSessionFactory factory = null;
	
	public MyBatisImpl(SqlSessionFactory factory) {
		this.factory = factory;
	}

	public SqlSessionFactory getFactory() {
		return factory;
	}

	public void setFactory(SqlSessionFactory factory) {
		this.factory = factory;
	}
	
	@Override
	public List<DBRet> getData() {
		// TODO Auto-generated method stub
		SqlSession sqlSession = factory.openSession();
        try {
        	MapperTest userMapper = sqlSession.getMapper(MapperTest.class);
        	List<DBRet> list = userMapper.getData();
        	for(DBRet ret:list) {
	            System.out.println("id: " + ret.get_POSITION_ID() + "|name: "
	                    + ret.get_POSITION_NAME());
        	}
            return list;
        } catch (Exception ex) {
        	System.out.println(ex);
        	return null;
        }finally {
        
            sqlSession.close();
        }
	}
}

