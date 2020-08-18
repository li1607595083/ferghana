package com.skyon.controller;


import com.skyon.domain.TDataSource;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import java.io.IOException;
import java.io.InputStream;

public class TestController {


    public static void main(String[] args) {
        // 根据 mybatis-confi.xml 配置的信息得到 sqlSessionFactory
        String resource = "mybatis/mybatis-config.xml";
        InputStream inputStream = null;
        try {
            inputStream = Resources.getResourceAsStream(resource);
        } catch (IOException e) {
            e.printStackTrace();
        }
        SqlSessionFactory sqlSessionFactory = new SqlSessionFactoryBuilder().build(inputStream);
        // 然后根据 sqlSessionFactory 得到 session
        SqlSession session = sqlSessionFactory.openSession();

        // 调用sql
        TDataSource tDataSource = session.selectOne("selectTDataSourceById", new Long("27"));


        System.out.println(tDataSource.toString());


    }

}
