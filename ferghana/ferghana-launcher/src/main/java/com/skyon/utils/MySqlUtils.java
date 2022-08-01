package com.skyon.utils;

import java.sql.*;

/**
 * @DESCIBE MySQL 工具类
 */
public class MySqlUtils {
    /**
     * @return 创建 MySQL 连接，并返回；
     * @throws Exception
     */
    public static Connection getConnection(String url, String username, String password, String driver) throws Exception {
        Class.forName(driver);
        return DriverManager.getConnection(url, username, password);
    }


    /**
     * @return 创建 MySQL 连接，并返回；
     * @throws Exception
     */
    public static Connection getConnection(String url, String username, String password) throws Exception {
        return DriverManager.getConnection(url, username, password);
    }

    /**
     * @param connection MySql 连接
     * @param sql 查询语句
     * @return 查询 MySQL 中表的数据，并返回查询结果
     * @throws Exception
     */
    public static ResultSet selectData(Connection connection, String sql) throws Exception {
        Statement statement = connection.createStatement();
        return statement.executeQuery(sql);
    }

    /**
     * @desc 关闭 Statement
     * @param statement
     * @throws SQLException
     */
    public static void  colseStatement(Statement statement) throws SQLException {
        if (statement != null){
            statement.close();
        }
    }

    /**
     * @desc  关闭MySQL 连接
     * @param connection MySql 连接
     */
    public static void closeConnection(Connection connection) {
        if (connection != null){
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

}
