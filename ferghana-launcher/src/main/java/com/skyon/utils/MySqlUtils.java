package com.skyon.utils;

import java.sql.*;

public class MySqlUtils {
    /**
     * Get MySQL connection
     * @return
     * @throws Exception
     */
    public static Connection getConnection(String url, String username, String password, String driver) throws Exception {
        Class.forName(driver);
        return DriverManager.getConnection(url, username, password);
    }


    /**
     * Read the table data
     * @param connection
     * @param sql
     * @return
     * @throws Exception
     */
    public static ResultSet selectData(Connection connection, String sql) throws Exception {
        Statement statement = connection.createStatement();
        return statement.executeQuery(sql);
    }

    /**
     * Close MySQL connection
     * @param connection
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
