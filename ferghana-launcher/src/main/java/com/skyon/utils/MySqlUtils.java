package com.skyon.utils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

public class MySqlUtils {

    private static final String url = "jdbc:mysql://master:3306/ferghana?characterEncoding=UTF-8";
    private static final String username = "ferghana";
    private static final String password = "Ferghana@1234";
    private static final String driver = "com.mysql.cj.jdbc.Driver";

    /**
     * Get MySQL connection
     * @return
     * @throws Exception
     */
    public static Connection getConnection() throws Exception {
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
