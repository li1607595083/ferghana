import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MySqlTest {

    String url = "jdbc:mysql://spark02:3306/test?characterEncoding=UTF-8";
    String username = "root";
    String password = "147268Tr";
    String driver = "com.mysql.cj.jdbc.Driver";
    Connection connection;

    @Before
    public void getConnection() throws Exception {
        Class.forName(driver);
        connection = DriverManager.getConnection(url, username, password);
    }

    @Test
    public void operator() throws Exception{
        ResultSet test = connection.getMetaData().getTables(null, null, "mysql_sink_table", null);
        if (test.next()){
            System.out.println("存在");
        } else {
            System.out.println("不存在");
        }
        PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM mysql_sink_table LIMIT 1");
        ResultSetMetaData metaData = preparedStatement.getMetaData();
        int columnCount = metaData.getColumnCount();
        for (int i = 1; i <= columnCount; i++){
            String name = metaData.getColumnName(i);
            String type = metaData.getColumnTypeName(i);
            System.out.println(name + ": " + type);
        }
//        connection.prepareStatement("ALTER TABLE mysql_sink_table ADD COLUMN wc1 BIGINT").execute();
    }

    @After
    public void closeConnection() {
        if (connection != null){
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

}
