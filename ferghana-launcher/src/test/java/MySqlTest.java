import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MySqlTest {

    String url = "jdbc:mysql://master:3306/test?characterEncoding=UTF-8";
    String username = "ferghana";
    String password = "Ferghana@1234";
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

    @Test
    public void insert() throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO EP_CUST_INF(CUST_NO_TWO, ADDR) VALUE (?, ?) " +
                "ON DUPLICATE key update  CUST_NO_TWO=?, ADDR=?");
        for (int i = 1; i < 15; i++){
            preparedStatement.setString(1, "10010120" + i);
            preparedStatement.setString(2, "frgr");
            preparedStatement.setString(3, "10010120" + i);
            preparedStatement.setString(4, "frgr");
            preparedStatement.addBatch();
        }
        preparedStatement.executeBatch();
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
