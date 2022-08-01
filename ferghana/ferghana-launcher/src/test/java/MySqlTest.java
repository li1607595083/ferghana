import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.Random;

public class MySqlTest {

    String url = "jdbc:mysql://192.168.0.201:3306/ferghana?characterEncoding=UTF-8";
//    String url = "jdbc:mysql://172.16.20.157:4000/crisps_bigdata_ads";
    String username = "ferghana";
//    String username = "root";
    String password = "Ferghana@1234";
//    String password = "TSProot123dgg";
    String driver = "com.mysql.cj.jdbc.Driver";
    Connection connection;

    @Before
    public void getConnection() throws Exception {
        Class.forName(driver);
        connection = DriverManager.getConnection(url, username, password);
        if (connection != null){
            System.out.println("----------");
        }
    }


    @Test
    public void opert() throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        System.out.println(metaData.getDriverName());
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
        Random random = new Random();
        PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO user_info  VALUE (?, ?, ?, ?, ?)");
        for (int i = 1; i < 50; i++){
            preparedStatement.setString(1, "skyonuser_" + i);
            preparedStatement.setString(2, 1966 + "" + random.nextInt(10) + "" + 32 + "" + random.nextInt(10) + "" + random.nextInt(10) + "" + random.nextInt(10) + "" + random.nextInt(10));
            preparedStatement.setBoolean(3, random.nextInt(2) == 1);
            preparedStatement.setString(4, random.nextInt(2) == 1 ? "男" : "女");
            preparedStatement.setInt(5, random.nextInt( 50) + 15);
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
