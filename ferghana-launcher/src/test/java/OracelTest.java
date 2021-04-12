import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.math.BigDecimal;
import java.sql.*;

public class OracelTest {

    Connection connection;

    @Before
    public void getOracleConnection() throws Exception {
        String url = "jdbc:oracle:thin:@192.168.30.72:1521:ORCL";  //连接oracle路径方式 “”gfs“”是要建立连接的数据库名 1521端口
        String user = "SCOTT";   //user是数据库的用户名
        String password = "tiger";  //用户登录密码
        //首先建立驱动
        Class.forName("oracle.jdbc.driver.OracleDriver");
        //驱动成功后进行连接
        connection = DriverManager.getConnection(url, user, password);
    }

    @Test
    public void query() throws Exception{
         PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM FLINK_ORACLE_TEST_001");
         ResultSet resultSet = preparedStatement.executeQuery();
         while (resultSet.next()){
            String trade_id = resultSet.getString(1);
             BigDecimal trade_amount = resultSet.getBigDecimal(2);
             Timestamp timestamp = resultSet.getTimestamp(3);
             System.out.println(trade_id + "\t" + trade_amount + "\t" + timestamp);
        }
    }

    @Test
    public void insertBath() throws Exception {
//        PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO SCOTT_TABLE1 VALUES(?, ?, ?)");
        PreparedStatement preparedStatement = connection.prepareStatement("MERGE INTO SCOTT_TABLE1 t USING DUAL "
                + "ON (t.ID = ?) WHEN NOT MATCHED THEN INSERT (ID, USER_NAME, USER_PWD) "
                + "VALUES (?, ?, ?) "
                + "WHEN MATCHED THEN UPDATE SET "
                + "USER_PWD = ?, USER_NAME = ?");
        for (int i = 1; i < 5; i++){
            preparedStatement.setBigDecimal(1, new BigDecimal(i + "999"));
            preparedStatement.setBigDecimal(2, new BigDecimal(i + "999"));
            preparedStatement.setString(3, "ewrwr_001");
            preparedStatement.setString(4, "dfefrrg_001");
            preparedStatement.setString(5, "ewrwr_001");
            preparedStatement.setString(6, "dfefrrg_001");
//            preparedStatement.setTimestamp(3, Timestamp.valueOf("2020-12-23 11:23:34." + i * 10));
            preparedStatement.addBatch();
        }
        preparedStatement.executeBatch();
    }

    @After
    public void closeOracleConnection() throws SQLException {
        if (connection != null){
            connection.close();
        }
    }

}
