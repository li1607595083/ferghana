import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.util.List;
/**
 *
 */
public class InFluxdbTest {

    InfluxDB influxdbConnection;

    @Before
    public  void getInfluxdbConnection(){
        influxdbConnection =  InfluxDBFactory.connect("http://192.168.4.95:8086", "ferghana", "Ferghana@1234");
    }

    @Test
    public  void  query(){
        String job_id = "4387d9722559e5d7adf13af9b2274d03";
        // 检查时间范围
        int checkTime = 5;
        // 数据库
        String dataBase = "flink_metrics";
        // 近5分钟是否出现背压
        String isBackPressuredSql =
                "SELECT value "
                 + " FROM taskmanager_job_task_isBackPressured "
                 + " WHERE job_id = " + "'" + job_id + "' "
                 + " AND time >= now() - " + checkTime + "m"
                 + " AND value = 'true' "
                 + " tz('Asia/Shanghai')";

        boolean isBackPressuredBoolean = isBackPressured(isBackPressuredSql, dataBase);
        System.out.println("近5分钟是否出现被压:\t" + isBackPressuredBoolean);

        // 近5分钟checkpoint失败次数
        String checkpointFailNumbersSql =
                "SELECT value "
                 + " FROM jobmanager_job_numberOfFailedCheckpoints "
                 + " WHERE job_id = " + "'" + job_id + "' "
                 + " AND time >= now() - " + checkTime + "m "
                 + " ORDER BY time DESC"
                 + " tz('Asia/Shanghai')";

        double failNumbers =  getValue(checkpointFailNumbersSql, dataBase);
        System.out.println("近5分钟checkpoint失败次数:\t" + failNumbers);

        // 5分钟内重启次数
        String restartsNumbersSql =
                "SELECT value "
                 + " FROM jobmanager_job_numRestarts "
                 + " WHERE job_id = " + "'" + job_id + "' "
                 + " AND time >= now() - " + checkTime  + "m "
                 + " ORDER BY time DESC"
                 + " tz('Asia/Shanghai')";
        double restartsNumbers =  getValue(restartsNumbersSql, dataBase);
        System.out.println("5分钟内重启次数:\t" + restartsNumbers);

        // 5分钟内平均延迟
        String computerDurationSql =
                "SELECT MEAN(value) "
                        + " FROM taskmanager_job_task_operator_flink_customer_metric_computer_duration "
                        + " WHERE job_id = " + "'" + job_id + "' "
                        + " AND time >= now() - " + checkTime  + "m "
                        + " ORDER BY time DESC"
                        + " tz('Asia/Shanghai')";
        double avgComputerDuration= getAvg(computerDurationSql, dataBase);
        System.out.println("5分钟内平局计算延迟:\t" + avgComputerDuration);


        // jobManager的CPU_Load
        String computerJobMangerCpuLoad =
                "SELECT () ";
    }

    private double getAvg(String sql, String database) {
        QueryResult queryResult = influxdbConnection.query(new Query(sql, database));
        return (double)queryResult.getResults().get(0).getSeries().get(0).getValues().get(0).get(1);
    }

    private boolean isBackPressured(String sql, String database) {
        List<QueryResult.Series> series = null;
        QueryResult queryResult = influxdbConnection.query(new Query(sql, database));
        List<QueryResult.Result> results = queryResult.getResults();
        for (QueryResult.Result result : results) {
            series = result.getSeries();
        }
        return series != null;
    }

    private double getValue(String sql, String database) {
        QueryResult queryResult = influxdbConnection.query(new Query(sql, database));
        List<QueryResult.Result> results = queryResult.getResults();
        List<List<Object>> values = results.get(0).getSeries().get(0).getValues();
        double current = (double)values.get(0).get(1);
        double last = (double)values.get(values.size() - 1).get(1);
        return  current - last;
    }

    @After
    public  void  close(){
        if (influxdbConnection != null){
            influxdbConnection.close();
        }
    }

}
