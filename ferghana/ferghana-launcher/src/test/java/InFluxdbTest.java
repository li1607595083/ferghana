import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;

/**
 *
 */
public class InFluxdbTest {
    private  static FastDateFormat dateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss.SSS");

    InfluxDB influxdbConnection;

    @Before
    public  void getInfluxdbConnection(){
        influxdbConnection =  InfluxDBFactory.connect("http://192.168.4.95:8086", "ferghana", "Ferghana@1234");
    }

    @Test
    public  void  query() throws ParseException {
        String job_id = "5f0a8c37de2df8a4691d2143c8120199";
        // metric reporter 时间间隔
        int reportInterval =  1;
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
                 + " ORDER BY time DESC "
                 + " tz('Asia/Shanghai')";

        double failNumbers =  getValue(checkpointFailNumbersSql, dataBase);
        System.out.println("近5分钟checkpoint失败次数:\t" + failNumbers);





        // 5分钟内重启次数
        String restartsNumbersSql =
                "SELECT value "
                 + " FROM jobmanager_job_numRestarts "
                 + " WHERE job_id = " + "'" + job_id + "' "
                 + " AND time >= now() - " + checkTime  + "m "
                 + " ORDER BY time DESC "
                 + " tz('Asia/Shanghai') ";
        double restartsNumbers =  getValue(restartsNumbersSql, dataBase);
        System.out.println("5分钟内重启次数:\t" + restartsNumbers);





        // 5分钟内平均延迟
        String computerDurationSql =
                "SELECT MEAN(value) "
                + " FROM taskmanager_job_task_operator_flink_customer_metric_computer_duration "
                + " WHERE job_id = " + "'" + job_id + "' "
                + " AND time >= now() - " + checkTime  + "m "
                + " ORDER BY time DESC "
                + " tz('Asia/Shanghai')";
        double avgComputerDuration= getAvg(computerDurationSql, dataBase);
        System.out.println("5分钟内平局计算延迟:\t" + avgComputerDuration);





//         获取当前的输出条数
//        String computerSinkResult =
//                "SELECT "





        // jobManger cpu 的使用情况
        String computerJobMangerCpuLoad =
                "SELECT value, job_id_2 "
                + " FROM jobmanager_Status_JVM_CPU_Load "
                + " WHERE job_id_2 = " + "'" + job_id + "' "
                + " ORDER BY time DESC "
                + " LIMIT 1 "
                + " tz('Asia/Shanghai')";
        Tuple2<Long, Double> jobManagerResourceCpuLoad = getJobManagerResource(computerJobMangerCpuLoad, dataBase);
        // 横坐标
        long jmCpuLoadTime = jobManagerResourceCpuLoad.f0;
        // 纵坐标
        double jmCpuLoadValue = jobManagerResourceCpuLoad.f1;
        System.out.println("jobManger cpu 的使用情况:\t" + jmCpuLoadTime + "\t" + jmCpuLoadValue);





        // jobManger head 的使用情况
        String computerJobMangerHeadLoad =
                "SELECT value "
                + " FROM jobmanager_Status_JVM_Memory_Heap_Used "
                + " WHERE job_id_2 = " + "'" + job_id + "' "
                + " ORDER BY time DESC"
                + " LIMIT 1 "
                + " tz('Asia/Shanghai')";
        Tuple2<Long, Double> jobManagerResourceHeadUsed = getJobManagerResource(computerJobMangerHeadLoad, dataBase);
        // 很坐标
        Long jmHeadUsedTime = jobManagerResourceHeadUsed.f0;
        // 中坐标
        double jmHeadUsedValue = jobManagerResourceHeadUsed.f1 / (1024 * 1024);
        System.out.println("jobManger head 的使用情况\t" + jmHeadUsedTime + "\t" + jmHeadUsedValue);





        // taskManger cpu 的使用情况
        String computerTaskMangerCpuLoad =
                "SELECT tm_id, value "
                + " FROM taskmanager_Status_JVM_CPU_Load "
                + " WHERE job_id_2 = " + "'" + job_id + "' "
                + " AND time > now() - " + reportInterval + "m "
                +  " ORDER BY time DESC "
                + " tz('Asia/Shanghai')";
        HashMap<String, Tuple2<Long, Double>> taskManagerResourceCpuLoad = getTaskManagerResource(computerTaskMangerCpuLoad, dataBase);
        Iterator<Map.Entry<String, Tuple2<Long, Double>>> iteratorTmCupLoad = taskManagerResourceCpuLoad.entrySet().iterator();
        while (iteratorTmCupLoad.hasNext()){
            Map.Entry<String, Tuple2<Long, Double>> next = iteratorTmCupLoad.next();
            // 分类标准
            String taskCpuLoadContainer = next.getKey();
            Tuple2<Long, Double> value = next.getValue();
            // 横坐标
            Long tmCpuLoadTime = value.f0;
            // 纵坐标
            Double tmCpuLoadValue = value.f1;
            System.out.println("taskManger cpu 的使用情况:\t" + taskCpuLoadContainer + "\t" + tmCpuLoadTime + "\t" + tmCpuLoadValue);
        }





        //taskManger cpu 的使用情况
        String computerTaskMangerHeadLoad =
                "SELECT tm_id, value"
                + " FROM taskmanager_Status_JVM_Memory_Heap_Used "
                + " WHERE job_id_2 = " + "'" + job_id + "' "
                + " AND time > now() - " + reportInterval + "m "
                + " ORDER BY time DESC "
                + " tz('Asia/Shanghai')";
        HashMap<String, Tuple2<Long, Double>> taskManagerResourceHeadUsed = getTaskManagerResource(computerTaskMangerHeadLoad, dataBase);
        Iterator<Map.Entry<String, Tuple2<Long, Double>>> iteratorTmHeadUsed = taskManagerResourceHeadUsed.entrySet().iterator();
        while (iteratorTmHeadUsed.hasNext()){
            Map.Entry<String, Tuple2<Long, Double>> next = iteratorTmHeadUsed.next();
            // 分类标准
            String taskHeadUsed = next.getKey();
            Tuple2<Long, Double> value = next.getValue();
            // 横坐标
            Long tmHedUseddTime = value.f0;
            // 纵坐标
            Double tmHeadUsedValue = value.f1 / (1024 * 1024);
            System.out.println("taskManger head 的使用情况:\t" + taskHeadUsed + "\t" + tmHedUseddTime + "\t" + tmHeadUsedValue);
        }





        // 获取数据输出条数
        String computerNumRecordsOut =
                "SELECT operator_name, count"
                        + " FROM taskmanager_job_task_operator_numRecordsOut "
                        + " WHERE job_id_2 = " + "'" + job_id + "' "
                        + " AND time > now() - " + reportInterval + "m "
                        + " ORDER BY time DESC "
                        + " tz('Asia/Shanghai')";
        Double numRecordsOut = getNumRecordsOut(computerNumRecordsOut, dataBase);
        System.out.println("当前 job 输出条数:\t" + numRecordsOut);

    }


    private Double getNumRecordsOut(String sql, String database) throws ParseException {
        double counts = 0;
        QueryResult.Result oneQueryResult = getOneQueryResult(sql, database);
        if (oneQueryResult.getSeries() != null){
            List<List<Object>> queryResultFieldValue = getQueryResultFieldValue(oneQueryResult);
            if (queryResultFieldValue != null && queryResultFieldValue.size() > 0 ){
                for (List<Object> values : queryResultFieldValue) {
                    String operatorName = values.get(1).toString();
                    if (operatorName.contains("SINK_OUTPUT_RESUTL")){
                        counts += (double)values.get(2);
                    }
                }
            }
        }
        return counts;
    }


    /**
     * @desc 用以获取 taskmanager 的 cpu 和 memory 使用情况
     * @param sql
     * @param database
     * @return
     */
    private HashMap<String, Tuple2<Long, Double>> getTaskManagerResource(String sql, String database) throws ParseException {
        HashMap<String, Tuple2<Long, Double>> valueHashMap = new HashMap<>();
        QueryResult.Result oneQueryResult = getOneQueryResult(sql, database);
        if (oneQueryResult.getSeries() != null) {
            List<List<Object>> queryResultFieldValue = getQueryResultFieldValue(oneQueryResult);
            if (queryResultFieldValue != null && queryResultFieldValue.size() > 0) {
                for (List<Object> values : queryResultFieldValue) {
                    String conatiner = values.get(1) == null ? null : values.get(1).toString();
                    if (conatiner != null){
                        long time = dateFormat.parse(values.get(0).toString().replace("T", " ").split("\\+", 2)[0]).getTime();
                        double value = (double)(values.get(2) == null ? 0.0 : values.get(2));
                        if (valueHashMap.get(conatiner) == null){
                            valueHashMap.put(conatiner, Tuple2.of(time, value));
                        }
                    }
                }
            }
        }
        return valueHashMap;
    }

    /**
     * @desc 用以获取 jobmanager 的 cpu 和 memory 使用情况
     * @param sql
     * @param database
     * @return
     */
    private Tuple2<Long, Double> getJobManagerResource(String sql, String database) throws ParseException {
        Tuple2<Long, Double> longDoubleTuple2 = Tuple2.of(0L, 0.0);
        QueryResult.Result oneQueryResult = getOneQueryResult(sql, database);
        if (oneQueryResult.getSeries() != null) {
            List<List<Object>> queryResultFieldValue = getQueryResultFieldValue(oneQueryResult);
            if (queryResultFieldValue != null && queryResultFieldValue.size() > 0) {
                for (List<Object> values : queryResultFieldValue) {
                    long time = dateFormat.parse(values.get(0).toString().replace("T", " ").split("\\+", 2)[0]).getTime();
                    double value = (double) (values.get(1) == null ? 0.0 : values.get(1));
                    longDoubleTuple2.f0 = time;
                    longDoubleTuple2.f1 = value;
                }
            }
        }
        return longDoubleTuple2;
    }

    /**
     * @desc 用以获取近五分钟的 task 重启次数，checkpoint 失败次数
     * @param sql
     * @param database
     * @return
     */
    private double getValue(String sql, String database) {
        double counts  = 0;
        QueryResult.Result oneQueryResult = getOneQueryResult(sql, database);
        if (oneQueryResult.getSeries() != null) {
            List<List<Object>> queryResultFieldValue = getQueryResultFieldValue(oneQueryResult);
            if (queryResultFieldValue != null && queryResultFieldValue.size() > 0) {
                List<Object> firstLine = queryResultFieldValue.get(0);
                List<Object> lastLine = queryResultFieldValue.get(queryResultFieldValue.size() - 1);
                double firstValue = (double)(firstLine.get(1) == null ? 0.0 : firstLine.get(1));
                double lastValue = (double)(lastLine.get(1) == null ? 0.0 : lastLine.get(1));
                if (queryResultFieldValue.size() == 1){
                    counts = firstValue;
                } else {
                   counts = firstValue - lastValue;
                }
            }
        }
        return counts;
    }

    /**
     * @desc 获取近五分钟的平均延迟
     * @param sql
     * @param database
     * @return
     */
    private double getAvg(String sql, String database) {
        double avg = 0;
        QueryResult.Result oneQueryResult = getOneQueryResult(sql, database);
        if (oneQueryResult.getSeries() != null){
            List<List<Object>> queryResultFieldValue = getQueryResultFieldValue(oneQueryResult);
            if (queryResultFieldValue != null && queryResultFieldValue.size() > 0){
                for (List<Object> values : queryResultFieldValue) {
                    avg  = (double) (values.get(1) == null ? 0.0 : values.get(1));
                }
            }
        }
        return avg;
    }

    /**
     * @desc 获取是否出现被压
     * @param sql
     * @param database
     * @return
     */
    private boolean isBackPressured(String sql, String database) {
        return getOneQueryResult(sql,database).getSeries() != null;
    }

    /**
     * @desc 获取单个sql查询结果
     * @param sql
     * @param database
     * @return
     */
    private QueryResult.Result getOneQueryResult(String sql, String database){
        // results.getResults()是同时查询多条SQL语句的返回值，此处我们只有一条SQL，所以只取第一个结果集即可
        QueryResult queryResult = influxdbConnection.query(new Query(sql, database));
        return queryResult.getResults().get(0);
    }

    /**
     * @desc 获取字段值
     * @param result
     * @return
     */
    private List<List<Object>> getQueryResultFieldValue(QueryResult.Result result){
       return result.getSeries().stream().map(QueryResult.Series::getValues).collect(Collectors.toList()).get(0);
    }

    @After
    public void close () {
        if (influxdbConnection != null) {
            influxdbConnection.close();
        }
    }

}
