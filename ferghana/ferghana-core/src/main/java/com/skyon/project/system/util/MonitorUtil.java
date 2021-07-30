package com.skyon.project.system.util;

import com.skyon.project.system.domain.OperationMonitor;
import com.skyon.project.system.domain.TVariablePackageManager;
import com.skyon.project.system.service.ITVariablePackageManagerService;
import com.skyon.project.system.service.OperationService;
import net.sf.json.JSONArray;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;

/**
 *
 *  预警监控工具类
 */
@Component
@EnableScheduling
public class MonitorUtil {

    private static FastDateFormat dateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss.SSS");

    private static InfluxDB influxdbConnection = InfluxDBUtil.getInfluxdbConnection();

    @Autowired
    private OperationService operationService;

    // 每分钟获取监控数据
    @Scheduled(cron = "0 */5 * * * ?")
    public void monitor() throws IOException, InterruptedException {
        List<TVariablePackageManager> runingList = operationService.getRuningJobList();
        runingList.forEach(item -> {
            try {
                query(String.valueOf(item.getJobId()));
            } catch (ParseException e) {
                e.printStackTrace();
            }
        });
    }

    @Scheduled(cron = "0 0 0 * * ?")
    public void deleteMonitor() {
        operationService.deleteOperationMonitor();
    }


    public void query(String job_id) throws ParseException {
//        String job_id = "5702950f851c471bdead642990ee3954";
        // 数据库
        String dataBase = "flink_metrics";
        // metric reporter 时间间隔
        int reportInterval =  1;
        // 检查时间范围
        int checkTime = 5;
        insert(queryJobMangerCpuLoad(job_id, dataBase), "jobMangerCpu", job_id);
        insert(queryJobMangerHeadLoad(job_id, dataBase), "jobMangerHead", job_id);
        insert(queryTaskMangerCpuLoad(job_id, dataBase, reportInterval), "taskMangerCpu", job_id);
        insert(queryTaskMangerHeadLoad(job_id, dataBase, reportInterval), "taskMangerHead", job_id);
        insert(queryAvgComputerDuration(job_id, dataBase, checkTime), "avgComputerDuration", job_id);
        insert(queryNumRecordsOut(job_id, dataBase, reportInterval), "numRecordsOut",job_id);
    }

    private void insert(Tuple2<Long, Double> value, String type, String job_id) {
        try {
            NumberFormat nf = NumberFormat.getInstance();
            nf.setMaximumFractionDigits(5);
            OperationMonitor operationMonitor = new OperationMonitor();
            operationMonitor.setMonitorTime(value.f0);
            operationMonitor.setMonitorValue(Double.valueOf(nf.format(value.f1)));
            operationMonitor.setMonitorType(type);
            operationMonitor.setJobId(job_id);
            operationService.insertOperationMonitor(operationMonitor);
        } catch(Exception e){
//            e.printStackTrace();
        }
    }

    private void insert(Double value, String type, String job_id) {
        try {
            NumberFormat nf = NumberFormat.getInstance();
            nf.setMaximumFractionDigits(5);
            OperationMonitor operationMonitor = new OperationMonitor();
            operationMonitor.setMonitorValue(Double.valueOf(nf.format(value)));
            operationMonitor.setMonitorType(type);
            operationMonitor.setJobId(job_id);
            operationService.insertOperationMonitor(operationMonitor);
        } catch(Exception e){
//            e.printStackTrace();
        }
    }



    // jobManger cpu 的使用情况
    private Tuple2<Long, Double> queryJobMangerCpuLoad(String job_id, String dataBase) throws ParseException {
        String computerJobMangerCpuLoad =
                "SELECT value, job_id_2 "
                        + " FROM jobmanager_Status_JVM_CPU_Load "
                        + " WHERE job_id_2 = " + "'" + job_id + "' "
                        + " ORDER BY time DESC "
                        + " LIMIT 1 "
                        + " tz('Asia/Shanghai')";
        Tuple2<Long, Double> jobManagerResourceCpuLoad = getJobManagerResource(computerJobMangerCpuLoad, dataBase);
        return jobManagerResourceCpuLoad;
    }

    // jobManger head 的使用情况
    private Tuple2<Long, Double> queryJobMangerHeadLoad(String job_id, String dataBase) throws ParseException {
        String computerJobMangerHeadLoad =
                "SELECT value "
                        + " FROM jobmanager_Status_JVM_Memory_Heap_Used "
                        + " WHERE job_id_2 = " + "'" + job_id + "' "
                        + " ORDER BY time DESC"
                        + " LIMIT 1 "
                        + " tz('Asia/Shanghai')";
        Tuple2<Long, Double> jobManagerResourceHeadUsed = getJobManagerResource(computerJobMangerHeadLoad, dataBase);
        jobManagerResourceHeadUsed.f1 = jobManagerResourceHeadUsed.f1 / (1024 * 1024);
        return  jobManagerResourceHeadUsed;
    }

    // taskManger cpu 的使用情况
    private Tuple2<Long, Double> queryTaskMangerCpuLoad(String job_id, String dataBase, int reportInterval) throws ParseException {
        String computerTaskMangerCpuLoad =
                "SELECT tm_id, value "
                        + " FROM taskmanager_Status_JVM_CPU_Load "
                        + " WHERE job_id_2 = " + "'" + job_id + "' "
                        + " AND time > now() - " + reportInterval + "m "
                        +  " ORDER BY time DESC "
                        + " tz('Asia/Shanghai')";
        HashMap<String, Tuple2<Long, Double>> taskManagerResourceCpuLoad = getTaskManagerResource(computerTaskMangerCpuLoad, dataBase);
        Iterator<Map.Entry<String, Tuple2<Long, Double>>> iteratorTmCupLoad = taskManagerResourceCpuLoad.entrySet().iterator();
        Tuple2<Long, Double> value = null;
        while (iteratorTmCupLoad.hasNext()){
            Map.Entry<String, Tuple2<Long, Double>> next = iteratorTmCupLoad.next();
            // 分类标准
            String taskCpuLoadContainer = next.getKey();
            value = next.getValue();
        }
        return value;
    }

    //taskManger head 的使用情况
    private Tuple2<Long, Double> queryTaskMangerHeadLoad(String job_id, String dataBase, int reportInterval) throws ParseException {
        String computerTaskMangerHeadLoad =
                "SELECT tm_id, value"
                        + " FROM taskmanager_Status_JVM_Memory_Heap_Used "
                        + " WHERE job_id_2 = " + "'" + job_id + "' "
                        + " AND time > now() - " + reportInterval + "m "
                        + " ORDER BY time DESC "
                        + " tz('Asia/Shanghai')";
        HashMap<String, Tuple2<Long, Double>> taskManagerResourceHeadUsed = getTaskManagerResource(computerTaskMangerHeadLoad, dataBase);
        Iterator<Map.Entry<String, Tuple2<Long, Double>>> iteratorTmHeadUsed = taskManagerResourceHeadUsed.entrySet().iterator();
        Tuple2<Long, Double> value = null;
        while (iteratorTmHeadUsed.hasNext()){
            Map.Entry<String, Tuple2<Long, Double>> next = iteratorTmHeadUsed.next();
            // 分类标准
            String taskHeadUsed = next.getKey();
            value = next.getValue();
            value.f1 = value.f1 / (1024 * 1024);
        }
        return value;
    }

    // 计算延迟
    public Double queryAvgComputerDuration(String job_id, String dataBase, int checkTime) {
        String computerDurationSql =
                "SELECT MEAN(value) "
                        + " FROM taskmanager_job_task_operator_flink_customer_metric_computer_duration "
                        + " WHERE job_id = " + "'" + job_id + "' "
                        + " AND time >= now() - " + checkTime  + "m "
                        + " ORDER BY time DESC "
                        + " tz('Asia/Shanghai')";
        double avgComputerDuration= getAvg(computerDurationSql, dataBase);
        return avgComputerDuration;
    }

    // 获取数据输出条数
    public Double queryNumRecordsOut(String job_id, String dataBase, int reportInterval) throws ParseException {
        String computerNumRecordsOut =
                "SELECT operator_name, count"
                        + " FROM taskmanager_job_task_operator_numRecordsOut "
                        + " WHERE job_id_2 = " + "'" + job_id + "' "
                        + " AND time > now() - " + (reportInterval * 2) + "m "
                        + " ORDER BY time DESC "
                        + " tz('Asia/Shanghai')";
        Double numRecordsOut = getNumRecordsOut(computerNumRecordsOut, dataBase);
        return numRecordsOut;
    }

    private Double getNumRecordsOut(String sql, String database) {
        HashMap<String, Double> hashMap = new HashMap<>();
        double counts = 0;
        QueryResult.Result oneQueryResult = getOneQueryResult(sql, database);
        if (oneQueryResult.getSeries() != null){
            List<List<Object>> queryResultFieldValue = getQueryResultFieldValue(oneQueryResult);
            if (queryResultFieldValue != null && queryResultFieldValue.size() > 0 ){
                for (List<Object> values : queryResultFieldValue) {
                    String operatorName = values.get(1).toString();
                    if (operatorName.contains("Sink:")){
                        double number = (double) values.get(2);
                        if (hashMap.get(operatorName) == null){
                            hashMap.put(operatorName, counts);
                            counts += number;
                        } else {
                            counts -= number;
                        }
                    }
                }
            }
        }
        return counts / 60;
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
}
