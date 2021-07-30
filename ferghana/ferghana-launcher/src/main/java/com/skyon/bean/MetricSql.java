package com.skyon.bean;

/**
 * @DESCRIPTION:
 * @NAME: TR
 * @DATE: 2021/7/11
 */
public class MetricSql {

    String job_id = "fc77503046d6a82612e9d300888c6acd";
    // metric reporter 时间间隔
    int reportInterval =  1;
    // 检查时间范围
    int checkTime = 5;

    // 近5分钟是否出现背压
    String isBackPressuredSql =
            "SELECT value "
                    + " FROM taskmanager_job_task_isBackPressured "
                    + " WHERE job_id = " + "'" + job_id + "' "
                    + " AND time >= now() - " + checkTime + "m"
                    + " AND value = 'true' "
                    + " tz('Asia/Shanghai')";

    // 近5分钟checkpoint失败次数
    String checkpointFailNumbersSql =
            "SELECT value "
                    + " FROM jobmanager_job_numberOfFailedCheckpoints "
                    + " WHERE job_id = " + "'" + job_id + "' "
                    + " AND time >= now() - " + checkTime + "m "
                    + " ORDER BY time DESC "
                    + " tz('Asia/Shanghai')";

    // 5分钟内重启次数
    String restartsNumbersSql =
            "SELECT value "
                    + " FROM jobmanager_job_numRestarts "
                    + " WHERE job_id = " + "'" + job_id + "' "
                    + " AND time >= now() - " + checkTime  + "m "
                    + " ORDER BY time DESC "
                    + " tz('Asia/Shanghai') ";

    // 5分钟内平均延迟
    String computerDurationSql =
            "SELECT MEAN(value) "
                    + " FROM taskmanager_job_task_operator_flink_customer_metric_computer_duration "
                    + " WHERE job_id = " + "'" + job_id + "' "
                    + " AND time >= now() - " + checkTime  + "m "
                    + " ORDER BY time DESC "
                    + " tz('Asia/Shanghai')";

    // jobManger cpu 的使用情况
    String computerJobMangerCpuLoad =
            "SELECT value, job_id_2 "
                    + " FROM jobmanager_Status_JVM_CPU_Load "
                    + " WHERE job_id_2 = " + "'" + job_id + "' "
                    + " ORDER BY time DESC "
                    + " LIMIT 1 "
                    + " tz('Asia/Shanghai')";

    // jobManger head 的使用情况
    String computerJobMangerHeadLoad =
            "SELECT value "
                    + " FROM jobmanager_Status_JVM_Memory_Heap_Used "
                    + " WHERE job_id_2 = " + "'" + job_id + "' "
                    + " ORDER BY time DESC"
                    + " LIMIT 1 "
                    + " tz('Asia/Shanghai')";

    // taskManger cpu 的使用情况
    String computerTaskMangerCpuLoad =
            "SELECT tm_id, value "
                    + " FROM taskmanager_Status_JVM_CPU_Load "
                    + " WHERE job_id_2 = " + "'" + job_id + "' "
                    + " AND time > now() - " + reportInterval + "m "
                    + " tz('Asia/Shanghai')";

    //taskManger cpu 的使用情况
    String computerTaskMangerHeadLoad =
            "SELECT tm_id, value"
                    + " FROM taskmanager_Status_JVM_Memory_Heap_Used "
                    + " WHERE job_id_2 = " + "'" + job_id + "' "
                    + " AND time > now() - " + reportInterval + "m "
                    + " tz('Asia/Shanghai')";

    // 获取数据输出条数
    String computerNumRecordsOut =
            "SELECT operator_name, count"
                    + " FROM taskmanager_job_task_operator_numRecordsOut "
                    + " WHERE job_id_2 = " + "'" + job_id + "' "
                    + " AND time > now() - " + reportInterval + "m "
                    + " ORDER BY time DESC "
                    + " tz('Asia/Shanghai')";
}
