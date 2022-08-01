package com.skyon.main;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.types.Row;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

public class MainSavepointTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment dbEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        dbEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 开启checkpoint,并指定checkpoint的触发间隔,以及checkpoint的模式
        dbEnv.enableCheckpointing(1*60*1000, CheckpointingMode.EXACTLY_ONCE);
        // 设置checkpoint之间的最小间隔时间
        dbEnv.getCheckpointConfig().setMinPauseBetweenCheckpoints(30*1000);
        // 设置checkpoint的最大并行度
        dbEnv.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 当程序停止时,保留checkpoint的状态数据, 默认会删除
        dbEnv.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 优先从checkpoint进行恢复操作,而不是savepoint
        dbEnv.getCheckpointConfig().setPreferCheckpointForRecovery(true);
        // 开启checkpoint未对齐模式
        dbEnv.getCheckpointConfig().enableUnalignedCheckpoints();
        // 重启策略
        dbEnv.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)));
        HashMap<String, String> stringStringHashMap = new HashMap<>();
        stringStringHashMap.put("idleTimeout", 5*1000+"");
        stringStringHashMap.put("delayTime", 5*1000+"");
        dbEnv.getConfig().setGlobalJobParameters(ParameterTool.fromMap(stringStringHashMap));
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        TableConfig tableConfig = TableConfig.getDefault();
        StreamTableEnvironment dbTableEnv = StreamTableEnvironmentImpl.create(dbEnv,bsSettings,tableConfig);
        dbTableEnv.executeSql("CREATE TABLE order_source_topic("
                + "orderId STRING,"
                + "productId STRING,"
                + "amount DOUBLE,"
                + "orderTime TIMESTAMP,"
                + "proctime AS PROCTIME(),"
                + "WATERMARK FOR orderTime as orderTime - INTERVAL '5' SECOND "
                + ") WITH ("
                + "'connector' = 'kafka-0.11' ,"
                + "'topic' = 'order_source_topic_1',"
                + "'properties.bootstrap.servers' = 'master:9092',"
                + "'properties.group.id' = 'test-group-2',"
                + "'scan.startup.mode' = 'specific-offsets',"
                + "'format' = 'json'"
                + ",'scan.startup.specific-offsets' = '" + "partition:0,offset:20" +"')");
        String sql_2 = "SELECT " +
                "orderId , " +
                "orderTime, " +
                "SUM(wc) " +
                "OVER( PARTITION BY productId ORDER BY orderTime RANGE BETWEEN INTERVAL '99' MINUTE preceding AND CURRENT ROW) " +
                "FROM (SELECT *, amount * 10 AS wc  FROM order_source_topic) AS tmp";
        Table table_2 = dbTableEnv.sqlQuery(sql_2);
        DataStream<Row> rowDataStream_2 = dbTableEnv.toAppendStream(table_2, Row.class, sql_2.toUpperCase());
        rowDataStream_2.print();
        String sql_1 = "SELECT * FROM order_source_topic";
        Table table_1 = dbTableEnv.sqlQuery(sql_1);
        DataStream<Row> rowDataStream_1 = dbTableEnv.toAppendStream(table_1, Row.class, sql_1.toUpperCase());
        rowDataStream_1.print();
        dbEnv.execute();
    }

}
