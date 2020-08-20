package com.skyon.main;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContinuousDeclineFunctionTest {
    private static final Logger LOG = LoggerFactory.getLogger(ContinuousDeclineFunctionTest.class);

    public static void main(String[] args) throws Exception {

        //flink运行环境设置
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);
        bsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        bsEnv.setParallelism(1);

//        客户号（CUST_NO）、账号（TRADE_ACCOUNT）、交易流水号（TRADE_ID）、交易金额（TRADE_AMOUNT）、交易时间（TRADE_TIME）、交易码（TRADE_CODE）

        String createSourceTableSql = "CREATE TABLE redis_source_table(CUST_NO STRING,TRADE_ACCOUNT STRING,OTHER_ACCOUNT STRING,TRADE_ID STRING,TRADE_AMOUNT DOUBLE,TRADE_TIME TIMESTAMP(3),proctime AS PROCTIME(),TRADE_CODE STRING,WATERMARK FOR TRADE_TIME AS TRADE_TIME - INTERVAL '0' SECOND) WITH ('connector.type' = 'kafka','connector.version' = '0.11','connector.topic' = 'redis_source_topic5','connector.properties.zookeeper.connect' = 'localhost:2181','connector.properties.bootstrap.servers' = 'localhost:9092','connector.properties.group.id' = 'test123','connector.startup-mode' = 'latest-offset','format.type' = 'json')";
        System.out.println("createSourceTableSql:" + createSourceTableSql);
        bsTableEnv.executeSql(createSourceTableSql);

        String createSinkTableSql = "CREATE TABLE result_sink_table(rule_count BIGINT) WITH ('connector.type' = 'kafka','connector.version' = '0.11','connector.topic' = 'result_sink_topic5','connector.properties.zookeeper.connect' = 'localhost:2181','connector.properties.bootstrap.servers' = 'localhost:9092','connector.sink-partitioner' = 'round-robin','format.type' = 'json')";
        System.out.println("createSinkTableSql:" + createSinkTableSql);
        bsTableEnv.executeSql(createSinkTableSql);

        String createFunctionSql = "CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS continuous_decline AS 'com.skyon.function.ContinuousDeclineFunction' LANGUAGE JAVA";
        System.out.println("createFunctionSql:" + createFunctionSql);
        bsTableEnv.executeSql(createFunctionSql);

//        同一用户20分钟内交易金额连续递减次数大于3次;
        String ruleSql = "SELECT continuous_decline(TRADE_AMOUNT) over (PARTITION BY CUST_NO ORDER BY TRADE_TIME RANGE BETWEEN INTERVAL '20' MINUTE preceding AND CURRENT ROW) AS rule_count FROM redis_source_table";
        System.out.println("ruleSql:" + ruleSql);
        bsTableEnv.executeSql(ruleSql);



    }

}
