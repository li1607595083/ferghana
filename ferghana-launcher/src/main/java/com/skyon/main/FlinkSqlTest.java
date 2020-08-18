package com.skyon.main;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkSqlTest {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkSqlTest.class);

    public static void main(String[] args) throws Exception {

        //flink运行环境设置
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);
        bsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        bsEnv.setParallelism(1);

//        客户号（CUST_NO）、账号（TRADE_ACCOUNT）、交易流水号（TRADE_ID）、交易金额（TRADE_AMOUNT）、交易时间（TRADE_TIME）、交易码（TRADE_CODE）

        String createSourceTableSql = "CREATE TABLE redis_source_table(CUST_NO STRING,TRADE_ACCOUNT STRING,OTHER_ACCOUNT STRING,TRADE_ID STRING,TRADE_AMOUNT DOUBLE,TRADE_TIME TIMESTAMP(3),proctime AS PROCTIME(),TRADE_CODE STRING,WATERMARK FOR TRADE_TIME AS TRADE_TIME - INTERVAL '0' SECOND) WITH ('connector.type' = 'kafka','connector.version' = '0.11','connector.topic' = 'redis_source_topic3','connector.properties.zookeeper.connect' = 'localhost:2181','connector.properties.bootstrap.servers' = 'localhost:9092','connector.properties.group.id' = 'test123','connector.startup-mode' = 'latest-offset','format.type' = 'json')";
        System.out.println("createSourceTableSql:" + createSourceTableSql);
        bsTableEnv.executeSql(createSourceTableSql);

        String createDimensionTableSql = "CREATE TABLE test_jdbc (CUST_NO STRING,name STRING,age INT,PRIMARY KEY (CUST_NO) NOT ENFORCED) WITH ('connector' = 'jdbc','url' = 'jdbc:mysql://39.107.230.151:3306/upwisdom','driver' = 'com.mysql.cj.jdbc.Driver','username' = 'MySQL','password' = '2052874615','table-name' = 'users')";
        System.out.println("createDimensionTableSql:" + createDimensionTableSql);
        bsTableEnv.executeSql(createDimensionTableSql);

        String createSinkTableSql = "CREATE TABLE result_sink_table(CUST_NO STRING) WITH ('connector.type' = 'kafka','connector.version' = '0.11','connector.topic' = 'result_sink_topic3','connector.properties.zookeeper.connect' = 'localhost:2181','connector.properties.bootstrap.servers' = 'localhost:9092','connector.sink-partitioner' = 'round-robin','format.type' = 'json')";
        System.out.println("createSinkTableSql:" + createSinkTableSql);
        bsTableEnv.executeSql(createSinkTableSql);


        String createFunctionSql = "CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS not_contain AS 'com.skyon.main.ContainFunction' LANGUAGE JAVA";
        System.out.println("createFunctionSql:" + createFunctionSql);
        bsTableEnv.executeSql(createFunctionSql);

////        rule1:同一用户在10分钟内转账金额在40000-49999元次数>=4;
//        String rule1Sql = "select * from (SELECT TRADE_ID,COUNT(TRADE_ID) over (PARTITION BY CUST_NO ORDER BY TRADE_TIME RANGE BETWEEN INTERVAL '10' MINUTE preceding AND CURRENT ROW) AS rule1_count FROM redis_source_table where TRADE_AMOUNT >=40000 and TRADE_AMOUNT<=49999),LATERAL TABLE(testaa(TRADE_ID,'rule1',rule1_count)) as T(aaa)";
//        System.out.println("rule1Sql:" + rule1Sql);
//        bsTableEnv.executeSql(rule1Sql);
//
//
////        同一账号过去5天内，在同一商户的交易笔数占比超过总交易笔数的80%(同一商户交易笔数);
//        String rule2_1Sql = "select * from (SELECT CUST_NO,TRADE_ID,COUNT(TRADE_ID) over (PARTITION BY TRADE_ACCOUNT,OTHER_ACCOUNT ORDER BY TRADE_TIME RANGE BETWEEN INTERVAL '5' DAY preceding AND CURRENT ROW) AS rule2_count FROM redis_source_table),LATERAL TABLE(testaa(TRADE_ID,'rule2_1',rule2_count)) as T(aaa)";
//        System.out.println("rule2_1Sql:" + rule2_1Sql);
//        bsTableEnv.executeSql(rule2_1Sql);
//
////        同一账号过去5天内，在同一商户的交易笔数占比超过总交易笔数的80%(总交易笔数);
//        String rule2_2Sql = "select * from (SELECT CUST_NO,TRADE_ID,COUNT(TRADE_ID) over (PARTITION BY TRADE_ACCOUNT ORDER BY TRADE_TIME RANGE BETWEEN INTERVAL '5' DAY preceding AND CURRENT ROW) AS rule3_count FROM redis_source_table),LATERAL TABLE(testaa(TRADE_ID,'rule2_2',rule3_count)) as T(aaa)";
//        System.out.println("rule2_2Sql:" + rule2_2Sql);
//        bsTableEnv.executeSql(rule2_2Sql);
//
////        关联mysql表;
//        String rule3Sql = "SELECT test_jdbc.name,test_jdbc.age FROM redis_source_table LEFT JOIN test_jdbc FOR SYSTEM_TIME AS OF redis_source_table.proctime ON redis_source_table.CUST_NO = test_jdbc.CUST_NO";
//
//        System.out.println("rule3Sql:" + rule3Sql);
//        bsTableEnv.executeSql(rule3Sql);

//        contain函数测试;
        String containSql = "insert into result_sink_table select CUST_NO from redis_source_table where not_contain(CUST_NO,'C00') = 1";

        System.out.println("containSql:" + containSql);
        bsTableEnv.executeSql(containSql);

    }

}
