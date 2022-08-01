package com.skyon.flink.data;

public class KafkaConnectTest {

//    public static void main(String[] args) throws Exception {
//
//        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
//        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);
//
//        String kafkaSourceSql = "CREATE TABLE kafkaSourceTable (product_id INT,product STRING,amount INT) WITH ('connector.type' = 'kafka','connector.version' = '0.10','connector.topic' = 'kafkaSourceTable','connector.properties.zookeeper.connect' = 'localhost:2181','connector.properties.bootstrap.servers' = 'localhost:9092','connector.properties.group.id' = 'testGroup','connector.startup-mode' = 'earliest-offset','format.type' = 'json')";
//        String kafkaSinkSql = "CREATE TABLE kafkaSinkTable (product_id INT,product STRING,amount INT) WITH ('connector.type' = 'kafka','connector.version' = '0.10','connector.topic' = 'kafkaSinkTable','connector.properties.zookeeper.connect' = 'localhost:2181','connector.properties.bootstrap.servers' = 'localhost:9092','connector.sink-partitioner' = 'round-robin','format.type' = 'json')";
//        String insertSql = "insert into kafkaSinkTable(product_id,product,amount) select product_id,product,amount from kafkaSourceTable";
//        bsTableEnv.sqlUpdate(kafkaSourceSql);
//        bsTableEnv.sqlUpdate(kafkaSinkSql);
//        bsTableEnv.sqlUpdate(insertSql);
//
//        bsEnv.execute(" test kafka connector demo");
//
//    }
}
