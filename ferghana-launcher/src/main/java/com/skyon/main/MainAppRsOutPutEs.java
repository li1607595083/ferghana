package com.skyon.main;

import com.skyon.utils.FlinkUtils;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class MainAppRsOutPutEs {

    public static void main(String[] args) throws Exception {

        // Flink datastream 运行环境
        StreamExecutionEnvironment dbEnv = FlinkUtils.dbEnv();

        // 指定事件类型
        dbEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        dbEnv.setParallelism(1);

        // Flink table 运行环境
        StreamTableEnvironment dbTableEnv = FlinkUtils.dbTableEnv(dbEnv);

        dbTableEnv.executeSql("CREATE TABLE order_source_topic("
                + "user_id STRING,"
                + "user_name STRING,"
                + "user_country STRING,"
                + "user_age INTEGER,"
                + "user_like STRING,"
                + "orderTime TIMESTAMP(3),"
                + "proctime AS PROCTIME(),"
                + "WATERMARK FOR orderTime AS orderTime - INTERVAL '0' SECOND"
                + ") WITH ("
                + "'connector.type' = 'kafka',"
                + "'connector.version' = '0.11',"
                + "'connector.topic' = 'order_source_topic',"
                + "'connector.properties.zookeeper.connect' = 'master:2181',"
                + "'connector.properties.bootstrap.servers' = 'master:9092,slave:9092',"
                + "'connector.properties.group.id' = 'test123',"
                + "'connector.startup-mode' = 'latest-offset',"
                + "'format.type' = 'json')");

        dbTableEnv.sqlQuery("SELECT * FROM order_source_topic").printSchema();

        String sql_kafka = "SELECT * FROM order_source_topic";
        Table table_kafka = dbTableEnv.sqlQuery(sql_kafka);
        table_kafka.printSchema();
        dbTableEnv.toAppendStream(table_kafka, Row.class).print();

        dbTableEnv.executeSql("CREATE TABLE myUserTable ("
                + "user_id STRING,"
                + "user_name STRING,"
                + "user_country STRING,"
                + "user_age INTEGER,"
                + "user_like STRING,"
                + "PRIMARY KEY (user_id) NOT ENFORCED"
                + ") WITH ("
                + "'connector' = 'elasticsearch-7',"
                + "'hosts' = 'http://spark02:9200;http://slave:9200',"
                + "'index' = 'user'"
                + ")");

        String sql_output = "INSERT INTO myUserTable "
                + "SELECT user_id, user_name, user_country, user_age, user_like FROM order_source_topic";

        dbTableEnv.executeSql(sql_output);

        // DataStream执行运行
        dbEnv.execute("feqg");
    }

}
