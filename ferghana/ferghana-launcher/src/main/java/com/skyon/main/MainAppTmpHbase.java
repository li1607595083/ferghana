package com.skyon.main;

import com.skyon.utils.FlinkUtils;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class MainAppTmpHbase {

    public static void main(String[] args) throws Exception {

        // Flink datastream 运行环境
        StreamExecutionEnvironment dbEnv = FlinkUtils.dbEnv();

        // 指定事件类型
        dbEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        dbEnv.setParallelism(1);

        // Flink table 运行环境
        StreamTableEnvironment dbTableEnv = FlinkUtils.dbTableEnv(dbEnv);

        dbTableEnv.executeSql("CREATE TABLE products ("
                + "productId STRING,"
                + "info ROW(name STRING, unitPrice DOUBLE),"
                + "PRIMARY KEY (productId) NOT ENFORCED"
                + ") with ("
                + " 'connector' = 'hbase-1.4',"
                + "'table-name' = 'test:products',"
                + "'zookeeper.quorum' = 'spark01:2181,spark02:2181,spark03:2181'"
                + ")");

        dbTableEnv.executeSql("CREATE TABLE order_source_topic("
                + "orderId STRING,"
                + "productId STRING,"
                + "units INT,"
                + "orderTime TIMESTAMP(3),"
                + "proctime AS PROCTIME(),"
                + "WATERMARK FOR orderTime AS orderTime - INTERVAL '0' SECOND"
                + ") WITH ("
                + "'connector.type' = 'kafka',"
                + "'connector.version' = '0.11',"
                + "'connector.topic' = 'order_source_topic',"
                + "'connector.properties.zookeeper.connect' = 'spark01:2181,spark02:2181,spark03:2181',"
                + "'connector.properties.bootstrap.servers' = 'spark01:9092,spark02:9092,spark03:9092',"
                + "'connector.properties.group.id' = 'test123',"
                + "'connector.startup-mode' = 'latest-offset',"
                + "'format.type' = 'json')");



        String sql_mysql = "SELECT * FROM products";
        String sql_kafka = "SELECT * FROM order_source_topic";
        Table table_mysql = dbTableEnv.sqlQuery(sql_mysql);
        table_mysql.printSchema();
        dbTableEnv.toAppendStream(table_mysql, Row.class).print();

        Table table_kafka = dbTableEnv.sqlQuery(sql_kafka);
        table_kafka.printSchema();
        dbTableEnv.toAppendStream(table_kafka, Row.class).print();

        //o.orderId, o.productId, o.units, o.orderTime, o.proctime, p.info.name, p.info.unitPrice
        String sql_join = "SELECT  * "
                + "FROM  order_source_topic AS o "
                + "LEFT JOIN products FOR SYSTEM_TIME AS OF o.proctime AS p "
                + "ON o.productId = p.productId";

        System.out.println(sql_join);
        Table table_join = dbTableEnv.sqlQuery(sql_join);
        table_join.printSchema();
        dbTableEnv.toAppendStream(table_join, Row.class).print();

        dbTableEnv.executeSql("CREATE TABLE ordeproduct ("
                + "orderId STRING,"
                + "info ROW(productId STRING, units STRING, orderTime STRING, proctime STRING),"
                + "info2 ROW(name STRING, unitPrice STRING),"
                + "PRIMARY KEY (orderId) NOT ENFORCED"
                + ") with ("
                + "'connector' = 'hbase-1.4',"
                + "'table-name' = 'test:ordeproduct',"
                + "'zookeeper.quorum' = 'spark01:2181,spark02:2181,spark03:2181'"
                + ")");

        String sql_output = "INSERT INTO ordeproduct ("
                + "SELECT orderId, ROW(productId, units, orderTime, proctime) AS info, ROW(name, unitPrice) AS info2 FROM ("
                + "(SELECT CAST(o.orderId AS STRING) AS orderId, CAST(o.productId AS STRING) AS productId, CAST(o.units AS STRING) AS units, CAST(o.orderTime AS STRING) AS orderTime, CAST(o.proctime AS STRING) AS proctime, CAST(p.name AS STRING) AS name, CAST(p.unitPrice AS STRING) AS unitPrice "
                + "FROM  order_source_topic AS o "
                + "LEFT JOIN products FOR SYSTEM_TIME AS OF o.proctime AS p "
                + "ON o.productId = p.productId))"
                + " AS tmp)";


        String fe = "INSERT INTO ordeproduct ("
                + "SELECT orderId, ROW(productId, units, orderTime, proctime) AS info, ROW(name, unitPrice) AS info2 FROM ("
                + "(SELECT CAST(o.orderId AS STRING) AS orderId, CAST(o.productId AS STRING) AS productId, CAST(o.units AS STRING) AS units, CAST(o.orderTime AS STRING) AS orderTime, CAST(o.proctime AS STRING) AS proctime, CAST(p.name AS STRING) AS name, CAST(p.unitPrice AS STRING) AS unitPrice "
                + "FROM  order_source_topic AS o "
                + "LEFT JOIN products FOR SYSTEM_TIME AS OF o.proctime AS p "
                + "ON o.productId = p.productId))"
                + " AS tmp)";

        dbTableEnv.executeSql(sql_output);

        // DataStream执行运行
        dbEnv.execute("feqg");
    }

}
