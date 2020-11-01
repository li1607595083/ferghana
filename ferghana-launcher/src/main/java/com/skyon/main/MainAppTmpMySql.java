package com.skyon.main;

import com.skyon.utils.FlinkUtils;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

public class MainAppTmpMySql {

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
                + "name STRING,"
                + "unitPrice DOUBLE,"
                + "PRIMARY KEY (productId) NOT ENFORCED"
                + ") with ("
                + "'connector' = 'jdbc',"
                + "'url' = 'jdbc:mysql://spark02:3306/test?characterEncoding=UTF-8',"
                + "'table-name' = 'products',"
                + "'username' = 'root',"
                + "'password' = '147268Tr',"
                + "'driver' = 'com.mysql.cj.jdbc.Driver'"
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


        String sql_output = "INSERT INTO ordeproduct"
                + "(SELECT o.orderId, o.productId, o.units, o.orderTime, o.proctime, p.name, p.unitPrice "
                + "FROM  order_source_topic AS o "
                + "LEFT JOIN products FOR SYSTEM_TIME AS OF o.proctime AS p "
                + "ON o.productId = p.productId)";

        String resutlSql = sql_output.substring(0, sql_output.length() - 1).split("\\(", 2)[1];
        System.out.println(resutlSql);
        String resultTable = "CREATE TABLE ordeproduct (";
        Table table = dbTableEnv.sqlQuery(resutlSql);
        TableSchema schema = table.getSchema();
        table.printSchema();
        String[] fieldNames = schema.getFieldNames();
        for (String fieldName : fieldNames) {
            DataType dataType = schema.getFieldDataType(fieldName).get();
            String fieldType = dataType.toString().split("NOT NULL")[0];
            resultTable = resultTable + fieldName + " " + fieldType  + ",";
            System.out.println(fieldName + ": " + fieldType);
        }

        resultTable = resultTable + "PRIMARY KEY (orderId) NOT ENFORCED"
                + ") with ("
                + "'connector' = 'jdbc',"
                + "'url' = 'jdbc:mysql://spark02:3306/test?characterEncoding=UTF-8',"
                + "'table-name' = 'ordeproduct',"
                + "'username' = 'root',"
                + "'password' = '147268Tr',"
                + "'driver' = 'com.mysql.cj.jdbc.Driver'"
                + ")";

        System.out.println(resultTable);

        dbTableEnv.executeSql(resultTable);

        table.executeInsert("ordeproduct");

    }

}
