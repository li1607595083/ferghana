package com.skyon.main;

import com.skyon.utils.FlinkUtils;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Properties;

public class MainAppTmpMySql {

    public static void main(String[] args) throws Exception {

        // Flink datastream 运行环境
        StreamExecutionEnvironment dbEnv = FlinkUtils.dbEnv();

        // 指定事件类型
        dbEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        dbEnv.setParallelism(4);

        // Flink table 运行环境
        StreamTableEnvironment dbTableEnv = FlinkUtils.dbTableEnv(dbEnv, new Properties());

        dbTableEnv.executeSql("CREATE TABLE products ("
                + "productId STRING,"
                + "info ROW(name STRING, unitPrice DOUBLE),"
                + "PRIMARY KEY (productId) NOT ENFORCED"
                + ") with ("
                + " 'connector' = 'hbase-1.4',"
                + "'table-name' = 'test:products',"
                + "'zookeeper.quorum' = 'master:2181'"
                + ")");

//        dbTableEnv.executeSql("CREATE TABLE products ("
//                + "productId STRING,"
//                + "name STRING,"
//                + "unitPrice DOUBLE,"
//                + "PRIMARY KEY (productId) NOT ENFORCED"
//                + ") with ("
//                + "'connector' = 'jdbc',"
//                + "'url' = 'jdbc:mysql://master:3306/test?characterEncoding=UTF-8',"
//                + "'table-name' = 'products',"
//                + "'username' = 'ferghana',"
//                + "'password' = 'Ferghana@1234',"
//                + "'driver' = 'com.mysql.cj.jdbc.Driver'"
//                + ")");

        dbTableEnv.executeSql("CREATE TABLE usertable ("
                + "orderId STRING,"
                + "name STRING,"
                + "PRIMARY KEY (orderId) NOT ENFORCED"
                + ") with ("
                + "'connector' = 'jdbc',"
                + "'url' = 'jdbc:mysql://master:3306/test?characterEncoding=UTF-8',"
                + "'table-name' = 'usertable',"
                + "'username' = 'ferghana',"
                + "'password' = 'Ferghana@1234',"
                + "'driver' = 'com.mysql.cj.jdbc.Driver'"
                + ")");

        dbTableEnv.executeSql("CREATE TABLE order_source_topic("
                + "orderId STRING,"
                + "productId STRING,"
                + "units INT,"
                + "orderTime TIMESTAMP,"
                + "proctime AS PROCTIME(),"
                + "WATERMARK FOR orderTime as orderTime - INTERVAL '0' SECOND"
                + ") WITH ("
                + "'connector' = 'kafka-0.11' ,"
                + "'topic' = 'order_source_topic',"
                + "'properties.bootstrap.servers' = 'master:9092',"
                + "'properties.group.id' = 'test-group',"
                + "'scan.startup.mode' = 'latest-offset',"
                + "'format' = 'json')");

        String sql_two_join = "SELECT o.orderId, o.productId, o.units, o.orderTime, o.proctime, p.name, p.unitPrice, u.name "
                + "FROM  order_source_topic AS o "
                + "LEFT JOIN products FOR SYSTEM_TIME AS OF o.proctime AS p "
                + "ON o.productId = p.productId "
                + "LEFT JOIN usertable FOR SYSTEM_TIME AS OF o.proctime AS u "
                + "ON o.orderId = u.orderId";

        Table table = dbTableEnv.sqlQuery(sql_two_join);
        table.printSchema();

        DataStream<Row> rowDataStream = dbTableEnv.toAppendStream(table, Row.class, "FEFR");
        rowDataStream.print();
        dbEnv.execute();


//        String sql_output = "INSERT INTO ordeproduct"
//                + "(SELECT o.orderId, o.productId, o.units, o.orderTime, o.proctime, p.name, p.unitPrice "
//                + "FROM  order_source_topic AS o "
//                + "LEFT JOIN products FOR SYSTEM_TIME AS OF o.proctime AS p "
//                + "LEFT JOIN products FOR SYSTEM_TIME AS OF o.proctime AS p "
//                + "ON o.productId = p.productId)";
//
//        String resutlSql = sql_output.substring(0, sql_output.length() - 1).split("\\(", 2)[1];
//        System.out.println(resutlSql);
//        String resultTable = "CREATE TABLE ordeproduct (";
//        Table table = dbTableEnv.sqlQuery(resutlSql);
//        TableSchema schema = table.getSchema();
//        table.printSchema();
//        String[] fieldNames = schema.getFieldNames();
//        for (String fieldName : fieldNames) {
//            DataType dataType = schema.getFieldDataType(fieldName).get();
//            String fieldType = dataType.toString().split("NOT NULL")[0];
//            resultTable = resultTable + fieldName + " " + fieldType  + ",";
//            System.out.println(fieldName + ": " + fieldType);
//        }
//
//        resultTable = resultTable + "PRIMARY KEY (orderId) NOT ENFORCED"
//                + ") with ("
//                + "'connector' = 'jdbc',"
//                + "'url' = 'jdbc:mysql://spark02:3306/test?characterEncoding=UTF-8',"
//                + "'table-name' = 'ordeproduct',"
//                + "'username' = 'root',"
//                + "'password' = '147268Tr',"
//                + "'driver' = 'com.mysql.cj.jdbc.Driver'"
//                + ")";
//
//        System.out.println(resultTable);
//
//        dbTableEnv.executeSql(resultTable);
//
//        table.executeInsert("ordeproduct");

    }

}
