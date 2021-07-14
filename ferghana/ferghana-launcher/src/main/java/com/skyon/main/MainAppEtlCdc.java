package com.skyon.main;

import com.skyon.utils.FlinkUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Properties;

public class MainAppEtlCdc {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment dbEnv = FlinkUtils.dbEnv();
        dbEnv.setParallelism(4);
        StreamTableEnvironment dbTableEnv = FlinkUtils.dbTableEnv(dbEnv, new Properties());
        ParameterTool parameterTool = ParameterTool.fromMap(new HashMap<String, String>());
        dbEnv.getConfig().setGlobalJobParameters(parameterTool);

        String cdcSource = "CREATE TABLE orders ("
                + "  order_id INT,"
                + "  product_id INT,"
                + "  order_status INT,"
                + " tradTime timestamp "
                + ") WITH ("
                + "  'connector' = 'mysql-cdc',"
                + "  'hostname' = '192.168.4.95',"
                + "  'port' = '3306',"
                + "  'username' = 'root',"
                + "  'password' = 'Skyon@1234',"
                + "  'database-name' = 'test',"
                + "  'table-name' = 'source_topic1111111110',"
//                + " 'debezium.snapshot.mode' = 'schema_only'，" // 不扫描全表
                + " 'debezium.snapshot.mode' = 'initial'，" // 扫描全表
                + " 'server-time-zone'= 'Asia/Shanghai'"
                + ")";

        // 其次是在cdc创建表语句里面需要添加写死一个参数: server-time-zone'= 'Asia/Shanghai'

        dbTableEnv.executeSql(cdcSource);

        Table table_1 = dbTableEnv.sqlQuery("SELECT * FROM orders");
//        Table table_2 = dbTableEnv.sqlQuery("SELECT product_id FROM orders");

        dbTableEnv.toRetractStream(table_1, Row.class).map(new MapFunction<Tuple2<Boolean, Row>, Tuple2<String, Row>>() {
            @Override
            public Tuple2<String,Row> map(Tuple2<Boolean, Row> value) throws Exception {
                return Tuple2.of(value.f1.getKind().shortString(), value.f1);
            }
        }).print("first\t");

//        dbTableEnv.toRetractStream(table_2, Row.class)
//                .print("second\t");


        dbEnv.execute("etlTest");
    }

}


