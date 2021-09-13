package com.skyon.main;

import com.skyon.bean.ParameterName;
import com.skyon.utils.DataStreamToTable;
import com.skyon.utils.FlinkUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Properties;

/**
 * @DESCRIPTION:
 * @NAME: TR
 * @DATE: 2021/7/27
 */
public class MainNestedQuery {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment dbEnv = FlinkUtils.dbEnv();
        Properties properties = new Properties();
        StreamTableEnvironment dbTableEnv = FlinkUtils.dbTableEnv(dbEnv, properties);

        HashMap<String, String> stringStringHashMap = new HashMap<>();
        stringStringHashMap.put(ParameterName.IDLE_TIME_OUT, 5 * 1000 + "");
        stringStringHashMap.put(ParameterName.TWO_STREAM_JOIN_DELAY_TIME, (1000 + ((1000 + 3000) / 2)  + 1) + "");
        ParameterTool parameterTool = ParameterTool.fromMap(stringStringHashMap);
        dbEnv.getConfig().setGlobalJobParameters(parameterTool);

        dbTableEnv.executeSql(
                "CREATE TABLE `order_main_table`("
                            + "`orderId` STRING,"
                            + "`productId` STRING,"
                            + "`productNumber` INT,"
                            + "`orderAmount` DOUBLE,"
                            + "`discountAmount` DOUBLE,"
                            + "`ifPay` BOOLEAN,"
                            + "`orderTime` TIMESTAMP,"
                            + "proctime AS PROCTIME(),"
                            + "WATERMARK FOR `orderTime` as orderTime - INTERVAL '5' SECOND"
                            + ") WITH ("
                            + "'connector' = 'kafka-0.11' ,"
                            + "'topic' = 'order_main_table_detail',"
                            + "'properties.bootstrap.servers' = '192.168.4.95:9092',"
                            + "'properties.group.id' = 'cp_01',"
                            + "'scan.startup.mode' = 'earliest-offset',"
                            + "'format' = 'json')");

        Table table = dbTableEnv.sqlQuery("SELECT * , count(orderId)  over(ORDER BY orderTime RANGE BETWEEN INTERVAL '30' MINUTE preceding AND CURRENT ROW)  AS indec_001 FROM order_main_table");
        String[] fieldNames = table.getSchema().getFieldNames();
        for (String fieldName : fieldNames) {
            String dataType = table.getSchema().getFieldDataType(fieldName).get().toString();
            System.out.println("first:\t" + fieldName + "\t" + dataType);
        }


        dbTableEnv.createTemporaryView("table_001", table);

        Table table1 = dbTableEnv.sqlQuery("SELECT *, sum(orderAmount)  over(PARTITION BY productId ORDER BY orderTime RANGE BETWEEN INTERVAL '30' MINUTE preceding AND CURRENT ROW)  AS indec_001 FROM table_001");
        String[] fieldNames1 = table1.getSchema().getFieldNames();
        for (String fieldName : fieldNames1) {
            String dataType = table1.getSchema().getFieldDataType(fieldName).get().toString();
            System.out.println("second:\t" + fieldName + "\t" + dataType);
        }

        dbTableEnv.toAppendStream(table1, Row.class);

        dbEnv.execute("NestedQuery");

    }

}
