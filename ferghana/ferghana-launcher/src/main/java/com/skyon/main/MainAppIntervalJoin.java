package com.skyon.main;

import com.skyon.bean.ParameterName;
import com.skyon.utils.FlinkUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Properties;


public class MainAppIntervalJoin {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment dbEnv = FlinkUtils.dbEnv();
        dbEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        dbEnv.setParallelism(2);

        StreamTableEnvironment dbTableEnv = FlinkUtils.dbTableEnv(dbEnv, new Properties());

        //{"product_no":"002","order_time":"2020-10-18 18:53:24.634"}
        //{"product_no":"004","order_time":"2020-10-18 18:53:19.634"}
        //{"product_no":"004","order_time":"2020-10-18 18:53:19.635"}
        //{"product_no":"100","order_time":"2020-10-18 18:53:27.636"}
        //{"product_no":"005","order_time":"2020-10-18 18:53:27.637"}
        //{"product_no":"003","order_time":"2020-10-18 18:53:36.636"}
        //{"product_no":"004","order_time":"2020-10-18 18:53:33.635"}
        //{"product_no":"005","order_time":"2020-10-18 18:53:40.636"}
        //{"product_no":"006","order_time":"2020-10-18 18:53:51.636"}
        //{"product_no":"007","order_time":"2020-10-18 18:54:33.634"}
        //{"product_no":"009","order_time":"2020-10-18 18:53:36.634"}
        //{"product_no":"010","order_time":"2020-10-18 18:55:37.634"}
        String source_001 = "CREATE TABLE `interval_join_001`("
                + "`product_no` STRING,"
                + "`qwer` STRING,"
                + "proctime AS PROCTIME(),"
                + "`order_time` TIMESTAMP,"
                + "WATERMARK FOR `order_time` as `order_time` - INTERVAL '5' SECOND"
                + ") WITH ("
                + "'connector' = 'kafka-0.11' ,"
                + "'topic' = 'interval_join_001',"
                + "'properties.bootstrap.servers' = 'master:9092',"
                + "'properties.group.id' = 'ij_01',"
                + "'scan.startup.mode' = 'latest-offset',"
                + "'format' = 'json')";

        //{"product_no":"001","trade_time":"2020-10-18 18:53:24.634","trade_amount":"200"}
        //{"product_no":"002","trade_time":"2020-10-18 18:53:27.634","trade_amount":"300"}
        //{"product_no":"003","trade_time":"2020-10-18 18:53:27.636","trade_amount":"400"}
        //{"product_no":"004","trade_time":"2020-10-18 18:53:38.637","trade_amount":"200"}
        //{"product_no":"005","trade_time":"2020-10-18 18:53:41.634","trade_amount":"500"}
        //{"product_no":"006","trade_time":"2020-10-18 18:53:42.000","trade_amount":"200"}
        //{"product_no":"007","trade_time":"2020-10-18 18:54:33.634","trade_amount":"200"}
        //{"product_no":"009","trade_time":"2020-10-18 18:53:37.634","trade_amount":"200"}
        String source_002 = "CREATE TABLE `interval_join_002`("
                + "`trade_amount` DOUBLE,"
                + "`product_no` STRING,"
                + "proctime AS PROCTIME(),"
                + "`trade_time` TIMESTAMP,"
                + "WATERMARK FOR `trade_time` as trade_time - INTERVAL '1' SECOND"
                + ") WITH ("
                + "'connector' = 'kafka-0.11' ,"
                + "'topic' = 'interval_join_002',"
                + "'properties.bootstrap.servers' = 'master:9092',"
                + "'properties.group.id' = 'ts_01',"
                + "'scan.startup.mode' = 'latest-offset',"
                + "'format' = 'json')";
        HashMap<String, String> stringStringHashMap = new HashMap<>();
        stringStringHashMap.put(ParameterName.IDLE_TIME_OUT, 5 * 1000 + "");
        stringStringHashMap.put(ParameterName.TWO_STREAM_JOIN_DELAY_TIME, (1000 + ((1000 + 3000) / 2)  + 1) + "");
        ParameterTool parameterTool = ParameterTool.fromMap(stringStringHashMap);
        dbEnv.getConfig().setGlobalJobParameters(parameterTool);

        String join_sql = "SELECT interval_join_002.trade_amount, interval_join_001.product_no, interval_join_001.order_time "
                + "FROM interval_join_001 "
                + "LEFT JOIN interval_join_002 "
                + "ON  interval_join_001.product_no = interval_join_002.product_no "
                + "AND interval_join_001.order_time BETWEEN interval_join_002.trade_time + INTERVAL '-1' SECOND AND interval_join_002.trade_time + INTERVAL '3' SECOND";


        String jiSunSql = "SELECT trade_amount, product_no, count(product_no)  over(PARTITION BY product_no ORDER BY order_time RANGE BETWEEN INTERVAL '30' MINUTE preceding AND CURRENT ROW)  AS trade_amount_sum_per_type_001_003  FROM interval_join_001_interval_join_002";
        dbTableEnv.executeSql(source_001);
        dbTableEnv.executeSql(source_002);
        Table table = dbTableEnv.sqlQuery(join_sql);

        dbTableEnv.createTemporaryView("interval_join_001_interval_join_002", table);
        Table table4 = dbTableEnv.sqlQuery(jiSunSql);
        dbTableEnv.toAppendStream(table4, Row.class, "qewer").print("qer\t\t:");

        dbEnv.execute();
    }

}


