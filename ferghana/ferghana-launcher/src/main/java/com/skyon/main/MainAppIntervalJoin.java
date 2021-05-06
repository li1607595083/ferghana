package com.skyon.main;

import com.skyon.utils.FlinkUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.HashMap;


public class MainAppIntervalJoin {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment dbEnv = FlinkUtils.dbEnv();
        dbEnv.setParallelism(1);

        StreamTableEnvironment dbTableEnv = FlinkUtils.dbTableEnv(dbEnv);

        //{"product_no":"001","order_time":"2020-10-18 18:53:25.634"}
        //{"product_no":"100","order_time":"2020-10-18 18:53:25.634"}
        //{"product_no":"002","order_time":"2020-10-18 18:53:32.634"}
        //{"product_no":"003","order_time":"2020-10-18 18:53:33.634"}
        //{"product_no":"004","order_time":"2020-10-18 18:53:33.635"}
        //{"product_no":"005","order_time":"2020-10-18 18:53:40.636"}
        //{"product_no":"006","order_time":"2020-10-18 18:53:51.636"}
        //{"product_no":"007","order_time":"2020-10-18 18:54:33.634"}
        //{"product_no":"009","order_time":"2020-10-18 18:53:36.634"}
        //{"product_no":"010","order_time":"2020-10-18 18:53:37.634"}
        String source_001 = "CREATE TABLE `interval_join_001`("
                + "`order_time` TIMESTAMP,"
                + "proctime AS PROCTIME(),"
                + "`product_no` STRING,"
                + "WATERMARK FOR `order_time` as `order_time` - INTERVAL '1' SECOND"
                + ") WITH ("
                + "'connector' = 'kafka-0.11' ,"
                    + "'topic' = 'interval_join_001',"
                + "'properties.bootstrap.servers' = 'master:9092',"
                + "'properties.group.id' = 'ij_01',"
                + "'scan.startup.mode' = 'latest-offset',"
                + "'format' = 'json')";
        //{"product_no":"001","trade_time":"2020-10-18 18:53:22.634","trade_amount":"200"}
        //{"product_no":"002","trade_time":"2020-10-18 18:53:32.634","trade_amount":"300"}
        //{"product_no":"003","trade_time":"2020-10-18 18:53:33.634","trade_amount":"400"}
        //{"product_no":"004","trade_time":"2020-10-18 18:53:33.635","trade_amount":"200"}
        //{"product_no":"005","trade_time":"2020-10-18 18:53:41.634","trade_amount":"500"}
        //{"product_no":"006","trade_time":"2020-10-18 18:53:42.000","trade_amount":"200"}
        //{"product_no":"007","trade_time":"2020-10-18 18:54:33.634","trade_amount":"200"}
        //{"product_no":"009","trade_time":"2020-10-18 18:53:37.634","trade_amount":"200"}
        String source_002 = "CREATE TABLE `interval_join_002`("
                + "`trade_amount` DOUBLE,"
                + "`trade_time` TIMESTAMP,"
                + "`product_no` STRING,"
                + "proctime AS PROCTIME(),"
                + "WATERMARK FOR `trade_time` as trade_time - INTERVAL '1' SECOND"
                + ") WITH ("
                + "'connector' = 'kafka-0.11' ,"
                + "'topic' = 'interval_join_002',"
                + "'properties.bootstrap.servers' = 'master:9092',"
                + "'properties.group.id' = 'ts_01',"
                + "'scan.startup.mode' = 'latest-offset',"
                + "'format' = 'json')";

        ParameterTool parameterTool = ParameterTool.fromMap(new HashMap<String, String>());
        dbEnv.getConfig().setGlobalJobParameters(parameterTool);
        //CAST('123' AS INT) AS qwer,      ，    CONCAT(interval_join_001.product_no, '	123') AS product_no     interval_join_001.product_no
        // leftRelativeSize + (leftRelativeSize + rightRelativeSize) / 2 + 1
        String idNess = 3000 + (3000 + 5000) / 2 + 1 + 1 * 1000 + "";
        System.out.println(idNess);
        String join_sql = "SELECT   interval_join_001.order_time, interval_join_002.trade_amount, CONCAT(interval_join_001.product_no, '\t"
                + idNess
                + "') AS product_no "
                + "FROM interval_join_001 "
                + "LEFT JOIN interval_join_002 "
                + "ON  interval_join_001.product_no = interval_join_002.product_no "
                + "AND interval_join_001.order_time BETWEEN interval_join_002.trade_time + INTERVAL '-3' SECOND AND interval_join_002.trade_time + INTERVAL '5' SECOND";


        String jiSunSql = "SELECT product_no, count(trade_amount)  over(PARTITION BY product_no ORDER BY order_time RANGE BETWEEN INTERVAL '30' MINUTE preceding AND CURRENT ROW)  AS trade_amount_sum_per_type_001_003  FROM interval_join_001_interval_join_002";


        System.out.println(source_001);
        System.out.println(source_002);
        System.out.println(join_sql);
        System.out.println("SELECT product_no, count(trade_amount)  over(PARTITION BY product_no ORDER BY order_time RANGE BETWEEN INTERVAL '30' MINUTE preceding AND CURRENT ROW)  AS trade_amount_sum_per_type_001_003  FROM table_name_001_mb");
        dbTableEnv.executeSql(source_001);
        dbTableEnv.executeSql(source_002);
        Table table = dbTableEnv.sqlQuery(join_sql);
//        Table table3 = table.addOrReplaceColumns(concat($("product_no"), "==123").as("product_no"));

//        TableSchema schema = table.getSchema();
//        String cast_type = "";
//        Expression[] expressions = new Expression[schema.getFieldNames().length];
//        TypeInformation[] typeArr = new TypeInformation[schema.getFieldNames().length];
//        int count = 0;
//        String[] nameArr = new String[schema.getFieldNames().length];
//        for (String fieldName : schema.getFieldNames()) {
//            String type = TypeTrans.getType(schema.getFieldDataType(fieldName).get().toString());
//            cast_type = cast_type + "CAST(" + fieldName + " AS " +  type + ") AS " + fieldName + ", ";
//            typeArr[count] = Types.STRING;;
//            nameArr[count] = fieldName;
//            expressions[count] = $(fieldName);
//            count += 1;
//        }
//        RowTypeInfo rowTypeInfo = new RowTypeInfo(typeArr, nameArr);
//        cast_type = cast_type.trim();
//        cast_type = cast_type.substring(0, cast_type.length() - 1);
//        System.out.println(cast_type);
//        SingleOutputStreamOperator<Row> preDeal = dbTableEnv.toAppendStream(table, Row.class).map(new MapFunction<Row, Row>() {
//            @Override
//            public Row map(Row value) throws Exception {
//                String[] split = value.toString().split(",");
//                String primary_key = split[0] + "\t125";
//                Row row = new Row(split.length);
//                for (int i = 0; i < split.length; i++) {
//                    if (i == 0) {
//                        row.setField(0, primary_key);
//                    } else {
//                        row.setField(i, split[i]);
//                    }
//                }
//                return row;
//            }
//        }).returns(rowTypeInfo);
//
//        dbTableEnv.createTemporaryView("`table_name_001_sky`", preDeal, expressions);
//        Table table2 = dbTableEnv.sqlQuery("SELECT " + cast_type + " FROM table_name_001_sky");
//        TableSchema schema1 = table2.getSchema();
//        for (String fieldName : schema1.getFieldNames()) {
//            System.out.println(fieldName + "\t" + schema1.getFieldDataType(fieldName).get());
//        }

        dbTableEnv.createTemporaryView("interval_join_001_interval_join_002", table);
//        Table table4 = dbTableEnv.sqlQuery("SELECT product_no, count(trade_amount)  over(PARTITION BY product_no ORDER BY order_time RANGE BETWEEN INTERVAL '30' MINUTE preceding AND CURRENT ROW)  AS trade_amount_sum_per_type_001_003  FROM table_name_001_mb");
        Table table4 = dbTableEnv.sqlQuery(jiSunSql);
                dbTableEnv.toAppendStream(table4, Row.class, "qewer").print("qer\t\t:");

        dbEnv.execute();
    }

}


