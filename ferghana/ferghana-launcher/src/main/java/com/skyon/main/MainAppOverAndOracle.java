package com.skyon.main;

import com.skyon.utils.FlinkUtils;
import com.skyon.utils.StoreUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

public class MainAppOverAndOracle {

    public static void main(String[] args) throws Exception {

        System.setProperty("oracle.jdbc.J2EE13Compliant", "true");
        StreamExecutionEnvironment dbEnv = FlinkUtils.dbEnv();
        dbEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        dbEnv.setParallelism(1);
        StreamTableEnvironment dbTableEnv = FlinkUtils.dbTableEnv(dbEnv);
        Map<String, String> stringLongHashMap = new HashMap<>();
//        stringLongHashMap.put("waterMark", 5000 + "");
//        stringLongHashMap.put("oracle_table","FLINK_ORACLE_TEST_001");
        dbEnv.getConfig().setGlobalJobParameters(ParameterTool.fromMap(stringLongHashMap));
//        dbTableEnv.executeSql("CREATE TABLE `EP_OPENACCT_FLOW_TABLE`(`TRADE_ID` STRING,`TRADE_AMOUNT` DOUBLE,`TRAN_TIME` TIMESTAMP,proctime AS PROCTIME(),WATERMARK FOR `TRAN_TIME` as TRAN_TIME - INTERVAL '0' SECOND) WITH ('connector' = 'kafka-0.11' ,'topic' = 'EP_OPENACCT_FLOW_TOPIC','properties.bootstrap.servers' = 'spark01:9092','properties.group.id' = 'test1','scan.startup.mode' = 'latest-offset','format' = 'json')");
//        Table table = dbTableEnv.sqlQuery("SELECT TRADE_ID, TRADE_AMOUNT, TRAN_TIME FROM EP_OPENACCT_FLOW_TABLE");
//        DataStream<Row> toAppendStream = dbTableEnv.toAppendStream(table, Row.class);
//        DataStream<Row> rebalance = toAppendStream.rebalance();
//        rebalance.print();
//        rebalance.addSink(JdbcSink.sink(
//                "INSERT INTO FLINK_ORACLE_TEST_001 VALUES(?,?,?)",
//                (ps, x) -> {
//                    String[] sp = x.toString().split(",");
//                    ps.setString(1, sp[0]);
//                    ps.setString(2, sp[1]);
//                    ps.setString(3, sp[2]);
//                }
//                ,
//                JdbcExecutionOptions.builder().withBatchSize(2).build()
//                ,
//                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
//                        .withUrl("jdbc:oracle:thin:@192.168.30.72:1521:ORCL")
//                        .withDriverName("oracle.jdbc.driver.OracleDriver")
//                        .withUsername("SCOTT")
//                        .withPassword("tiger")
//                        .build()));
        //{"TRADE_ID":"001","TRAN_TIME":"2021-04-25 10:30:01.123"}
        dbTableEnv.executeSql("CREATE TABLE `main_oracle`("
                + "`TRADE_ID` STRING,"
                + "`TRAN_TIME` TIMESTAMP,"
                + "proctime AS PROCTIME(),"
                + "WATERMARK FOR `TRAN_TIME` as TRAN_TIME - INTERVAL '0' SECOND"
                + ") WITH ("
                + "'connector' = 'kafka-0.11' ,"
                + "'topic' = 'main_oracle',"
                + "'properties.bootstrap.servers' = 'master:9092',"
                + "'properties.group.id' = 'test6',"
                + "'scan.startup.mode' = 'latest-offset',"
                + "'format' = 'json')");

        dbTableEnv.executeSql("CREATE TABLE MyUserTable ("
                + "TRADE_ID VARCHAR(25),"
                + "AMS INT,"
                + "PRIMARY KEY (TRADE_ID) NOT ENFORCED"
                + ") WITH ("
                + "'connector' = 'jdbc',"
                + "'url' = 'jdbc:mysql://master:3306/test',"
                + "'table-name' = 'ferghana',"
                + "'password' = 'Ferghana@1234'"
                + ")");

//        dbTableEnv.executeSql("CREATE TABLE oracle_dim ("
//                + "TRADE_ID STRING,"
//                + "TRADE_AMOUNT DECIMAL(6,2),"
//                + "TRADE_DATE TIMESTAMP,"
//                + "PRIMARY KEY (TRADE_ID) NOT ENFORCED "
//                + ") WITH ("
//                + "'connector' = 'jdbc',"
//                + "'url' = 'jdbc:oracle:thin:@192.168.30.72:1521:ORCL',"
//                + "'table-name' = 'FLINK_ORACLE_TEST_001',"
//                + "'username' = 'SCOTT',"
//                + "'password' = 'tiger',"
//                + "'driver' = 'oracle.jdbc.driver.OracleDriver')");

//        String sql_join =   "SELECT ep.TRADE_ID, my.TRADE_AMOUNT "
//                + "FROM  main_oracle AS ep "
//                + "LEFT JOIN oracle_dim FOR SYSTEM_TIME AS OF ep.proctime AS my "
//                + "ON ep.TRADE_ID = my.TRADE_ID";
        String sql_join = "SELECT  TRADE_ID, COUNT(TRADE_ID) OVER(PARTITION BY TRADE_ID ORDER BY TRAN_TIME RANGE BETWEEN INTERVAL '30' MINUTE preceding AND CURRENT ROW) AS AMS FROM main_oracle";

        Table table = dbTableEnv.sqlQuery(sql_join);
        dbTableEnv.toAppendStream(table, Row.class, sql_join).print();

//        dbTableEnv.executeSql("CREATE TABLE MyOracleTable ("
//                + "TRADE_ID STRING,"
//                + "TRADE_AMOUNT DECIMAL(6,2),"
//                + "TRADE_DATE TIMESTAMP,"
//                + "PRIMARY KEY (TRADE_ID) NOT ENFORCED"
//                + ") WITH ("
//                + "'connector' = 'jdbc',"
//                + "'url' = 'jdbc:oracle:thin:@192.168.30.72:1521:ORCL',"
//                + "'table-name' = 'FLINK_ORACLE_TEST_001',"
//                + "'username' = 'SCOTT',"
//                + "'password' = 'tiger',"
//                + "'driver' = 'oracle.jdbc.driver.OracleDriver'"
//                + ")");
//        String sql_join = "SELECT ep.TRADE_ID, my.TRADE_AMOUNT "
//                + "FROM  EP_OPENACCT_FLOW_TABLE AS ep "
//                + "LEFT JOIN MyOracleTable FOR SYSTEM_TIME AS OF ep.proctime AS my "
//                + "ON ep.TRADE_ID = my.TRADE_ID";

//        Table table = dbTableEnv.sqlQuery("SELECT * FROM MyOracleTable");
//        dbTableEnv.toAppendStream(table, Row.class).print();

//        dbTableEnv.executeSql("CREATE TABLE MyMysqlTable ("
//                + "id STRING,"
//                + "id_timestamp TIMESTAMP,"
//                + "PRIMARY KEY (id) NOT ENFORCED"
//                + ") WITH ("
//                + "'connector' = 'jdbc',"
//                + "'url' = 'jdbc:mysql://spark02:3306/test?characterEncoding=UTF-8&useSSL=false',"
//                + "'table-name' = 'mysql_data_test',"
//                + "'username' = 'root',"
//                + "'password' = '147268Tr',"
//                + "'driver' = 'com.mysql.cj.jdbc.Driver'"
//                + ")");


//        Table table = dbTableEnv.sqlQuery(sql_join);
//        DataStream<Row> rowDataStream = dbTableEnv.toAppendStream(table, Row.class);
//        SingleOutputStreamOperator<Tuple2<String, Integer>> process = rowDataStream
//                .map(x -> Tuple2.of(x.toString(), 1))
//                .returns(Types.TUPLE(Types.STRING, Types.INT))
//                .keyBy(x -> x.f0)
//                .process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
//
//                    ValueState<Integer> value_State;
//                    ValueState<Long> time_state;
//                    int count = 0;
//
//                    @Override
//                    public void open(Configuration parameters) throws Exception {
//                        super.open(parameters);
//                        ValueStateDescriptor<Integer> value_state_desc = new ValueStateDescriptor<>(
//                                "value_state",
//                                Integer.class,
//                                0
//                        );
//                        value_State = getRuntimeContext().getState(value_state_desc);
//                        ValueStateDescriptor<Long> time_state_desc = new ValueStateDescriptor<>(
//                                "time_state",
//                                Long.class
//                        );
//                        time_state = getRuntimeContext().getState(time_state_desc);
//                    }
//
//                    @Override
//                    public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
//                        value_State.update(value_State.value() + value.f1);
//                        long processingTime = ctx.timerService().currentProcessingTime() + 30000;
//                        Long last_proceeTime = time_state.value();
//                        if (last_proceeTime == null) {
//                            ctx.timerService().registerProcessingTimeTimer(processingTime);
//                        } else {
//                            ctx.timerService().deleteProcessingTimeTimer(last_proceeTime);
//                            ctx.timerService().registerProcessingTimeTimer(processingTime);
//                        }
//                        time_state.update(processingTime);
//                        count++;
//                        System.out.println(value + "\t" + count);
//                    }
//
//                    @Override
//                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
//                        super.onTimer(timestamp, ctx, out);
//                        out.collect(Tuple2.of(ctx.getCurrentKey(), value_State.value()));
//                        time_state.clear();
//                        value_State.clear();
//                    }
//                });
//        process.print();


//        Table table = dbTableEnv.sqlQuery(sql_join);
//        DataStream<Row> rowDataStream = dbTableEnv.toAppendStream(table, Row.class);
//        SingleOutputStreamOperator<String> rebalance = rowDataStream.map(x -> x.toString().replaceFirst("\\(", "").replaceFirst("\\)", "") + "," + System.currentTimeMillis());
//        rebalance.print();
//        rebalance.addSink(JdbcSink.sink(
//                "INSERT INTO FLINK_ORACLE_TOTAL VALUES(?,?,?)",
//                (ps, x) -> {
//                    String[] sp = x.split(",");
//                    ps.setString(1, sp[0]);
//                    if (!"null".equals(sp[1])){
//                        ps.setBigDecimal(2, new BigDecimal(sp[1]));
//                    } else {
//                        ps.setNull(2, Types.DECIMAL);
//                    }
//                        ps.setString(3, sp[2]);
//
//                }
//                ,
//                JdbcExecutionOptions.builder().withBatchSize(100).build()
//                ,
//                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
//                        .withUrl("jdbc:oracle:thin:@192.168.30.72:1521:ORCL")
//                        .withDriverName("oracle.jdbc.driver.OracleDriver")
//                        .withUsername("SCOTT")
//                        .withPassword("tiger")
//                        .build()));
//        Properties properties = new Properties();
//        properties.put("joinSql", sql_join);
//        MianAppProcesTest.soureAndDimConcat(properties, dbTableEnv, AppDealOperation.of());
//        Table table = dbTableEnv.sqlQuery("SELECT * FROM oracel_dimtable_test");
//        DataStream<Row> rowDataStream = dbTableEnv.toAppendStream(table, Row.class);
//        rowDataStream.print();
//        String oracel = "CREATE TABLE MyOracleTable ("
//                + "TRADE_ID STRING,"
//                + "TRADE_AMOUNT DOUBLE,"
//                + "TRADE_DATE TIMESTAMP,"
//                + "PRIMARY KEY (TRADE_ID) NOT ENFORCED"
//                + ") WITH ("
//                + "'connector' = 'jdbc',"
//                + "'url' = 'jdbc:oracle:thin:@192.168.30.72:1521:ORCL',"
//                + "'table-name' = 'FLINK_ORACLE_TEST_001',"
//                + "'username' = 'SCOTT',"
//                + "'password' = 'tiger',"
//                + "'driver' = 'oracle.jdbc.driver.OracleDriver'"
//                + ")";
//        StoreUtils.of(oracel).createSqlTable("oracle");
        dbEnv.execute();
    }

}
