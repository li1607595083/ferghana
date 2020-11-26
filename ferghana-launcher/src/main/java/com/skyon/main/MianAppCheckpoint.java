package com.skyon.main;

import com.skyon.sink.KafkaSink;
import com.skyon.utils.FlinkUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.concurrent.TimeUnit;

public class MianAppCheckpoint {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = FlinkUtils.dbEnv();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(1*60*1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1 * 30 * 1000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);
        env.getCheckpointConfig().enableUnalignedCheckpoints();
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)));

        StreamTableEnvironment tableEnv = FlinkUtils.dbTableEnv(env);
//        tableEnv.executeSql("CREATE TABLE SOURCE("
//                + "STR STRING,"
//                + "TRAN_TIME TIMESTAMP,"
//                + "proctime AS PROCTIME(),"
//                + "WATERMARK FOR TRAN_TIME as TRAN_TIME - INTERVAL '0' SECOND"
//                + ") WITH ("
//                + "'connector' = 'kafka-0.11' ,"
//                + "'topic' = 'SOURCE',"
//                + "'properties.bootstrap.servers' = 'master:9092',"
//                + "'properties.group.id' = 'test-1',"
//                + "'scan.startup.mode' = 'latest-offset',"
//                + "'format' = 'json')");
        tableEnv.executeSql("CREATE TABLE trade_info_table ( CUST_NO STRING, TRADE_AMOUNT DOUBLE, TRADE_ACCOUNT STRING, OTHER_ACCOUNT STRING, TRADE_ID STRING, TRADE_TIME TIMESTAMP, proctime AS PROCTIME(), WATERMARK FOR TRADE_TIME as TRADE_TIME - INTERVAL '0' SECOND ) WITH ( 'connector' = 'kafka-0.11' , 'topic' = 'trade_info_table', 'properties.bootstrap.servers' = 'master:9092', 'properties.group.id' = 'eve', 'scan.startup.mode' = 'latest-offset', 'format' = 'json')");

        Table table1 = tableEnv.sqlQuery("SELECT * FROM trade_info_table");
        DataStream<Row> rowDataStream1 = tableEnv.toAppendStream(table1, Row.class);
        rowDataStream1.print();

        Table table = tableEnv.sqlQuery("SELECT TRADE_ID FROM trade_info_table");
        table.printSchema();

        DataStream<Row> rowDataStream = tableEnv.toAppendStream(table, Row.class);

        SingleOutputStreamOperator<Tuple2<String, Integer>> map = rowDataStream
                .map(x -> Tuple2.of(x.toString(), 1)).returns(Types.TUPLE(Types.STRING, Types.INT));

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = map.keyBy(x -> x.f0);

        SingleOutputStreamOperator<String> sum = keyedStream.map(new RichMapFunction<Tuple2<String, Integer>, String>() {
            private transient ValueState<Integer> state;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                ValueStateDescriptor<Integer> valueStateDescriptor = new ValueStateDescriptor<>(
                        "sum",
                        Integer.class
                );
                state = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public String map(Tuple2<String, Integer> value) throws Exception {
                if (state.value() == null){
                    state.update(0);
                }
                value.f1 = value.f1 + state.value();
                state.update(value.f1);
                return value.toString();
            }
        });

        sum.print();

        sum.addSink(KafkaSink.transaction("OUTPUT", "master:9092"));

        env.execute();
    }

}


