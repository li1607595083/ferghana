package com.skyon.main;

import com.skyon.app.AppDealOperation;
import com.skyon.sink.KafkaSink;
import com.skyon.utils.FlinkUtils;
import com.skyon.utils.KafkaUtils;
import kafka.utils.ZkUtils;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class MainAppOneStream {

    private static final Logger LOG = LoggerFactory.getLogger(MainAppOneStream.class);

    public static void main(String[] args) throws Exception {

//        // Flink datastream 运行环境
//        StreamExecutionEnvironment dbEnv = FlinkUtils.dbEnv();
//
//        // 指定事件类型
//        dbEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//
//        // Flink table 运行环境
//        StreamTableEnvironment dbTableEnv = FlinkUtils.dbTableEnv(dbEnv);
//
//        // 创建一个appOneStreamOver实例
//        Properties properties = new Properties();
//        AppDealOperation appOneStreamOver = AppDealOperation.of(properties);
//
//        // 创建中间topic
//        ZkUtils zkUtils = KafkaUtils.getZkUtils("spark01:2181,spark02:2181,spark03:2181");
//        KafkaUtils.createKafkaTopic(zkUtils, appOneStreamOver.middle_table);
//        KafkaUtils.clostZkUtils(zkUtils);
//
//        // 创建数据原表，sql_set第一个sql为创建数据原表语句
//        dbTableEnv.executeSql(properties.getProperty("").replaceAll("@_@", " "));
//
//        // 指定查询操作，并进行合并成一个DataStream
//        DataStream<String> sqlQueryAndUnion = appOneStreamOver.sqlQueryAndUnion(dbTableEnv);
//
//        // 变量拼接
//        SingleOutputStreamOperator<String> keyedProcess_indicators_merge = appOneStreamOver.mergerIndicators(sqlQueryAndUnion);
//
//        // sink 到中间topic
//        keyedProcess_indicators_merge.addSink(KafkaSink.of(appOneStreamOver.middle_table, "")).setParallelism(1).name("sink_kafka");
//
//        // 使用中间topic创建表
//        appOneStreamOver.createMinddleTable(appOneStreamOver.middle_schema, dbTableEnv);
//
//        // 测试查询
//        Table sqlTestQuery = dbTableEnv.sqlQuery("SELECT * FROM redis_middle_table");
//
//        // 将Table转化为DataStream进行输出打印
//        dbTableEnv.toAppendStream(sqlTestQuery,Row.class)
//                .map(x -> x.toString())
//                .startNewChain()
//                .slotSharingGroup("others")
//                .setParallelism(1)
//                .name("map_result_deal")
//                .print("nice")
//                .setParallelism(1)
//                .name("print_result");
//
//        // DataStream执行运行
//        dbEnv.execute("MainAppOneStream");
    }

}

