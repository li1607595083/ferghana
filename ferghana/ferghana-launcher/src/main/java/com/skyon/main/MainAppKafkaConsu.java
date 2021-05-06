package com.skyon.main;

import com.skyon.utils.FlinkUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class MainAppKafkaConsu {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment dbEnv = FlinkUtils.dbEnv();
        dbEnv.setParallelism(1);
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "tes1");
        properties.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG,"read_committed");
        FlinkKafkaConsumer011<String> output = new FlinkKafkaConsumer011(
                "OUTPUT",
                new SimpleStringSchema(),
                properties
        );


        DataStreamSource dataStreamSource = dbEnv.addSource(output);
        dataStreamSource.print();

        dbEnv.execute();
    }

}
