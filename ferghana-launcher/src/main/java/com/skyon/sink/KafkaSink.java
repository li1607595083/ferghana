package com.skyon.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

public class KafkaSink {

    public KafkaSink(){ }

    /**
     * @param topic
     * @return
     */
    public static FlinkKafkaProducer011<String> of(String topic, String brokelist) {
        return new FlinkKafkaProducer011<>(brokelist, topic, new SimpleStringSchema());
    }

}
