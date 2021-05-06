package com.skyon.sink;

import com.skyon.serialize.StringKeyedSerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Optional;
import java.util.Properties;

public class KafkaSink {

    public KafkaSink(){ }

    /**
     * @param topic
     * @return
     */
    public static FlinkKafkaProducer011<String> transaction(String topic, String brokelist, String kafkaProducersPoolSize) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokelist);
        properties.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 3*60*1000+"");
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        // 设置了retries参数，可以在Kafka的Partition发生leader切换时，Flink不重启，而是做5次尝试
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "5");
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        StringKeyedSerializationSchema stringKeyedSerializationSchema = new StringKeyedSerializationSchema();
        FlinkKafkaProducer011<String> output = new FlinkKafkaProducer011<String>(topic, stringKeyedSerializationSchema,properties, Optional.empty(),FlinkKafkaProducer011.Semantic.EXACTLY_ONCE, Integer.parseInt(kafkaProducersPoolSize) + 3);
        return output;
    }


    public static FlinkKafkaProducer011<String> untransaction(String topic, String brokelist) {
        return new FlinkKafkaProducer011<>(brokelist, topic, new SimpleStringSchema());
    }

}
