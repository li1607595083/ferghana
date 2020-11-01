package com.skyon.utils;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZkUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.security.JaasUtils;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaUtils {

    private static int sessionTimeout = 30000;
    private static int connectionTimeout = 30000;

    public  KafkaUtils() {}

    /**
     * Create Zookeeper connection
     * @param zkUrl
     * @return
     */
    public static ZkUtils getZkUtils(String zkUrl) {
        return ZkUtils.apply(zkUrl, sessionTimeout, connectionTimeout, JaasUtils.isZkSecurityEnabled());
    }

    /**
     * Close ZkUtils
     */
    public static void clostZkUtils(ZkUtils zkUtils) {
        zkUtils.close();
    }

    /**
     * Create kafka topic
     */
    public static void createKafkaTopic(ZkUtils zkUtils, String topic) {
        boolean ifNotExists = kafkaTopicExists(zkUtils, topic);
        if (!ifNotExists){
            AdminUtils.createTopic(zkUtils, topic, 1, 1, new Properties(), RackAwareMode.Enforced$.MODULE$);
        }
    }

    /**
     * Create kafka topic
     */
    public static void createKafkaTopic(ZkUtils zkUtils, String topic, int partitions, int replicationFactor) {
        boolean ifNotExists = kafkaTopicExists(zkUtils, topic);
        if (!ifNotExists){
            AdminUtils.createTopic(zkUtils, topic, partitions, replicationFactor, new Properties(), RackAwareMode.Enforced$.MODULE$);
        }
    }

    /**
     * Determine whether the Kafka Topic exists
     * @param topic
     * @return
     */
    public static boolean kafkaTopicExists(ZkUtils zkUtils, String topic){
         return zkUtils.getAllTopics().toList().contains(topic);
    }

    /**
     * Kafka producer
     * @param topic
     * @param data
     */
    public static void  kafkaProducer(String topic, Object[] data, String brokeList){
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", brokeList);
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("acks", "all");
        KafkaProducer producer  = new KafkaProducer<String, String>(props);
        for (Object message : data) {
            producer.send(new ProducerRecord<String, String>(topic, message.toString()));
        }
        producer.close();
    }

    /**
     * Delete topic
     * @param zkUtils
     * @param topic
     */
    public static void deleteKafkaTopic(ZkUtils zkUtils, String topic) {
        if (kafkaTopicExists(zkUtils, topic)){
            AdminUtils.deleteTopic(zkUtils, topic);
        }
    }

}
