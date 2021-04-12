package com.skyon.project.system.tuil;

import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;
import org.apache.kafka.common.security.JaasUtils;

public class KafkaUtil {
    public static void deleteKafkaTopic(String zookeeperAddress, String topicName) {
        ZkUtils zkUtils = ZkUtils.
                apply(zookeeperAddress, 30000, 30000, JaasUtils.isZkSecurityEnabled());

        AdminUtils.deleteTopic(zkUtils, topicName);
        zkUtils.close();
    }

}
