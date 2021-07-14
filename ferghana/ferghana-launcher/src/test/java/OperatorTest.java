
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.junit.Test;

import java.util.*;

public class OperatorTest {

    @Test
    public void jiami() throws Exception {
        //09+11+19+28;04+05+29|04+05+09
        String str = "{\"sourceTableSql\":\"CREATE TABLE `order_main_table`(`orderId` STRING,`productId` STRING,`productNumber` INT,`orderAmount` DOUBLE,`discountAmount` DOUBLE,`ifPay` BOOLEAN,`orderTime` TIMESTAMP,proctime AS PROCTIME(),WATERMARK FOR `orderTime` as orderTime - INTERVAL '5' SECOND) WITH ('connector' = 'kafka-0.11' ,'topic' = 'order_main_table_detail','properties.bootstrap.servers' = '192.168.4.95:9092','properties.group.id' = 'cp_01','scan.startup.mode' = 'latest-offset','format' = 'json')\",\"connectorType\":\"01\",\"kafkaTopic\":\"test_sink_kafka_001\",\"testTopicName\":\"topic16262593156951\",\"kafkaZK\":\"192.168.4.95:2181\",\"variableSqls\":\"SELECT orderId, count(orderId)  over( ORDER BY orderTime RANGE BETWEEN INTERVAL '30' MINUTE preceding AND CURRENT ROW)  AS order_quan_statis FROM order_main_table\",\"kafkaAddress\":\"192.168.4.95:9092\",\"sinkSql\":\" insert into test_sink_kafka_001 (select order_quan_statis,orderId FROM tmp_order_mian_table_no)\",\"runMode\":\"01\",\"testSourcedata\":[{\"orderTime\":\"2021-02-05 17:04:01.123\",\"orderId\":\"21424210001\"},{\"orderTime\":\"2021-02-05 17:04:01.124\",\"orderId\":\"21424210002\"},{\"orderTime\":\"2021-02-05 17:03:56.125\",\"orderId\":\"21424210003\"}],\"fieldOutNum\":2,\"sourcePrimaryKey\":\"orderId\",\"waterMark\":\"orderTime|5\"}";
        System.out.println(str);
        JSONObject jsonObject = JSON.parseObject(str);
        jsonObject.entrySet().stream().forEach(x -> System.out.println(x));
        byte[] bytes = str.getBytes();
        //Base64 加密
        String encoded = Base64.getEncoder().encodeToString(bytes);
        System.out.println("Base 64 加密后：" + encoded);
        byte[] decode = Base64.getDecoder().decode(encoded);
        System.out.println(new String(decode));
        // taskManger cpu 的使用情况
        double rate = 0.0000027003768868056586;
        System.out.println(rate + "");
    }

}
