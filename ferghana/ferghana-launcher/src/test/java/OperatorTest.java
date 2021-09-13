
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.util.MathUtils;
import org.junit.Test;

import java.util.*;

public class OperatorTest {

    @Test
    public void jiami() throws Exception {
        //09+11+19+28;04+05+29|04+05+09
        String mimi = "eyJzb3VyY2VUYWJsZVNxbCI6IkNSRUFURSBUQUJMRSBgb3JkZXJfbWFpbl90YWJsZWAoYG9yZGVySWRgIFNUUklORyxgcHJvZHVjdElkYCBTVFJJTkcsYHByb2R1Y3ROdW1iZXJgIElOVCxgb3JkZXJBbW91bnRgIERPVUJMRSxgZGlzY291bnRBbW91bnRgIERPVUJMRSxgaWZQYXlgIEJPT0xFQU4sYG9yZGVyVGltZWAgVElNRVNUQU1QLHByb2N0aW1lIEFTIFBST0NUSU1FKCksV0FURVJNQVJLIEZPUiBgb3JkZXJUaW1lYCBhcyBvcmRlclRpbWUgLSBJTlRFUlZBTCAnNScgU0VDT05EKSBXSVRIICgnY29ubmVjdG9yJyA9ICdrYWZrYS0wLjExJyAsJ3RvcGljJyA9ICdvcmRlcl9tYWluX3RhYmxlX2RldGFpbCcsJ3Byb3BlcnRpZXMuYm9vdHN0cmFwLnNlcnZlcnMnID0gJzE5Mi4xNjguNC45NTo5MDkyJywncHJvcGVydGllcy5ncm91cC5pZCcgPSAnY3BfMDEnLCdzY2FuLnN0YXJ0dXAubW9kZScgPSAnbGF0ZXN0LW9mZnNldCcsJ2Zvcm1hdCcgPSAnanNvbicpIiwiZmllbGRPdXROdW0iOjIsInNpbmtTcWwiOiIgaW5zZXJ0IGludG8gdGVzdF9zaW5rX2thZmthXzAwMSAoc2VsZWN0IG9yZGVyX3F1YW5fc3RhdGlzLG9yZGVySWQgRlJPTSB0bXBfb3JkZXJfbWlhbl90YWJsZV9ubykiLCJjb25uZWN0b3JUeXBlIjoiMDEiLCJ2YXJpYWJsZVBhY2tFbiI6Im9yZGVyX21pYW5fdGFibGVfbm8iLCJrYWZrYVRvcGljIjoidGVzdF9zaW5rX2thZmthXzAwMSIsInZhcmlhYmxlU3FscyI6IlNFTEVDVCBvcmRlcklkLCBjb3VudChvcmRlcklkKSAgb3ZlciggT1JERVIgQlkgb3JkZXJUaW1lIFJBTkdFIEJFVFdFRU4gSU5URVJWQUwgJzMwJyBNSU5VVEUgcHJlY2VkaW5nIEFORCBDVVJSRU5UIFJPVykgIEFTIG9yZGVyX3F1YW5fc3RhdGlzIEZST00gb3JkZXJfbWFpbl90YWJsZSIsImthZmthWksiOiIxOTIuMTY4LjQuOTU6MjE4MSIsInNvdXJjZVByaW1hcnlLZXkiOiJvcmRlcklkIiwia2Fma2FBZGRyZXNzIjoiMTkyLjE2OC40Ljk1OjkwOTIiLCJ3YXRlck1hcmsiOiJvcmRlclRpbWV8NSIsInJ1bk1vZGUiOiIwMiJ9";
        String jiemi = new String(Base64.getDecoder().decode(mimi));
        System.out.println("解密后:\t" +jiemi);
        String str = "{\"sourceTableSql\":\"CREATE TABLE `order_main_table`(`orderId` STRING,`productId` STRING,`productNumber` INT,`orderAmount` DOUBLE,`discountAmount` DOUBLE,`ifPay` BOOLEAN,`orderTime` TIMESTAMP,proctime AS PROCTIME(),WATERMARK FOR `orderTime` as orderTime - INTERVAL '5' SECOND) WITH ('connector' = 'kafka-0.11' ,'topic' = 'order_main_table_detail','properties.bootstrap.servers' = '192.168.4.95:9092','properties.group.id' = 'cp_01','scan.startup.mode' = 'latest-offset','format' = 'json')\",\"sinkSql\":\" insert into test_sink_kafka_001 (select * ,index_1, index_2, index_3, index_4 FROM tmp_order_mian_table_no)\",\"connectorType\":\"01\",\"variablePackEn\":\"order_mian_table_no\",\"kafkaTopic\":\"test_sink_kafka_001\",\"aggNoPartitionSql\":\"SELECT *, sum(orderAmount)  over(PARTITION BY productId ORDER BY orderTime RANGE BETWEEN INTERVAL '30' MINUTE preceding AND CURRENT ROW)  AS index_1 FROM tmp_table;SELECT *, sum(orderAmount)  over(PARTITION BY ifPay ORDER BY orderTime RANGE BETWEEN INTERVAL '30' MINUTE preceding AND CURRENT ROW)  AS index_2 FROM tmp_table;SELECT *, sum(orderAmount)  over(PARTITION BY orderId ORDER BY orderTime RANGE BETWEEN INTERVAL '30' MINUTE preceding AND CURRENT ROW)  AS index_3 FROM tmp_table;SELECT *, count(orderId)  over( ORDER BY orderTime RANGE BETWEEN INTERVAL '30' MINUTE preceding AND CURRENT ROW)  AS index_4 FROM tmp_table\",\"kafkaZK\":\"192.168.4.95:2181\",\"sourcePrimaryKey\":\"orderId\",\"kafkaAddress\":\"192.168.4.95:9092\",\"waterMark\":\"orderTime|5\",\"runMode\":\"02\"}";
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

    @Test
    public void re(){
        int count = 128;
        int d = 0;
        for (int i = 0; i < 20000; i++) {
            int a = MathUtils.murmurHash((i + "").hashCode()) % 128;
            if (a == d){
                System.out.println(i);
                d += 1;
                count  -= 1;
                if (count < 0){
                    break;
                }
            }
        }
        System.out.println(13.42 * 1.1 * 1.1 * 1.1 * 1.1 * 2300);
    }

}
