
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.skyon.udf.NullForObject;
import com.skyon.utils.FlinkUtils;
import com.skyon.utils.ParameterUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.*;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;

import java.io.IOException;
import java.sql.*;
import java.util.*;

public class OperatorTest {

    @Test
    public void jiami() throws Exception {
        //09+11+19+28;04+05+29|04+05+09
        String str = "{\"sourceTableSql\":\"CREATE TABLE `order_total`(`orderId` STRING,`orderAmount` DOUBLE,`orderType` STRING,`orderStatues` STRING,`orderTime` TIMESTAMP,`orderTime` TIMESTAMP,proctime AS PROCTIME(),WATERMARK FOR `orderTime` as orderTime - INTERVAL '0' SECOND) WITH ('connector' = 'kafka-0.11' ,'topic' = 'order_wlgxxx','properties.bootstrap.servers' = '192.168.30.72:9092','properties.group.id' = 'gr_001','scan.startup.mode' = 'latest-offset','format' = 'json')\",\"fieldOutNum\":\"2\",\"dimensionTable\":[{\"dimensionTableSql\":\" CREATE TABLE `order_dim_table` ( `orderDimId` STRING,`orderCounts` INT ,PRIMARY KEY (`orderDimId`) NOT ENFORCED) WITH  ('connector' = 'jdbc', 'url' = 'jdbc:mysql://192.168.30.72:3306/ferghana?characterEncoding=UTF-8','driver' = 'com.mysql.cj.jdbc.Driver','username' = 'root','password' = '123456','table-name' = 'order_dim_table')\",\"testDimType\":\"02\"}],\"testTopicName\":\"topic1620889038199\",\"variableSqls\":\"SELECT orderId, sum(orderAmount_RE)  over( ORDER BY orderTime RANGE BETWEEN INTERVAL '30' MINUTE preceding AND CURRENT ROW)  AS prepaid_trade_aomuts FROM(SELECT orderId,IF( orderStatues='1',orderAmount,CAST(ifFalseSetNull() AS DOUBLE)) AS orderAmount_RE,orderTime FROM order_total_join_order_dim_table) AS tmp\",\"sourcePrimaryKey\":\"orderId\",\"waterMark\":\"orderTime|0\",\"joinSql\":\"create table `order_total_join_order_dim_table` (select s.*,order_dim_table.* from order_total s  left join order_dim_table FOR SYSTEM_TIME AS OF s.proctime AS order_dim_table ON s.orderId = order_dim_table.orderDimId)\",\"runMode\":\"01\",\"testSourcedata\":[{\"orderTime\":\"2020-11-08 18:34:12.123\",\"orderAmount\":\"11111\",\"orderId\":\"001\",\"orderStatues\":\"1\"},{\"orderTime\":\"2020-11-08 18:34:12.124\",\"orderAmount\":\"22222\",\"orderId\":\"002\",\"orderStatues\":\"0\"},{\"orderTime\":\"2020-11-08 18:34:12.125\",\"orderAmount\":\"33333\",\"orderId\":\"003\",\"orderStatues\":\"1\"}]}";
        System.out.println(str);
        JSONObject jsonObject = JSON.parseObject(str);
        jsonObject.entrySet().stream().forEach(x -> System.out.println(x));
        byte[] bytes = str.getBytes();
        //Base64 加密
        String encoded = Base64.getEncoder().encodeToString(bytes);
        System.out.println("Base 64 加密后：" + encoded);
        byte[] decode = Base64.getDecoder().decode(encoded);
        System.out.println(new String(decode));
    }

}
