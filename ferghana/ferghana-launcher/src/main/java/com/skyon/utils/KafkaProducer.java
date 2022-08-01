package com.skyon.utils;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @description: KafkaProducer
 * @author: ......
 * @create: 2022/3/3116:18
 */
public class KafkaProducer {
    public static void main(String[] args) throws InterruptedException {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "192.168.4.95:9092");
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("acks", "all");
        org.apache.kafka.clients.producer.KafkaProducer producer  = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(props);
        int counts = 0;
        int stma = 0;
        while (true){
            Map<String,String> map = new HashMap<String,String>();
            map.put("TRAN_NO", UtilData.getTranNo());
            map.put("CUST_NO",UtilData.getCustNo());
            map.put("CUST_NAME",UtilData.custName());
            map.put("STATUS","00");
            map.put("BSN_CODE","01");
            map.put("ACCT_NO",UtilData.getAcctNo());
            map.put("MOBILE_NO",UtilData.getTel());
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            map.put("TRAN_TIME",sdf.format(new Date()));
            String s = JSON.toJSONString(map);
            producer.send(new ProducerRecord<String, String>("EP_OPENACCT_FLOW_TOPIC", s));
            counts ++;
            stma++;
            if (counts >= 100){
                counts = 0;
                System.out.println("当前发送条数:\t" + stma);
//                Thread.sleep(Integer.parseInt(args[0]));
                Thread.sleep(Integer.parseInt("50"));
            }
        }

    }

}