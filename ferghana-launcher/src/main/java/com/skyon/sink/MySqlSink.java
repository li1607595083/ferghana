package com.skyon.sink;

import com.alibaba.fastjson.JSON;
import com.skyon.type.TypeTrans;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import java.util.*;

public class MySqlSink {

    public static  SinkFunction<String> inserOrUpdateMySQL(Properties properties, String insertsql, Set<Map.Entry<String, String>> entries){
        Iterator<Map.Entry<String, String>> entryIterator = entries.iterator();
        ArrayList<String> arr = new ArrayList<>();
        while (entryIterator.hasNext()){
            Map.Entry<String, String> next = entryIterator.next();
            String key = next.getKey();
            String value = next.getValue();
            arr.add(key + "\t" + value);
        }
        return JdbcSink.sink(
                insertsql,
                (ps, x) -> {
                    HashMap hashMap = JSON.parseObject(x, HashMap.class);
                    int index = 1;
                    int columnNum = arr.size();
                    for (String s : arr) {
                        String[] split = s.split("\t", 2);
                        String columName = split[0];
                        String columType = TypeTrans.getTranKey(split[1]);
                        ps = TypeTrans.mysqlPs(ps, columType, index, hashMap.get(columName).toString(), columnNum);
                        index += 1;
                    }
                }
                ,
                JdbcExecutionOptions.builder().withBatchSize(1).build()
//                JdbcExecutionOptions.builder().withBatchSize(5000).build()
                ,
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(properties.getProperty("jdbcURL"))
                        .withDriverName(properties.getProperty("jdbcDrive"))
                        .withUsername(properties.getProperty("jdbcUserName"))
                        .withPassword(properties.getProperty("jdbcUserPwd"))
                        .build());
    }

}
