package com.skyon.sink;

import com.alibaba.fastjson.JSON;
import com.skyon.bean.ParameterName;
import com.skyon.bean.RunMode;
import com.skyon.bean.SinkType;
import com.skyon.type.TypeTrans;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import java.util.*;

public class OracelAndMysqlSink {


    public static  SinkFunction<String> inserOrUpdateJdbc(Properties properties, String insertsql, Set<Map.Entry<String, String>> entries,String pk, String jdbcType, int bathSize ,String pkType){
        Iterator<Map.Entry<String, String>> entryIterator = entries.iterator();
        ArrayList<String> arr = new ArrayList<>();
        while (entryIterator.hasNext()){
            Map.Entry<String, String> next = entryIterator.next();
            String key = next.getKey();
            String value = next.getValue();
            arr.add(key + "\t" + value);
        }
        if (jdbcType.equals(SinkType.SINK_JDBC_MYSQL)) {
            return org.apache.flink.connector.jdbc.JdbcSink.sink(
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
                    JdbcExecutionOptions.builder().withBatchSize(bathSize).build()
                    ,
                    new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                            .withUrl(properties.getProperty(properties.getProperty(ParameterName.RUM_MODE).equals(RunMode.START_MODE) ? ParameterName.JDBC_URL : ParameterName.TEST_MYSQL_DIM_URL))
                            .withDriverName(properties.getProperty(properties.getProperty(ParameterName.RUM_MODE).equals(RunMode.START_MODE) ? ParameterName.JDBC_DRIVER : ParameterName.TEST_DRIVER))
                            .withUsername(properties.getProperty(properties.getProperty(ParameterName.RUM_MODE).equals(RunMode.START_MODE) ? ParameterName.JDBC_USER_NAME : ParameterName.TEST_USER_NAME))
                            .withPassword(properties.getProperty(properties.getProperty(ParameterName.RUM_MODE).equals(RunMode.START_MODE) ? ParameterName.JDBC_USER_PWD : ParameterName.TEST_PASSWORD))
                            .build());
        }else {
                return org.apache.flink.connector.jdbc.JdbcSink.sink(
                        insertsql,
                        (ps, x) -> {
                            HashMap hashMap = JSON.parseObject(x, HashMap.class);
                            ps = TypeTrans.oraclePs(ps, TypeTrans.getTranKey(pkType), 1, hashMap.get(pk).toString(), 0);
                            int columnNum = arr.size();
                            int index1 = 2;
                            int index2;
                            for (String s : arr) {
                                String[] split = s.split("\t", 2);
                                String columName = split[0];
                                String columType = TypeTrans.getTranKey(split[1]);
                                if (!pk.equals(columName)){
                                    index2 = index1 + columnNum;
                                } else {
                                    index2 = 0;
                                }
                                ps = TypeTrans.oraclePs(ps, columType, index1, hashMap.get(columName).toString(), index2);
                                index1 += 1;
                            }
                        }
                        ,
                        JdbcExecutionOptions.builder().withBatchSize(bathSize).build()
                        ,
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl(properties.getProperty(properties.getProperty(ParameterName.RUM_MODE).equals(RunMode.START_MODE) ? ParameterName.JDBC_URL : ParameterName.TEST_ORACLE_DIM_URL))
                                .withDriverName(properties.getProperty(properties.getProperty(ParameterName.RUM_MODE).equals(RunMode.START_MODE) ? ParameterName.JDBC_DRIVER : ParameterName.TEST_ORACLE_DRIVER))
                                .withUsername(properties.getProperty(properties.getProperty(ParameterName.RUM_MODE).equals(RunMode.START_MODE) ? ParameterName.JDBC_USER_NAME : ParameterName.JDBC_USER_NAME))
                                .withPassword(properties.getProperty(properties.getProperty(ParameterName.RUM_MODE).equals(RunMode.START_MODE) ? ParameterName.JDBC_USER_PWD : ParameterName.JDBC_USER_PWD))
                                .build());
            }
        }

}
