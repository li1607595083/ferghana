package com.skyon.function;

import com.alibaba.fastjson.JSONObject;
import kafka.log.Log;
import oracle.ucp.jdbc.oracle.RACInstance;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.scala.OutputTag;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Tuple;

import java.util.HashMap;
import java.util.LinkedHashMap;

public class FunMapGiveSchema extends ProcessFunction<Row, Tuple2<String, String>> {

    private String[] fieldNames;
    private String primary_key;

    public FunMapGiveSchema() {
    }

    @Override
    public void processElement(Row row, Context context, Collector<Tuple2<String, String>> collector) throws Exception {
        HashMap<String, String> hashMap = new HashMap<>();
        String[] sp = row.toString().split(",");
        String key = "";
        int cn = 0;
        String field_values = "";
        for (String fieldName : fieldNames) {
            field_values = sp[cn];
            if (primary_key.equals(fieldName)){
                key = fieldName + ":"  + field_values;
            } else {
                hashMap.put(fieldName, field_values);
            }
            cn += 1;
        }
        collector.collect(Tuple2.of(key, JSONObject.toJSON(hashMap).toString()));
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    public FunMapGiveSchema(String[] fieldNames, String primary_key) {
        this.fieldNames = fieldNames;
        this.primary_key = primary_key;
    }

    /**
     * Create an instance
     * @param fieldNames
     * @param primary_key
     * @return
     */
    public static FunMapGiveSchema of(String[] fieldNames, String primary_key) {
        return new FunMapGiveSchema(fieldNames, primary_key);
    }

}
