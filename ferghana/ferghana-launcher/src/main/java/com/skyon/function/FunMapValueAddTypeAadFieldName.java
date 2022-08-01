package com.skyon.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Set;

public class FunMapValueAddTypeAadFieldName extends ProcessFunction<Row, Tuple2<Long,String>> {

    private String[] fieldNames;
    private LinkedHashMap fieldTypeHashMap;

    public FunMapValueAddTypeAadFieldName(String[] fieldNames, LinkedHashMap fieldTypeHashMap) {
        this.fieldNames = fieldNames;
        this.fieldTypeHashMap = fieldTypeHashMap;
    }

    /**
     * Create an instance
     * @param fieldNames
     * @param fieldTypeHashMap
     * @return
     */
    public static FunMapValueAddTypeAadFieldName of(String[] fieldNames, LinkedHashMap fieldTypeHashMap) {
        return new FunMapValueAddTypeAadFieldName(fieldNames, fieldTypeHashMap);
    }


    @Override
    public void processElement(Row row, Context context, Collector<Tuple2<Long,String>> collector) throws Exception {
        HashMap<String, String> hashMap = new HashMap<>();
        int cn = 0;
        String field_values;
        for (String fieldName : fieldNames) {
            Object field = row.getField(cn);
            field_values = field == null ? null : field.toString();
            hashMap.put(fieldName, fieldName+"&"+ fieldTypeHashMap.get(fieldName) + "&" + field_values);
            cn++;
        }
        collector.collect(Tuple2.of(0L,JSONObject.toJSON(hashMap).toString()));
    }
}
