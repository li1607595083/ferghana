package com.skyon.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Set;

public class FunMapValueAddTypeAadFieldName extends ProcessFunction<Row, Tuple2<String, String>> {

    private String[] fieldNames;
    private LinkedHashMap fieldTypeHashMap;
    private String primary_key;

    public FunMapValueAddTypeAadFieldName(String[] fieldNames, LinkedHashMap fieldTypeHashMap, String primary_key) {
        this.fieldNames = fieldNames;
        this.fieldTypeHashMap = fieldTypeHashMap;
        this.primary_key = primary_key;
    }

    /**
     * Create an instance
     * @param fieldNames
     * @param fieldTypeHashMap
     * @param primary_key
     * @return
     */
    public static FunMapValueAddTypeAadFieldName of(String[] fieldNames, LinkedHashMap fieldTypeHashMap, String primary_key) {
        return new FunMapValueAddTypeAadFieldName(fieldNames, fieldTypeHashMap, primary_key);
    }


    @Override
    public void processElement(Row row, Context context, Collector<Tuple2<String, String>> collector) throws Exception {
        HashMap<String, String> hashMap = new HashMap<>();
        String[] sp = row.toString().split(",");
        String key = "";
        int cn = 0;
        String field_values;
        for (String fieldName : fieldNames) {
            field_values = sp[cn];
            if (primary_key.equals(fieldName)){
                key = fieldName + ":"  + field_values.split("\t")[0];
            } else {
                //字段名&字段类型&字段值
                hashMap.put(fieldName, fieldName+"&"+ fieldTypeHashMap.get(fieldName) + "&" + field_values);
            }
            cn += 1;
        }
        collector.collect(Tuple2.of(key, JSONObject.toJSON(hashMap).toString()));
    }
}
