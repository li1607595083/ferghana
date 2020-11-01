package com.skyon.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;

import java.util.HashMap;

public class FunMapGiveSchema implements MapFunction<Row, String> {

    private String[] fieldNames;
    private HashMap fieldTypeHashMap;
    private String primary_key;

    public FunMapGiveSchema() {
    }

    public FunMapGiveSchema(String[] fieldNames, HashMap fieldTypeHashMap, String primary_key) {
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
    public static FunMapGiveSchema of(String[] fieldNames, HashMap fieldTypeHashMap, String primary_key) {
        return new FunMapGiveSchema(fieldNames, fieldTypeHashMap, primary_key);
    }

    @Override
    public String map(Row value) throws Exception {
        HashMap<String, String> hashMap = new HashMap<>();
        String[] sp = value.toString().split(",");
        String key = "";
        int cn = 0;
        String field_values = "";
        for (String fieldName : fieldNames) {
            field_values = sp[cn];
            if ("TIMESTAMP(3)".equals(fieldTypeHashMap.get(fieldName))){
                field_values = field_values + "Z";
            }
            if (primary_key.equals(fieldName)){
                key = fieldName + ":"  + field_values;
            } else {
                hashMap.put(fieldName, field_values);
            }
            cn += 1;
        }
        String vl = JSONObject.toJSON(hashMap).toString();
        return key + "\t" + vl;
    }

}
