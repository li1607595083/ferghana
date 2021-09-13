package com.skyon.function;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

import java.sql.Time;
import java.sql.Timestamp;
import java.util.*;

public class FunMapJsonForPars extends RichMapFunction<String, Row> {

    private LinkedHashMap<String, String> singleFieldTypeHashMap;
    private ArrayList<String> arr;
    private Integer fieldCounts;
    private ArrayList<String>  timeStampField;

    private FunMapJsonForPars(){}

    private FunMapJsonForPars(LinkedHashMap<String, String> singleFieldTypeHashMap){
        this.singleFieldTypeHashMap = singleFieldTypeHashMap;
    }

    public static FunMapJsonForPars of(LinkedHashMap<String, String> singleFieldTypeHashMap){
        return new FunMapJsonForPars(singleFieldTypeHashMap);
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        arr = new ArrayList<>();
        timeStampField = new ArrayList<>();
        fieldCounts = singleFieldTypeHashMap.size();
        Iterator<Map.Entry<String, String>> iterator = singleFieldTypeHashMap.entrySet().iterator();
        while (iterator.hasNext()){
            Map.Entry<String, String> next = iterator.next();
            String key = next.getKey();
            arr.add(key);
            if (next.getValue().startsWith("TIMESTAMP")){
                timeStampField.add(key);
            }
        }
    }

    @Override
    public Row map(String value) {
        HashMap<String,String> map = JSON.parseObject(value, HashMap.class);
        Row row = new Row(fieldCounts);
        int count = 0;
        for (String s : arr) {
            String fieldValue = map.get(s);
            if (timeStampField.contains(s)){
                Timestamp timestamp = Timestamp.valueOf(fieldValue.replaceFirst("T", " "));
                row.setField(count, timestamp);
            } else {
                row.setField(count, fieldValue);
            }
            count++;
        }
        return row;
    }
}
