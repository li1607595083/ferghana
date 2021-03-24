package com.skyon.function;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

import java.util.*;

public class FunMapJsonForPars extends RichMapFunction<String, Row> {

    private LinkedHashMap<String, String> singleFieldTypeHashMap;
    private ArrayList<String> arr;
    private Integer fieldCounts;

    public FunMapJsonForPars(){}

    public FunMapJsonForPars(LinkedHashMap<String, String> singleFieldTypeHashMap){
        this.singleFieldTypeHashMap = singleFieldTypeHashMap;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        arr = new ArrayList<>();
        fieldCounts = singleFieldTypeHashMap.size();
        Iterator<Map.Entry<String, String>> iterator = singleFieldTypeHashMap.entrySet().iterator();
        while (iterator.hasNext()){
            Map.Entry<String, String> next = iterator.next();
            String key = next.getKey();
            arr.add(key);
        }
    }

    @Override
    public Row map(String value) throws Exception {
        HashMap hashMap = JSON.parseObject(value, HashMap.class);
        Row row = new Row(fieldCounts);
        int count = 0;
        for (String s : arr) {
            row.setField(count, hashMap.get(s));
            count++;
        }
        return row;
    }
}
