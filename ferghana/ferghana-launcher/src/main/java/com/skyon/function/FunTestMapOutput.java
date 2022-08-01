package com.skyon.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @DESCRIPTION:
 * @NAME: TR
 * @DATE: 2021/6/29
 */
public class FunTestMapOutput implements MapFunction<String, String> {
    @Override
    public String map(String value) throws Exception {
        HashMap<String, String> hashMap = JSON.parseObject(value, HashMap.class);
        Iterator<Map.Entry<String, String>> iterator = hashMap.entrySet().iterator();
        while (iterator.hasNext()){
            Map.Entry<String, String> next = iterator.next();
            String ky = next.getKey();
            String vl = next.getValue();
            String[] split = vl.split("&", -1);
            if (split.length == 3){
                vl = split[2];
                if (vl.equals("null")){
                    vl = "0";
                }
            }
            hashMap.put(ky, vl);
        }
        return JSONObject.toJSON(hashMap).toString();
    }
}

