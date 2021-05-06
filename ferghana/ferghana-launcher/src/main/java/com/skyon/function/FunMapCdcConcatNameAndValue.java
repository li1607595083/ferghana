package com.skyon.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.List;

public class FunMapCdcConcatNameAndValue implements MapFunction<Row, String> {

    List<String> arrFieldName;

    private FunMapCdcConcatNameAndValue(List<String> arrFieldName){
        this.arrFieldName = arrFieldName;
    }

    public static FunMapCdcConcatNameAndValue of(List<String> arrFieldName){
        return new FunMapCdcConcatNameAndValue(arrFieldName);
    }

    @Override
    public String map(Row value) throws Exception {
        int counts = 0;
        HashMap<String, String> mapNameAndValue = new HashMap<>();
        for (String s : value.toString().split(",")) {
            mapNameAndValue.put(arrFieldName.get(counts), s);
            counts +=1;
        }
        return JSONObject.toJSON(mapNameAndValue).toString();
    }
}
