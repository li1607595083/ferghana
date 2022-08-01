package com.skyon.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

public class FunMapValueMoveTypeAndFieldNameTest extends RichMapFunction<Row, String> {
    private String[] fieldNames;
    public FunMapValueMoveTypeAndFieldNameTest() {
    }

    public FunMapValueMoveTypeAndFieldNameTest(String[] fieldNames) {
        this.fieldNames = fieldNames;
    }

    /**
     * Create an instance
     * @param fieldNames
     * @return
     */
    public static FunMapValueMoveTypeAndFieldNameTest of(String[] fieldNames) {
        return new FunMapValueMoveTypeAndFieldNameTest(fieldNames);
    }


    @Override
    public String map(Row value) {
        LinkedHashMap<String, String> hashMap = new LinkedHashMap<>();
        String[] sp = value.toString().split(",");
        int cn = 0;
        String field_values = "";
        for (String fieldName : fieldNames) {
            field_values = sp[cn];
            //字段名&字段类型&字段值
            String[] sp_ftv = field_values.split("&", -1);
            if (sp_ftv.length == 3){
                field_values = sp_ftv[2];
                if (field_values.equals("null")){
                    field_values = "0";
                }
            }
            hashMap.put(fieldName, field_values);
            cn += 1;
        }
        return JSONObject.toJSON(hashMap).toString();
    }

}
