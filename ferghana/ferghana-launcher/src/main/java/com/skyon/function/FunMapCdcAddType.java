package com.skyon.function;


import com.alibaba.fastjson.JSONObject;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

import java.text.SimpleDateFormat;
import java.util.*;

public class FunMapCdcAddType extends RichMapFunction<Tuple2<Boolean, Row>, String> {

    private List<String> fieldName;
    private List<String> filterType;

    public FunMapCdcAddType(String[] fieldName, String cdcType){
        this.fieldName = Arrays.asList(fieldName);
        this.filterType = Arrays.asList(cdcType.split(","));
    }

    public static  FunMapCdcAddType of(String[] fieldName, String cdcTye){
        return new FunMapCdcAddType(fieldName, cdcTye);
    }

    @Override
    public String map(Tuple2<Boolean, Row> tp2) throws Exception {
        HashMap<String, String> fieldNameAndFieldValue = new HashMap<>();
        int index = 0;
        Row value = tp2.f1;
        String op_type = value.getKind().shortString();
        if (!op_type.equals("-U")){
            op_type = op_type.substring(1);
            if (filterType.contains(op_type)){
                String vl = value.toString();
                for (String s : vl.split(",")) {
                    fieldNameAndFieldValue.put(fieldName.get(index), s);
                    index++;
                }
                fieldNameAndFieldValue.put("CDC_OP", op_type);
            }
            return JSONObject.toJSON(fieldNameAndFieldValue).toString();
        } else {
            return null;
        }
    }

}
