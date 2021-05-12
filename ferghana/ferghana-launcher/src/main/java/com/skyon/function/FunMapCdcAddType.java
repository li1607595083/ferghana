package com.skyon.function;


import com.alibaba.fastjson.JSONObject;
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

public class FunMapCdcAddType extends RichMapFunction<Tuple2<Boolean, Row>, Tuple2<Long, String>> {

    private ArrayList<String> fieldName;
    private List<String> filterType;
    private String waterMarkField;
    private FastDateFormat fastDateFormat;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        String[] split = parameterTool.get("CDC_TYPE").split(",");
        filterType = Arrays.asList(split);
        fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss.SSS");
    }

    public FunMapCdcAddType(ArrayList<String> fieldName, String waterMarkField){
        this.fieldName = fieldName;
        this.waterMarkField = waterMarkField;
    }

    public static  FunMapCdcAddType of(ArrayList<String> fieldName, String waterMarkField){
        return new FunMapCdcAddType(fieldName, waterMarkField);
    }

    @Override
    public Tuple2<Long, String> map(Tuple2<Boolean, Row> tp2) throws Exception {
        Long timeStamp = null;
        Row value = tp2.f1;
        String op_type = value.getKind().shortString();
        if (!op_type.equals("-U")){
            op_type = op_type.substring(1);
            if (filterType.contains(op_type)){
                String vlToStr = value.toString();
                HashMap<String, String> fieldNameAndFieldValue = new HashMap<>();
                int index = 0;
                for (String s : vlToStr.split(",")) {
                    fieldNameAndFieldValue.put(fieldName.get(index), s);
                    if (fieldName.get(index).equals(waterMarkField)){
                        if (!s.equals("null")){
                            timeStamp = fastDateFormat.parse(s.replaceFirst("T", " ")).getTime() + 8*60*60*1000;
                            fieldNameAndFieldValue.put(fieldName.get(index),  fastDateFormat.format(timeStamp));
                        }
                    }
                    index++;
                }
                fieldNameAndFieldValue.put("CDC_OP", op_type);
                return Tuple2.of(timeStamp,JSONObject.toJSON(fieldNameAndFieldValue).toString());
            } else {
                return null;
            }
        } else {
            return null;
        }

    }

}
