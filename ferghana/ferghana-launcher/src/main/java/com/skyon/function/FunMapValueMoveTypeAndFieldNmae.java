package com.skyon.function;

import com.alibaba.fastjson.JSONObject;
import com.skyon.bean.ParameterValue;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.types.Row;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

public class FunMapValueMoveTypeAndFieldNmae extends RichMapFunction<Row, String> {
    private  double computer_duration = 0.0;
    private  String proctimeTime = "proctime";
    private  String[] fieldNames;
    private transient SimpleDateFormat simpleDateFormat;
    public FunMapValueMoveTypeAndFieldNmae() {
    }

    public FunMapValueMoveTypeAndFieldNmae(String[] fieldNames) {
        this.fieldNames = fieldNames;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        super.open(parameters);
        getRuntimeContext()
                .getMetricGroup()
                .addGroup("flink_customer_metric")
                .gauge("computer_duration", new Gauge<Double>() {
                    @Override
                    public Double getValue() {
                        return computer_duration;
                    }
                });
    }

    /**
     * Create an instance
     * @param fieldNames
     * @return
     */
    public static FunMapValueMoveTypeAndFieldNmae of(String[] fieldNames) {
        return new FunMapValueMoveTypeAndFieldNmae(fieldNames);
    }


    @Override
    public String map(Row value) throws ParseException {
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
            if (fieldName.equals(proctimeTime)){
                computer_duration = System.currentTimeMillis() - simpleDateFormat.parse(field_values.replaceFirst("T", " ")).getTime();
            }
        }
        return JSONObject.toJSON(hashMap).toString();
    }

}
