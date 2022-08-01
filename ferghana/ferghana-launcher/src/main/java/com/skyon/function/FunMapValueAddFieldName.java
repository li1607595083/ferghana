package com.skyon.function;

import com.alibaba.fastjson.JSONObject;
import com.skyon.utils.TimeFieldUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.TimeUtils;
import scala.collection.mutable.HashMap$;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

public class FunMapValueAddFieldName extends ProcessFunction<Row, Tuple3<Integer, String,Long>> {

    private transient SimpleDateFormat sdf;
    private transient int indexOfThisSubtask;
    private boolean initData = true;
    private String[] fieldNames;
    private ArrayList<Integer> keySet;
    private String fieldTime;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    }

    public FunMapValueAddFieldName(String[] fieldNames, int parallelism, String fieldTime) {
        this.fieldTime = fieldTime;
        int counts = parallelism;
        this.fieldNames = fieldNames;
        this.keySet  = new ArrayList<>();
        int initIndexChannel = 0;
        int initKey = 0;
        while (counts > 0) {
            int value = KeyGroupRangeAssignment.assignKeyToParallelOperator(initKey, 128, parallelism);
            if (value == initIndexChannel){
               keySet.add(initIndexChannel,initKey);
                initIndexChannel += 1;
                counts -= 1;
            }
            initKey += 1;
        }
    }

    /**
     * Create an instance
     * @param fieldNames
     * @return
     */
    public static FunMapValueAddFieldName of(String[] fieldNames, int parallelism,String fieldTime) {
        return new FunMapValueAddFieldName(fieldNames, parallelism, fieldTime);
    }


    @Override
    public void processElement(Row row, Context context, Collector<Tuple3<Integer, String,Long>> collector) throws Exception {
        HashMap<String, String> hashMap = new HashMap<>();
        int cn = 0;
        Tuple3<Integer, String, Long> tuple3 = new Tuple3<>();
        String field_values;
        for (String fieldName : fieldNames) {
            Object field = row.getField(cn);
            field_values = field == null ? null : field.toString();
            if (fieldName.equals(fieldTime)){
                tuple3.f2 = sdf.parse(TimeFieldUtils.dealTimeField(field_values).replaceFirst("T", " ")).getTime()+ 8 * 60 * 60 * 1000;
            }
            hashMap.put(fieldName, field_values);
            cn++;
        }
        tuple3.f0 = keySet.get(indexOfThisSubtask);
        tuple3.f1 = JSONObject.toJSON(hashMap).toString();
        collector.collect(tuple3);
        if (initData){
            for (Integer integer : keySet) {
                collector.collect(Tuple3.of(integer, null, 0L));
            }
            initData = false;
        }
        hashMap.clear();
    }

}
