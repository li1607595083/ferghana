package com.skyon.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MathUtils;
import scala.collection.mutable.HashMap$;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

public class FunMapValueAddFieldName extends ProcessFunction<Row, Tuple3<String, String, Long>> {

    SimpleDateFormat sdf;
    private boolean initData = true;
    private String[] fieldNames;
    private String key;
    private ArrayList<Integer> keySet;
    HashMap<String, String> hashMap = new HashMap<>();
    private String fieldTime;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        key = keySet.get(getRuntimeContext().getIndexOfThisSubtask()) + "";
    }

    public FunMapValueAddFieldName(String[] fieldNames, int parallelism, String fieldTime) {
        this.fieldTime = fieldTime;
        this.sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        int counts = parallelism;
        this.fieldNames = fieldNames;
        this.keySet  = new ArrayList<>();
        int initIndexChannel = 0;
        int initKey = 0;
        while (counts > 0) {
            int groupId = MathUtils.murmurHash((initKey + "").hashCode()) % 128;
            int value = groupId * parallelism / 128;
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
    public void processElement(Row row, Context context, Collector<Tuple3<String,String, Long>> collector) throws Exception {
        String[] sp = row.toString().split(",");
        int cn = 0;
        Tuple3<String, String, Long> tuple3 = new Tuple3<>();
        String field_values;
        for (String fieldName : fieldNames) {
            field_values = sp[cn];
            hashMap.put(fieldName, field_values);
            cn++;
            if (fieldName.equals(fieldTime)){
                tuple3.f2 = sdf.parse(field_values.replaceFirst("T", " ")).getTime() + 8 * 60 * 60 * 1000;
            }
        }
        tuple3.f0 = key;
        tuple3.f1 = JSONObject.toJSON(hashMap).toString();
        collector.collect(tuple3);
        if (initData){
            for (Integer integer : keySet) {
                collector.collect(Tuple3.of(integer + "", null, 0L));
            }
            initData = false;
        }
        hashMap.clear();
    }
}
