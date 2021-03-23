package com.skyon.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import static org.apache.flink.api.common.typeinfo.Types.MAP;
import static org.apache.flink.api.common.typeinfo.Types.STRING;

public class FunKeyedProValCon extends KeyedProcessFunction<String, Tuple2<String, String>, String> {

    private transient ValueState<Map<String, String>> vm_state;
    private transient ValueState<Long> timer_state;
    private transient MapState<String, Integer>  field_name;
    private final  int field_counts;
    private HashSet<String> fieldSet;

    private FunKeyedProValCon(int field_counts, HashSet<String> fieldSet) {
        this.field_counts  = field_counts;
        this.fieldSet = fieldSet;
    }


    /**
     * Create an instance
     * @param field_counts
     * @return
     */
    public static FunKeyedProValCon of(int field_counts, HashSet<String> fieldSet) {
        return new FunKeyedProValCon(field_counts, fieldSet);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor<Map<String, String>> value_map_desc = new ValueStateDescriptor<Map<String, String>>(
                "value-keyed-state",
                MAP(STRING, STRING)
        );
        ValueStateDescriptor<Long> registerTimer = new ValueStateDescriptor<>(
                "register-timer",
                Types.LONG()
        );

        MapStateDescriptor<String, Integer> field_name_state_desc = new MapStateDescriptor<>(
                "field-name-state",
                String.class,
                Integer.class
        );

        vm_state = getRuntimeContext().getState(value_map_desc);
        timer_state = getRuntimeContext().getState(registerTimer);
        field_name = getRuntimeContext().getMapState(field_name_state_desc);
    }

    @Override
    public void processElement(Tuple2<String, String> value, Context ctx, Collector<String> out) throws Exception {
            String ky = value.f0;
            String[] split = ky.split(":", 2);
            String vl = value.f1;
            Map<String, String> map = vm_state.value();
            if (map == null){
                map = new HashMap<>();
                map.put(split[0], split[1]);
                Iterator<String> iterator = fieldSet.iterator();
                while (iterator.hasNext()){
                    field_name.put(iterator.next(), 1);
                }
            }
            HashMap hashMap = JSONObject.parseObject(vl, HashMap.class);
            Iterator<Map.Entry<String, String>> iterator = hashMap.entrySet().iterator();
            while (iterator.hasNext()){
                Map.Entry<String, String> next = iterator.next();
                String kk = next.getKey();
                String vv = next.getValue();
                map.put(kk, vv);
                field_name.remove(kk);
            }
            if (map.size() >= field_counts){
                String result = JSONObject.toJSON(map).toString();
                vm_state.clear();
                out.collect(result);
                if (timer_state.value() != null){
                    ctx.timerService().deleteProcessingTimeTimer(timer_state.value());
                }
            } else if (field_name.isEmpty()){
                vm_state.update(map);
                long processingTime = ctx.timerService().currentProcessingTime() + 10 * 1000;
                if (timer_state.value() != null){
                    ctx.timerService().deleteProcessingTimeTimer(timer_state.value());
                }
                ctx.timerService().registerProcessingTimeTimer(processingTime);
                timer_state.update(processingTime);
            } else {
                vm_state.update(map);
            }

    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        if (!vm_state.value().isEmpty()){
            Map<String, String> value = vm_state.value();
            String result = JSONObject.toJSON(value).toString();
            out.collect(result);
        }
        vm_state.clear();
        timer_state.clear();
    }
}
