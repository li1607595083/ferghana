package com.skyon.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.apache.flink.api.common.typeinfo.Types.MAP;
import static org.apache.flink.api.common.typeinfo.Types.STRING;

public class FunKeyedProValCon extends KeyedProcessFunction<String, Tuple2<String, String>, String> {

    private transient ValueState<Map<String, String>> vm_state;
    private transient ValueState<Long> timer_state;
    private final  int field_counts;

    private FunKeyedProValCon(int field_counts) {
        this.field_counts  = field_counts;
    }


    /**
     * Create an instance
     * @param field_counts
     * @return
     */
    public static FunKeyedProValCon of(int field_counts) {
        return new FunKeyedProValCon(field_counts);
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
        vm_state = getRuntimeContext().getState(value_map_desc);
        timer_state = getRuntimeContext().getState(registerTimer);
    }

    @Override
    public void processElement(Tuple2<String, String> value, Context ctx, Collector<String> out) throws Exception {
        String ky = value.f0;
        String vl = value.f1;
        Map<String, String> map = vm_state.value();
        String[] split = ky.split(":", 2);
        if (map == null){
            map = new HashMap<>();
            map.put(split[0], split[1]);
        }
        HashMap hashMap = JSONObject.parseObject(vl, HashMap.class);
        Iterator<Map.Entry<String, String>> iterator = hashMap.entrySet().iterator();
        while (iterator.hasNext()){
            Map.Entry<String, String> next = iterator.next();
            String kk = next.getKey();
            String vv = next.getValue();
            map.put(kk, vv);
        }
        if (map.size() != field_counts){
            vm_state.update(map);
            long registerTime = ctx.timerService().currentProcessingTime() + 5000;
            Long last_timer = timer_state.value();
            if (last_timer != null){
                ctx.timerService().deleteProcessingTimeTimer(last_timer);
            }
            timer_state.update(registerTime);
            ctx.timerService().registerProcessingTimeTimer(registerTime);
        } else if (map.size() == field_counts){
            String result = JSONObject.toJSON(map).toString();
            vm_state.clear();
            out.collect(result);
            Long aLong = timer_state.value();
            if (aLong != null){
                ctx.timerService().deleteProcessingTimeTimer(aLong);
            }
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            Map<String, String> value = vm_state.value();
            String result = JSONObject.toJSON(value).toString();
            vm_state.clear();
            timer_state.clear();
            out.collect(result);
    }
}
