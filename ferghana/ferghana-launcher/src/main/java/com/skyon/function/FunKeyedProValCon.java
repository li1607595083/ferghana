package com.skyon.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.time.FastDateFormat;
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
    private final  int field_counts;
    private String timeField;
    private FastDateFormat fastDateFormat;

    private FunKeyedProValCon(int field_counts, String timeField) {
        this.field_counts  = field_counts;
        this.timeField = timeField;
    }


    /**
     * Create an instance
     * @param field_counts
     * @return
     */
    public static FunKeyedProValCon of(int field_counts, String timeField) {
        return new FunKeyedProValCon(field_counts, timeField);
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
        fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss.SSS");
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
            }
            HashMap hashMap = JSONObject.parseObject(vl, HashMap.class);
            Iterator<Map.Entry<String, String>> iterator = hashMap.entrySet().iterator();
            while (iterator.hasNext()){
                Map.Entry<String, String> next = iterator.next();
                String kk = next.getKey();
                String vv = next.getValue();
                map.put(kk, vv);
                if (timer_state.value() == null && kk.equals(timeField)){
                    long time = fastDateFormat.parse(vv.split("&")[2].replaceAll("T", " ")).getTime();
                    timer_state.update(time);
                    ctx.timerService().registerEventTimeTimer(time);
                }

            }
            if (map.size() >= field_counts + 1){
                String result = JSONObject.toJSON(map).toString();
                vm_state.clear();
                out.collect(result);
                if (timer_state.value() != null){
                    ctx.timerService().deleteProcessingTimeTimer(timer_state.value());
                }
            }

    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        if (vm_state.value() != null){
            if (!vm_state.value().isEmpty()){
                Map<String, String> value = vm_state.value();
                String result = JSONObject.toJSON(value).toString();
                out.collect(result);
            }
            vm_state.clear();
            timer_state.clear();
        }
    }
}
