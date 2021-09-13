package com.skyon.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import static org.apache.flink.api.common.typeinfo.Types.MAP;
import static org.apache.flink.api.common.typeinfo.Types.STRING;

public class FunKeyedProValCon extends KeyedProcessFunction<String, Tuple2<String, String>, String> {

    private String procetimeField = "proctime";
    private transient ValueState<Map<String, String>> valueKeyedState;
    private transient ValueState<Long> timerState;
    private transient ValueState<Integer> sqlCountsStates;
    private String timeField;
    private FastDateFormat fastDateFormat;
    private int sqlCounts;

    private FunKeyedProValCon( int sqlCounts, String timeField) {
        this.timeField = timeField;
        this.sqlCounts = sqlCounts;
    }


    /**
     * Create an instance
     * @param sqlCounts
     * @return
     */
    public static FunKeyedProValCon of(int sqlCounts, String timeField) {
        return new FunKeyedProValCon(sqlCounts,timeField);
    }

    @Override
    public void open(Configuration parameters) throws Exception {

        super.open(parameters);
        ValueStateDescriptor<Map<String, String>> valueMapDesc = new ValueStateDescriptor<Map<String, String>>(
                "value-keyed-state",
                MAP(STRING, STRING)
        );
        ValueStateDescriptor<Long> registerTimerDesc = new ValueStateDescriptor<>(
                "register-timer",
                Types.LONG()
        );

        ValueStateDescriptor<Integer> sqlCountDesc = new ValueStateDescriptor<>(
                "sql-counts",
                Types.INT()
        );
        sqlCountsStates = getRuntimeContext().getState(sqlCountDesc);
        valueKeyedState = getRuntimeContext().getState(valueMapDesc);
        timerState = getRuntimeContext().getState(registerTimerDesc);
        fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss.SSS");
    }

    @Override
    public void processElement(Tuple2<String, String> value, Context ctx, Collector<String> out) throws Exception {
            String ky = value.f0;
            String[] split = ky.split(":", 2);
            String vl = value.f1;
            Map<String, String> map = valueKeyedState.value();
            if (map == null){
                map = new HashMap<>();
                map.put(split[0], split[1]);
            }
            if (sqlCountsStates.value() == null){
                sqlCountsStates.update(0);
            }
            HashMap hashMap = JSONObject.parseObject(vl, HashMap.class);
            Iterator<Map.Entry<String, String>> iterator = hashMap.entrySet().iterator();
            while (iterator.hasNext()){
                Map.Entry<String, String> next = iterator.next();
                String kk = next.getKey();
                String vv = next.getValue();
                map.put(kk, vv);
                if (!timeField.equals(procetimeField) && timerState.value() == null && kk.equals(timeField)){
                    long time = fastDateFormat.parse(vv.split("&")[2].replaceAll("T", " ")).getTime();
                    timerState.update(time);
                    ctx.timerService().registerEventTimeTimer(time);
                }

            }
            if (sqlCountsStates.value() + 1 >= sqlCounts){
                String result = JSONObject.toJSON(map).toString();
                valueKeyedState.clear();
                sqlCountsStates.clear();
                out.collect(result);
                if (timerState.value() != null){
                    ctx.timerService().deleteEventTimeTimer(timerState.value());
                    timerState.clear();
                }
            } else {
                valueKeyedState.update(map);
                sqlCountsStates.update(sqlCountsStates.value() + 1);
            }

    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        if (valueKeyedState.value() != null){
            if (!valueKeyedState.value().isEmpty()){
                Map<String, String> value = valueKeyedState.value();
                String result = JSONObject.toJSON(value).toString();
                out.collect(result);
            }
                valueKeyedState.clear();
                sqlCountsStates.clear();
                timerState.clear();
        } else {
            timerState.clear();
        }
    }
}
