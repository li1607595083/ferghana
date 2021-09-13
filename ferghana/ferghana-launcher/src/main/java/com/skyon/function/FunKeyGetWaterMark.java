package com.skyon.function;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @DESCRIPTION:
 * @NAME: TR
 * @DATE: 2021/8/12
 */
public class FunKeyGetWaterMark  extends KeyedProcessFunction<String, Tuple3<String, String,Long>, Tuple2<Long, String>> {

    public TimerService timerService;
    private long lastRegisterProcessTime;

    private FunKeyGetWaterMark(){}


    public static FunKeyGetWaterMark of(){
        return new FunKeyGetWaterMark();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void processElement(Tuple3<String, String, Long> value, Context ctx, Collector<Tuple2<Long, String>> out) throws Exception {
        if (timerService == null){
            timerService = ctx.timerService();
        }
        timerService.deleteEventTimeTimer(lastRegisterProcessTime);
        lastRegisterProcessTime = System.currentTimeMillis() + 1000;
        timerService.registerProcessingTimeTimer(lastRegisterProcessTime);
        out.collect(Tuple2.of(value.f2, value.f1));
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<Long, String>> out) throws Exception {
        out.collect(Tuple2.of(timerService.currentWatermark(), null));
        lastRegisterProcessTime = System.currentTimeMillis() + 1000;
        timerService.registerProcessingTimeTimer(lastRegisterProcessTime);
    }
}
