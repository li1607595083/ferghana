package com.skyon.function;

import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Map;

public class FunKeyByGetMinWaterMark extends KeyedProcessFunction<String,Tuple5<String, String, Long, String, String>, Tuple4<String, String, Long, Boolean>> {

    private transient MapState<String, Long> mapState;
    private int all_task_nums;
    private transient ValueState<Long> last_register_process_time;
    private transient ValueState<Long> sample_waterMark;
    private transient ListState<Tuple3<String, String, Long>> sample_waterMark_values;

    private FunKeyByGetMinWaterMark(){}


    public FunKeyByGetMinWaterMark(int numbers){
        this.all_task_nums = numbers;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        MapStateDescriptor<String, Long> all_taks_waterMark_desc = new MapStateDescriptor<>(
                "all_taks_waterMark",
                Types.STRING,
                Types.LONG
        );
        mapState = getRuntimeContext().getMapState(all_taks_waterMark_desc);

        ValueStateDescriptor<Long> last_register_process_time_desc = new ValueStateDescriptor<>(
                "last_register_process_time",
                Long.class
        );
        last_register_process_time = getRuntimeContext().getState(last_register_process_time_desc);

        ValueStateDescriptor<Long> sample_waterMark_desc = new ValueStateDescriptor<>(
                "sample_waterMark",
                Types.LONG
        );
        sample_waterMark = getRuntimeContext().getState(sample_waterMark_desc);

        ListStateDescriptor<Tuple3<String, String, Long>> sample_waterMark_values_des = new ListStateDescriptor<>(
                "sample_waterMark_values",
                Types.TUPLE(Types.STRING, Types.STRING, Types.LONG)
        );
        sample_waterMark_values = getRuntimeContext().getListState(sample_waterMark_values_des);
    }

    @Override
    public void processElement(Tuple5<String, String, Long, String, String> stringStringLongTuple3, Context context, Collector<Tuple4<String, String, Long, Boolean>> collector) throws Exception {
        int counts = 0;
        String task_uid = stringStringLongTuple3.f1;
        Long water_mark = stringStringLongTuple3.f2;
        if (sample_waterMark.value() == water_mark){
            sample_waterMark_values.add(Tuple3.of(stringStringLongTuple3.f3, stringStringLongTuple3.f4, water_mark));
        } else {
            sample_waterMark_values.clear();
            sample_waterMark_values.add(Tuple3.of(stringStringLongTuple3.f3, stringStringLongTuple3.f4, water_mark));
            sample_waterMark.update(water_mark);
        }
        Long min_water = water_mark;
        mapState.put(task_uid, water_mark);
        Iterator<Map.Entry<String, Long>> iterator = mapState.entries().iterator();
        while (iterator.hasNext()){
            counts = counts + 1;
            min_water = Math.min(min_water, iterator.next().getValue());
        }
        if (counts == all_task_nums){
            collector.collect(Tuple4.of("null", "null", min_water - 5 * 1000, false));
        }
        TimerService timerService = context.timerService();
        if (last_register_process_time.value() != null){
            timerService.deleteProcessingTimeTimer(last_register_process_time.value());
        }
        long processingTime = timerService.currentProcessingTime() + 10 * 1000;
        timerService.registerProcessingTimeTimer(processingTime);
        last_register_process_time.update(processingTime);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple4<String, String, Long, Boolean>> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        int counts = 0;
        Iterator<Map.Entry<String, Long>> iterator = mapState.entries().iterator();
        Long max_water_mark = Long.MIN_VALUE;
        while (iterator.hasNext()){
            max_water_mark = Math.max(max_water_mark, iterator.next().getValue());
            counts = counts + 1;
        }
        if (counts == all_task_nums){
            for (Tuple3<String, String, Long> stringStringLongTuple3 : sample_waterMark_values.get()) {
                out.collect(Tuple4.of(stringStringLongTuple3.f0, stringStringLongTuple3.f1, stringStringLongTuple3.f2 + 1 * 1000, true));
            }
        }
    }

    public static FunKeyByGetMinWaterMark of(int numbers){
        return  new FunKeyByGetMinWaterMark(numbers);
    }
}
