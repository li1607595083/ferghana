package com.skyon.function;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class FunMapGetWaterMark extends ProcessFunction<Tuple4<String, String, Long, String>, Tuple5<String,String, Long, String, String>> implements CheckpointedFunction {

    private Long water_mark = Long.MIN_VALUE;
    private String key = "WATER_MARK";
    private transient ListState<Long> checkpoint_water_martk;

    @Override
    public void processElement(Tuple4<String, String, Long, String> t4, Context context, Collector<Tuple5<String,String, Long, String, String>> collector) throws Exception {
        if (t4.f2 >= water_mark){
            water_mark = t4.f2;
            collector.collect(Tuple5.of(key,t4.f3, water_mark, t4.f0, t4.f1));
        }
    }


    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        checkpoint_water_martk.clear();
        checkpoint_water_martk.add(water_mark);
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {

        ListStateDescriptor<Long> max_water_martk_desc = new ListStateDescriptor<>(
                "max_water_martk",
                Long.class
        );
        checkpoint_water_martk = functionInitializationContext.getOperatorStateStore().getListState(max_water_martk_desc);
        if (functionInitializationContext.isRestored()){
            for (Long aLong : checkpoint_water_martk.get()) {
                water_mark = aLong;
            }
        }
    }

}
