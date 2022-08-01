package com.skyon.bean;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.java.tuple.Tuple2;

public class WaterMarkGeneratorCounuser implements WatermarkGenerator<Tuple2<Long, String>>{

    private  long maxOutOfOrderness;
    private long lastSendWaterMark = Long.MIN_VALUE;
    private long currentMaxTimestamp = Long.MIN_VALUE;

    public WaterMarkGeneratorCounuser(long maxOutOfOrderness){
        this.maxOutOfOrderness = maxOutOfOrderness;
    }



    @Override
    public void onEvent(Tuple2<Long, String> event, long eventTimestamp, WatermarkOutput output) {
        currentMaxTimestamp = Math.max(event.f1 != null ? event.f0 - maxOutOfOrderness :  event.f0, currentMaxTimestamp);
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        if (currentMaxTimestamp > lastSendWaterMark){
            lastSendWaterMark = currentMaxTimestamp;
            output.emitWatermark(new Watermark(currentMaxTimestamp));
        }
    }
}
