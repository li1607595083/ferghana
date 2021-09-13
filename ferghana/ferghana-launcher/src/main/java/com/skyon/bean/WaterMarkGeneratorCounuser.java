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
        if (event.f1 == null){
            currentMaxTimestamp = Math.max(event.f0 - maxOutOfOrderness, currentMaxTimestamp);
        }
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        if (currentMaxTimestamp > lastSendWaterMark){
            lastSendWaterMark = currentMaxTimestamp;
            output.emitWatermark(new Watermark(currentMaxTimestamp));
        }
    }
}
