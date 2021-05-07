package com.skyon.watermark;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.types.Row;

/**
 * @author jiangshine
 * @version 1.0
 * @date 2021/4/12 16:29
 * @Content
 */
public class GeneratorWatermark implements WatermarkGenerator<Row> {
    public long watermark;

    public GeneratorWatermark(long watermark) {
        this.watermark = watermark;
    }

    //这个onEvent是每条数据来时都会调用的，event是数据值，eventTimestamp是经过TimestampAssigner返回的timestamp
    @Override
    public void onEvent(Row event, long eventTimestamp, WatermarkOutput output) {
        output.emitWatermark(new Watermark(eventTimestamp - watermark));//事件时间 - 5s
        System.out.println("onEvent-------------------");
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
//        System.out.println("onPeriodicEmit-------------------");
    }
}
