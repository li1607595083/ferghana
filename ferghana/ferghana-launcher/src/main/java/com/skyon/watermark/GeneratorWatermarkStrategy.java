package com.skyon.watermark;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.types.Row;

/**
 * @author jiangshine
 * @version 1.0
 * @date 2021/4/13 14:03
 * @Content
 */
public class GeneratorWatermarkStrategy implements WatermarkStrategy<Row> {
    public int k;
    public long watermark;

    public GeneratorWatermarkStrategy(int k, long watermark) {
        this.k = k;
        this.watermark = watermark;
    }

    @Override
    public TimestampAssigner<Row> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return new GenenratorTimestampAssigner(k);
    }

    @Override
    public WatermarkGenerator<Row> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new GeneratorWatermark(watermark);
    }

}
