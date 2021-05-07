package com.skyon.watermark;

import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.types.Row;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * @author jiangshine
 * @version 1.0
 * @date 2021/4/13 14:42
 * @Content
 */
public class GenenratorTimestampAssigner implements TimestampAssigner<Row> {
    public int k;
    public GenenratorTimestampAssigner() {

    }

    public GenenratorTimestampAssigner(int k) {
        this.k = k;
    }

    @Override // element是数据值，recordTimestamp 是数据自带的timestamp
    public long extractTimestamp(Row element, long recordTimestamp) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            return simpleDateFormat.parse(element.getField(k).toString().replace("T","")).getTime() ;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return 0l;
    }
}
