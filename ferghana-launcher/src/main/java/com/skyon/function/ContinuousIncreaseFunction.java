package com.skyon.function;

import org.apache.flink.table.functions.AggregateFunction;

public class ContinuousIncreaseFunction extends AggregateFunction<Long, ContinuousIncreaseFunction.CountAccum> {

    //定义存放count UDAF状态的accumulator的数据的结构。
    public static class CountAccum {
        public long total;
        public Double lastValue;
        public Boolean continuous;
    }

    //初始化count UDAF的accumulator。
    public CountAccum createAccumulator() {
        CountAccum acc = new CountAccum();
        acc.lastValue = 0.00;
        acc.total = 0;
        acc.continuous = false;
        return acc;
    }

    //getValue提供了，如何通过存放状态的accumulator，计算count UDAF的结果的方法。
    public Long getValue(CountAccum accumulator) {
        return accumulator.total;
    }

    //accumulate提供了，如何根据输入的数据，更新count UDAF存放状态的accumulator。
    public void accumulate(CountAccum accumulator, Double v) {
        if (v > accumulator.lastValue) {
            accumulator.lastValue = v;
            accumulator.continuous = true;
        } else {
            accumulator.lastValue = v;
            accumulator.continuous = false;
        }
        if (accumulator.continuous) {
            accumulator.total++;
        } else {
            accumulator.total = 0;
        }

    }

    public void retract(CountAccum accumulator, Double v) {

    }
}
