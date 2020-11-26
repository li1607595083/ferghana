package com.skyon.utils;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class FlinkUtils {

    /**
     * @return DataStream runtime environment
     * @param properties Configuration parameter properties
     */
    public static StreamExecutionEnvironment dbEnv(Properties properties) {
        StreamExecutionEnvironment env = dbEnv();
       // "01"为测模式,设置并行度为1
        if ("01".equals(properties.getProperty("runMode"))){
            env.setParallelism(1);
        }
        // "02"为非测试模式,开启checkpoint
        if ("02".equals(properties.getProperty("runMode"))){
            // 开启checkpoint,并指定checkpoint的触发间隔,以及checkpoint的模式
            env.enableCheckpointing(1*60*1000, CheckpointingMode.EXACTLY_ONCE);
            // 设置checkpoint之间的最小间隔时间
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30 * 1000);
            // 设置checkpoint的最大并行度
            env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
            // 当程序停止时,保留checkpoint的状态数据, 默认会删除
            env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
            // 优先从checkpoint进行恢复操作,而不是savepoint
            env.getCheckpointConfig().setPreferCheckpointForRecovery(true);
            // 开启checkpoint未对齐模式
            env.getCheckpointConfig().enableUnalignedCheckpoints();
            // 重启策略
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)));
        }
        return env;
    }

    /**
     * @return DataStream runtime environment
     */
    public static StreamExecutionEnvironment dbEnv() {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 指定时间类型为事件时间
        executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        return executionEnvironment;
    }

    /**
     * @return Flink Table runtime environment
     */
    public static StreamTableEnvironment dbTableEnv(StreamExecutionEnvironment bsEnv) {
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        return StreamTableEnvironment.create(bsEnv, bsSettings);
    }

}
