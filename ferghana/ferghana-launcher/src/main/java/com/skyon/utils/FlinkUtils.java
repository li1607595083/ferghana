package com.skyon.utils;

import com.skyon.bean.ParameterName;
import com.skyon.bean.ParameterValue;
import com.skyon.bean.RunMode;
import com.skyon.bean.SourceType;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
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
     * 根据配置参数来设置运行环境；
     */
    public static StreamExecutionEnvironment dbEnv(Properties properties) {
        // 获得 DataStream 运行环境
        StreamExecutionEnvironment env = dbEnv();
        // 设置时间类型
        setTimeType(properties, env);
        // 设置发送 waterMark 间隔
        env.getConfig().setAutoWatermarkInterval(Integer.parseInt(properties.getProperty(ParameterName.WATERMARK_INTERVAL, "50")));
        // 如果为测试模式(01)，着设置并行度为1
        if (RunMode.TEST_MODE.equals(properties.getProperty(ParameterName.RUM_MODE))){
            env.setParallelism(1);
        }
        // 如果为非测试模式,开启checkpoint
        if (RunMode.START_MODE.equals(properties.getProperty(ParameterName.RUM_MODE))){
            setCheckPointingConfig(properties, env);
            env.setMaxParallelism(128);
        }
        return env;
    }

    /**
     * @desc 根据 waterMark 来设置时间类型；
     * @param properties
     * @param env
     */
    private static void setTimeType(Properties properties, StreamExecutionEnvironment env) {
        // "waterMark":"timeFile|outorderTime"(时间字段|允许迟到的时间)
        // 如果字段名不等于 proctime, 视为事件时间，否者视为处理时间
        if (!properties.getProperty(ParameterName.WATERMARK).split("[|]")[0].equals(ParameterValue.PROCTIME)) {
            // 事件时间
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        } else {
            // 处理时间
            env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        }
    }

    /**
     * @desc 设置 checkpoint 相关配置；
     * @param properties 参数属性；
     * @param env DataStream 运行环境；
     */
    private static void setCheckPointingConfig(Properties properties, StreamExecutionEnvironment env) {
            // 开启checkpoint,并指定checkpoint的触发间隔,以及checkpoint的模式
            env.enableCheckpointing(Integer.parseInt(properties.getProperty(ParameterName.CHECKPOINT_INTERVAL,2 * 60 * 1000 + "")), CheckpointingMode.EXACTLY_ONCE);
            // 设置checkpoint超时时间
            env.getCheckpointConfig().setCheckpointTimeout(Integer.parseInt(properties.getProperty(ParameterName.CHECKPOINT_TIME_OUT, 2 * 60 * 1000 + "")));
            // 设置checkpoint之间的最小间隔时间
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(Integer.parseInt(properties.getProperty(ParameterName.BETWEEN_CHECKPOINT_INTERVAL, 60 * 1000 + "")));
            // 设置checkpoint的最大并行度
            env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
            // 当程序停止时,保留checkpoint的状态数据, 默认会删除
            env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
            // 允许checkpoint失败的次数
            env.getCheckpointConfig().setTolerableCheckpointFailureNumber(Integer.parseInt(properties.getProperty(ParameterName.CHECKPOINT_FAILURE_NUMBER, 3 + "")));
//             开启checkpoint未对齐模式
//            env.getCheckpointConfig().enableUnalignedCheckpoints();
            // 重启策略，固定次数重启策略
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.parseInt(properties.getProperty(ParameterName.RESTART_STRATEGY_NUMBER, 3 + "")), Time.of(Integer.parseInt(properties.getProperty(ParameterName.RESTART_STRATEGY_DELAY_TIME, 10 + "")), TimeUnit.SECONDS)));
    }

    /**
     * @return 创建一个 DataStream 的运行环境，并返回；
     */
    public static StreamExecutionEnvironment dbEnv() {
        return StreamExecutionEnvironment.getExecutionEnvironment();
    }

    /**
     * @return 创建 StreamTableEnvironment 运行环境，并返回;
     */
    public static StreamTableEnvironment dbTableEnv(StreamExecutionEnvironment bsEnv, Properties parameterProperties) {
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(bsEnv, bsSettings);
        setStateRetentionTime(streamTableEnvironment, parameterProperties);
        return streamTableEnvironment;
    }

    /**
     *
     * @param streamTableEnvironment StreamTableEnvironment 运行环境;
     * @param parameterProperties 参数属性
     * @return 对 StreamTableEnvironment 运行环境进行设置
     */
    private static void setStateRetentionTime(StreamTableEnvironment streamTableEnvironment, Properties parameterProperties){
        // 设置状态的最小空闲时间和最大的空闲时间，0 表示不清除
        streamTableEnvironment.getConfig().setIdleStateRetentionTime(
                Time.hours(Integer.parseInt(parameterProperties.getProperty(ParameterName.IDLE_STATE_MIN_RETENTION_TIME, "0"))),
                Time.hours(Integer.parseInt(parameterProperties.getProperty(ParameterName.IDLE_STATE_MAX_RETENTION_TIME, "0")))
        );
    }

}
