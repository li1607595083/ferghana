package com.skyon.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkUtils {

    /**
     * @return DataStream runtime environment
     */
    public static StreamExecutionEnvironment dbEnv() {
        return StreamExecutionEnvironment.getExecutionEnvironment();
    }

    /**
     * @return Flink Table runtime environment
     */
    public static StreamTableEnvironment dbTableEnv(StreamExecutionEnvironment bsEnv) {
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        return StreamTableEnvironment.create(bsEnv, bsSettings);
    }

}
