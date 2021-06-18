package com.skyon.project.system.util;

import com.skyon.framework.config.InfluxDBConfig;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;

@AutoConfigureAfter(InfluxDBConfig.class)
public class InfluxDBUtil {

    public static InfluxDB influxdbConnection;

    static {
        InfluxDBConfig influxDBConfig = new InfluxDBConfig();
        if (influxdbConnection == null) {
            influxdbConnection = InfluxDBFactory.connect(influxDBConfig.url,influxDBConfig.userName,influxDBConfig.passWord);
        }
    }

    public static InfluxDB getInfluxdbConnection(){
        return influxdbConnection;
    }

    public static void close(){
        influxdbConnection.close();
    }

}
