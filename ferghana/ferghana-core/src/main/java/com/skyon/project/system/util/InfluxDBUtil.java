package com.skyon.project.system.util;

import com.skyon.common.utils.spring.SpringUtils;
import com.skyon.framework.config.InfluxDBConfig;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;

@AutoConfigureAfter(InfluxDBConfig.class)
public class InfluxDBUtil {

    private InfluxDBUtil(){

    }

    private static InfluxDB influxdbConnection;

    public static InfluxDB getInfluxdbConnection(){
        if (influxdbConnection == null){
            InfluxDBConfig bean = SpringUtils.getBean(InfluxDBConfig.class);
            influxdbConnection = InfluxDBFactory.connect(bean.url,bean.userName,bean.passWord);
        }
        return influxdbConnection;
    }

    public static void close(){
        influxdbConnection.close();
    }

}
