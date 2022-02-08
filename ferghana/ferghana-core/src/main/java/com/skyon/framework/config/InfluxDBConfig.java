package com.skyon.framework.config;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/**
 * 读取InfluxDB相关配置
 *
 *
 */
@Configuration
public class InfluxDBConfig {

    /** 地址 **/
    @Value("${influxdb.url}")
    public String url;

    /** 用户 **/
    @Value("${influxdb.username}")
    public String userName;

    /** 密码 **/
    @Value("${influxdb.password}")
    public String passWord;


}
