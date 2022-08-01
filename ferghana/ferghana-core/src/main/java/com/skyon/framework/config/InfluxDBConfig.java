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
    public static String url;

    /** 用户 **/
    public static String userName;

    /** 密码 **/
    public static String passWord;

    @Value("${influxdb.url}")
    public void setUrl(String url) {
        InfluxDBConfig.url = url;
    }

    @Value("${influxdb.username}")
    public void setUserName(String username) {
        InfluxDBConfig.userName = username;
    }

    @Value("${influxdb.password}")
    public void setPassWord(String password) {
        InfluxDBConfig.passWord = password;
    }
}
