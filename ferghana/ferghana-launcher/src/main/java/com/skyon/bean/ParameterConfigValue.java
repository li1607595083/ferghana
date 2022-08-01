package com.skyon.bean;

/**
 * @DESCRIPTION:
 * @NAME: TR
 * @DATE: 2021/6/23
 */
public class ParameterConfigValue {
    // 从 kafka 最开始的位置进行消费
    public static String EARLIEST_OFFSET = "earliest-offset";
    // zookeeper 客服端端口号
    public static  int ZK_CLIENT_PORT = 2181;
    // mysql cdc
    public static String MYSQL_CDC = "mysql-cdc";
}
