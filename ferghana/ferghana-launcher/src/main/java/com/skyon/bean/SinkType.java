package com.skyon.bean;

/**
 * @DESCRIPTION:
 * @NAME: TR
 * @DATE: 2021/6/30
 */
public class SinkType {
    // 输出到 kafka
    public static String SINK_KAFKA  = "01";
    // 输出到 hbase
    public static String SINK_HBASE = "03";
    // 输出到 es
    public static String SINK_ES = "04";
    // 输出到 mysql
    public static String SINK_JDBC_MYSQL = "05";
    // 输出到 oracle
    public static String SINK_JDBC_ORACLE = "06";



}
