package com.skyon.bean;

/**
 * @DESCRIPTION:
 * @NAME: TR
 * @DATE: 2021/6/30
 */
public class SinkType {
    // 输出到 kafka
    public static String SINK_KAFKA  = "01";
    // 输出到 jdbc
    public static String SINK_JDBC = "02";
    // 输出到 mysql
    public static String SINK_JDBC_MYSQL = "mysql";
    // 输出到 oracle
    public static String SINK_JDBC_ORACLE = "oracle";
    // 输出到 hbase
    public static String SINK_HBASE = "03";
    // 输出到 es
    public static String SINK_ES = "04";

}
