package com.skyon.bean;

/**
 * @DESCRIPTION:
 * @NAME: TR
 * @DATE: 2021/6/22
 */
public class SourceType {
    // 双流 join
    public static String TWO_STREAM_JOIN = "KAFKA_TWO_SOURCE_JOIN";
    // 单个 kafka
    public static String ONE_STREAM = "KAFKA_SINGLE_SOURCE";
    // mysql cdc
    public static String MYSQL_CDC = "MYSQL_CDC";
    // oracle cdc
    public static String ORACLE_CDC = "ORACLE_CDC";

}
