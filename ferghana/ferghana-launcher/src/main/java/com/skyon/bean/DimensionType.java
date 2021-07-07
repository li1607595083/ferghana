package com.skyon.bean;

/**
 * @DESCRIPTION:
 * @NAME: TR
 * @DATE: 2021/6/24
 */
public class DimensionType {
    // jdbc 维表，目前仅支持 mysql，oracle
    public static String DIM_JDBC =  "02";
    // mysql 维表
    public static String DIM_JDBC_MYSQL = "mysql";
    // oracle 维表
    public static String DIM_JDBC_ORACLE = "oracle";
    // hbase 维表
    public static String DIM_HBASE = "03";
}
