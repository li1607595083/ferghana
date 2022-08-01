package com.skyon.bean;

/**
 * @DESCRIPTION:
 * @NAME: TR
 * @DATE: 2021/6/24
 */
public class ParameterValue {
    // 测试时 flink job 名字
    public static String JOB_NAME = "test";
    // flink 内置函数类型
    public static String INTERNAEL_FUN_TYPE = "00";
    // 测试模式 hbase namespace
    public static String NAME_SPACE = "TEST";
    // 处理时间字段名
    public static String PROCTIME = "proctime";
    // mysql cdc 新增数据类型字段
    public static String CDC_TYPE = "CDC_OP";
    // sql 计算语句的表名
    public static String TMP_TABLE = "tmp_table";
}
