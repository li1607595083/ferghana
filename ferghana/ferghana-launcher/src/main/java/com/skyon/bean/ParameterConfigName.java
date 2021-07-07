package com.skyon.bean;

/**
 * @DESCRIPTION:
 * @NAME: TR
 * @DATE: 2021/6/22
 */
public class ParameterConfigName {
    // 数据源连接类型
    public static String SOURCE_CONNECTOR = "connector";
    // 忽略 json 解析错误，配置名
    public static String JSON_IGNORE_PARSE_ERRORS = "json.ignore-parse-errors";
    // kafka 消费模式，配置名
    public static String SCAN_STARTUP_MODE = "scan.startup.mode";
    // 从 kafka 指定位置消费
    public static String SCAN_STARTUP_SPECIFIC_OFFSETS = "scan.startup.specific-offsets";
    // kafka 的连接信息
    public static String PROPERTIES_BOOTSTRAP_SERVERS = "properties.bootstrap.servers";
    // kafka topic
    public static String TOPIC = "topic";
    // mysql, oracle , habse 维表 name
    public static String TABLE_NAME = "table-name";
    // mysql, oracle 维表 url
    public static String TABLE_URL = "url";
    // mysql, oracle 维表 user
    public static String TABLE_USERNAME = "username";
    // mysql, oracle 维表 passwrod
    public static String TABLE_PASSWORD = "password";
    // mysql, oracle 维表 driver
    public static String TABLE_DRIVER = "driver";
    // zookeeper 连接地址
    public static String ZK_QUORUM = "zookeeper.quorum";
    // habse 连接 zookeeper 属性名
    public static String HBASE_ZK_QUORUM = "hbase.zookeeper.quorum";
    // zookeeper 客服端连接端口号
    public static String ZK_CLIENT_PORT = "hbase.zookeeper.property.clientPort";

}
