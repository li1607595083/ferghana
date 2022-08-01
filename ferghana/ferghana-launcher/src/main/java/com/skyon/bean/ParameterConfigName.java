package com.skyon.bean;

/**
 * @DESCRIPTION:
 * @NAME: TR
 * @DATE: 2021/6/22
 */
public class ParameterConfigName {
    // 数据源连接类型
    public static String SOURCE_CONNECTOR = "connector";
    // kafka 消费模式，配置名
    public static String SCAN_STARTUP_MODE = "scan.startup.mode";
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
    // redis 维表连接模式
    public static String REDIS_CONN_MODE = "mode";
    // redis 单节点模式
    public static String REDIS_SINGLE = "single";
    // redis 单节点节点信息
    public static String REDIS_SINGLE_NODE = "single-node";
    // redsi 集群节点模式
    public static String REDIS_CLUSTER = "cluster";
    // redis 集群节点节点信息
    public static String REDIS_CLUSTER_NODES = "single-node";
    // redis 大 key 名
    public static String REDIS_HASHNAME = "hashname";

}
