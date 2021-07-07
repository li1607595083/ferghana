package com.skyon.bean;

/**
 * @DESCRIPTION:
 * @NAME: TR
 * @DATE: 2021/6/22
 */
public class ParameterName {
    // 批量输出，批次大小
    public static String BATH_SIZE = "batchSize";
    // topic 分区数
    public static String KAFKA_PARTITION = "kafka_partition";
    // 输出到 kafka 的地址
    public static String KAFKA_ADDRESS = "kafkaAddress";
    // topic 副本数
    public static String TOPIC_REPLICATION = "topic_replication";
    // kafka 所依赖的 zk
    public  static String KAFKA_ZK = "hbaseZK";
    // 输出到 es 地址
    public static String ES_ADDRESS = "esAddress";
    // 测试输出到 es 地址
    public static String TEST_ES_ADDRESS = "testEsAddress";
    // 输出到 hbase 所以依赖的 zk
    public static String HBASE_ZK = "hbaseZK";
    // 输出 jdbc driver
    public static String JDBC_DRIVER = "jdbcDrive";
    // 输出 jdbc url
    public static String JDBC_URL = "jdbcURL";
    // 输出 jdbc user
    public static String JDBC_USER_NAME = "jdbcUserName";
    // 输出 jdbc pwassword
    public static String JDBC_USER_PWD = "jdbcUserPwd";
    // 外部存储系统类型
    public static String SINK_TYPE = "connectorType";
    // checkpoint 触发间隔
    public static String CHECKPOINT_INTERVAL = "checkpointInterval";
    // checkpoint 超时时间
    public static String CHECKPOINT_TIME_OUT = "checkpointTimeOut";
    // checkpoint 最小间隔时间
    public static String BETWEEN_CHECKPOINT_INTERVAL = "BetweenCheckpointInterval";
    // 允许 checkpoint 失败的次数
    public static String CHECKPOINT_FAILURE_NUMBER = "checkpointFailureNumber";
    // 重起次数
    public static String RESTART_STRATEGY_NUMBER = "restartStrategyNumber";
    // 重起间隔
    public static String RESTART_STRATEGY_DELAY_TIME = "restartStrategyDealyTime";
    // watermark 发送间隔
    public static String WATERMARK_INTERVAL = "waterMarkInterval";
    // 最终指标数据个数
    public static String FIELD_OUT_NUMBER = "fieldOutNum";
    // watermark
    public static String WATERMARK = "waterMark";
    // 测试环境 mysql 连接 url
    public static String TEST_MYSQL_DIM_URL = "testDimensionUrl";
    // 测试环境 oracle 连接 url
    public static String TEST_ORACLE_DIM_URL = "testOracleDimensionUrl";
    // 测试环境 oracle 连接 user
    public static String TEST_ORACLE_USERNAME = "testOracleUserName";
    // 测试环境 oracle 连接 password
    public static String TEST_ORACLE_PASSWORD = "testOraclePassWord";
    // 测试环境 oracle 连接 driver
    public static String TEST_ORACLE_DRIVER = "testOracleDriver";
    // 结果指标 sql 集合
    public static String SQL_SET = "sqlSet";
    // 派生变量 sql
    public static String DEVARIABLE_SQLS = "deVariableSqls";
    // 变量 sql
    public static String VARIABLE_SQLS = "variableSqls";
    // 原始时段 sql
    public static String ORIGINAL_VARIABLE_SQL = "originalVariableSql";
    // 数据源表和数据维表的关联的SQL语句
    public static String SOURCE_JOIN_DIM_SQL = "joinSql";
    // 维表类型
    public static String DIM_TYPE = "testDimType";
    // 测试时 维表数据
    public static String DIM_DATA = "testDimdata";
    // 维表创建语句
    public static String DIMENSION_TABLE_SQL = "dimensionTableSql";
    // 维表参数
    public static String DIMENSION_TABLE = "dimensionTable";
    // mysql cdc source 注册的表名
    public static String CDC_SOURCE_TABLE_NAME = "cdcSourceTableName";
    // 测试环境 zookeeper 地址
    public static String TEST_ZK = "testZK";
    // 测试数据
    public static String TEST_SOURCE_DATA = "testSourcedata";
    // 测试数据源, 测试时需要使用此表名，插入测数据
    public static String TEST_SOUCE_TABLE_NAME = "test_source_table_name";
    // 数据源类型
    public static String SOURCE_TYPE = "sourceType";
    // savepoint 路径;
    public static String SAVEPOINT_PATH = "savepointPath";
    // 数据原表创建语句
    public static String SOURCE_TABLE_SQL= "sourceTableSql";
    // 数据源解析 json 格式时，是够忽略错误格式
    public static String  JSON_IGNORE_PARSE_ERRORS = "json_ignore_parse_errors";
    // 程序运行模式
    public static String RUM_MODE = "runMode";
    // 用以保存 savepoint 信息的 url(mysql url)
    public static String SAVEPOINT_URL = "savepointUrl";
    // 测试环境链接 mysql 的 username
    public static String TEST_USER_NAME = "testUserName";
    // 测试环境链接 mysql 的 password
    public static String TEST_PASSWORD = "testPassWord";
    // 测试环境链接 mysql 的 driver
    public static String TEST_DRIVER = "testDriver";
    // 测试时，用以存放计算结果 kakfa topic
    public static String TEST_TOPIC_NAME = "testTopicName";
    // 测试环境连接 kafka 的 broker 信息
    public static  String TEST_BROKER_LIST = "testBrokeList";
    // CDC 数据同步类型
    public static String CDC_ROW_KIND = "cdcRowKind";
    // 数据源检测机制
    public static String IDLE_TIME_OUT = "idleTimeout";
    // 双流 join 参数
    public static String TWO_STREAM_JOIN_PARAMETER = "twoStreamJoinSqls";
    // 双流 join 延迟时间
    public static String TWO_STREAM_JOIN_DELAY_TIME = "two_stream_join_delay_time";
    // 双流 join sql
    public static String TWO_STREAM_JOIN_SQL = "twoStreamJoinSql";
    // 双流 join 后，注册的新表
    public static String TWO_STREAM_JOIN_REGISTER_TABLE_NAME = "twoStreamJoinRegisterTableName";
    // 全局参数
    public static String GLOAB_PARAMETER = "gloabParameter";
    // 计算指标中间注册表
    public static String MIDDLE_TABLE_NAME = "middleTable";
    // 变量包名称
    public static String VARIABLE_PACK_EN = "variablePackEn";
    // 输出 SQL 语句
    public static String SINK_SQL = "sinkSql";
    // 数据源主键
    public static String SOURCE_PRIMARY_KEY = "sourcePrimaryKey";
    // 状态最小空闲时间
    public static String IDLE_STATE_MIN_RETENTION_TIME = "idleStateMinRetentionTime";
    // 状态最大空闲时间
    public static String IDLE_STATE_MAX_RETENTION_TIME = "idleStateMaxRetentionTime";
    // 判断为 null 自定义函数
    public static String IF_FALSE_SET_NULL = "ifFalseSetNull";
    // 存储自定义函数的 mysql url
    public static String REGIS_FUN_URL = "regisfunUrl";
    // 引擎侧率测试
    public static String DECISION_SQL = "decisionSql";
}
