package com.skyon.project.system.domain;

import com.skyon.framework.aspectj.lang.annotation.Excel;
import com.skyon.framework.aspectj.lang.annotation.Excel.Type;
import com.skyon.framework.web.domain.BaseEntity;
import org.springframework.data.annotation.Transient;

import java.util.Arrays;
import java.util.Date;

/**
 * 对象 t_data_source
 *
 * @date 2020-05-21
 */
public class TDataSource extends BaseEntity {
    private static final long serialVersionUID = 1L;

    /**
     * id
     */
    @Excel(name = "id")
    private Long dataSourceId;

    /**
     * 数据源名称
     */
    @Excel(name = "数据源名称")
    private String dataSourceName;

    /**
     * 数据源类型
     */
    @Excel(name = "数据源类型")
    private String dataSourceType;


    /**
     * 连接器类型
     */
    @Excel(name = "连接器类型")
    private String connectorType;

    /**
     * 数据来源
     */
    @Excel(name = "数据来源")
    private String dataSource;

    /**
     * topic名
     */
    @Excel(name = "topic名")
    private String topicName;

    /**
     * 表名
     */
    @Excel(name = "表名")
    private String tableName;

    /**
     * 消费组
     */
    @Excel(name = "消费组")
    private String consumerGroup;

    /**
     * 消费模式
     */
    @Excel(name = "消费模式")
    private String consumerMode;

    /**
     * zookeeper地址
     */
    @Excel(name = "zookeeper地址")
    private String zookeeperAddress;

    /**
     * kafka地址
     */
    @Excel(name = "kafka地址")
    private String kafkaAddress;

    private String myAddress; // URL地址

    private String port; // 端口

    private String userName; // 用户名

    private String password; // 密码

    private String myDatabase; // 数据库

    private String myTableName; // 表名

    private String scanAll; // 是否全表扫描

    private Object handleData; // 数据操作


    /**
     * schema
     */
    @Excel(name = "schema")
    private String schemaDefine;


    // schama的主键
    private String schemaPrimaryKey;
    // 水印字段
    private String waterMarkName;
    // 水印设值 单位秒
    private String waterMarkTime;

    /**
     * dataBaseType
     */
    @Excel(name = "dataBaseType")
    private String dataBaseType;

    /**
     * 可选参数
     */
    @Excel(name = "可选参数")
    private String optionalParam;


    /**
     * 描述
     */
    @Excel(name = "描述")
    private String description;

    // 建表sql
    private String createTableSql;

    @Transient
    private Object[] dynamicItem;


    public String getMyAddress() {
        return myAddress;
    }

    public void setMyAddress(String myAddress) {
        this.myAddress = myAddress;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getMyDatabase() {
        return myDatabase;
    }

    public void setMyDatabase(String myDatabase) {
        this.myDatabase = myDatabase;
    }

    public String getMyTableName() {
        return myTableName;
    }

    public void setMyTableName(String myTableName) {
        this.myTableName = myTableName;
    }

    public String getScanAll() {
        return scanAll;
    }

    public void setScanAll(String scanAll) {
        this.scanAll = scanAll;
    }

    public Object getHandleData() {
        return handleData;
    }

    public void setHandleData(Object handleData) {
        this.handleData = handleData;
    }

    public String getCreateTableSql() {
        return createTableSql;
    }

    public void setCreateTableSql(String createTableSql) {
        this.createTableSql = createTableSql;
    }

    public void setDataSourceId(Long dataSourceId) {
        this.dataSourceId = dataSourceId;
    }

    public Long getDataSourceId() {
        return dataSourceId;
    }

    public void setDataSourceName(String dataSourceName) {
        this.dataSourceName = dataSourceName;
    }

    public String getDataSourceName() {
        return dataSourceName;
    }

    public void setDataSourceType(String dataSourceType) {
        this.dataSourceType = dataSourceType;
    }

    public String getDataSourceType() {
        return dataSourceType;
    }

    public void setConnectorType(String connectorType) {
        this.connectorType = connectorType;
    }

    public String getConnectorType() {
        return connectorType;
    }

    public void setDataSource(String dataSource) {
        this.dataSource = dataSource;
    }

    public String getDataSource() {
        return dataSource;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerMode(String consumerMode) {
        this.consumerMode = consumerMode;
    }

    public String getConsumerMode() {
        return consumerMode;
    }

    public void setZookeeperAddress(String zookeeperAddress) {
        this.zookeeperAddress = zookeeperAddress;
    }

    public String getZookeeperAddress() {
        return zookeeperAddress;
    }

    public void setKafkaAddress(String kafkaAddress) {
        this.kafkaAddress = kafkaAddress;
    }

    public String getKafkaAddress() {
        return kafkaAddress;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getDescription() {
        return description;
    }

    public String getSchemaDefine() {
        return schemaDefine;
    }

    public void setSchemaDefine(String schemaDefine) {
        this.schemaDefine = schemaDefine;
    }

    public String getDataBaseType() {
        return dataBaseType;
    }

    public void setDataBaseType(String dataBaseType) {
        this.dataBaseType = dataBaseType;
    }

    public Object[] getDynamicItem() {
        return dynamicItem;
    }

    public void setDynamicItem(Object[] dynamicItem) {
        this.dynamicItem = dynamicItem;
    }

    public String getSchemaPrimaryKey() {
        return schemaPrimaryKey;
    }

    public void setSchemaPrimaryKey(String schemaPrimaryKey) {
        this.schemaPrimaryKey = schemaPrimaryKey;
    }

    public String getWaterMarkName() {
        return waterMarkName;
    }

    public void setWaterMarkName(String waterMarkName) {
        this.waterMarkName = waterMarkName;
    }

    public String getWaterMarkTime() {
        return waterMarkTime;
    }

    public void setWaterMarkTime(String waterMarkTime) {
        this.waterMarkTime = waterMarkTime;
    }

    public String getOptionalParam() {
        return optionalParam;
    }

    public void setOptionalParam(String optionalParam) {
        this.optionalParam = optionalParam;
    }

    @Override
    public String toString() {
        return "TDataSource{" +
                "dataSourceId=" + dataSourceId +
                ", dataSourceName='" + dataSourceName + '\'' +
                ", dataSourceType='" + dataSourceType + '\'' +
                ", connectorType='" + connectorType + '\'' +
                ", dataSource='" + dataSource + '\'' +
                ", topicName='" + topicName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", consumerGroup='" + consumerGroup + '\'' +
                ", consumerMode='" + consumerMode + '\'' +
                ", zookeeperAddress='" + zookeeperAddress + '\'' +
                ", kafkaAddress='" + kafkaAddress + '\'' +
                ", myAddress='" + myAddress + '\'' +
                ", port='" + port + '\'' +
                ", userName='" + userName + '\'' +
                ", password='" + password + '\'' +
                ", myDatabase='" + myDatabase + '\'' +
                ", myTableName='" + myTableName + '\'' +
                ", scanAll='" + scanAll + '\'' +
                ", handleData='" + handleData + '\'' +
                ", schemaDefine='" + schemaDefine + '\'' +
                ", schemaPrimaryKey='" + schemaPrimaryKey + '\'' +
                ", waterMarkName='" + waterMarkName + '\'' +
                ", waterMarkTime='" + waterMarkTime + '\'' +
                ", dataBaseType='" + dataBaseType + '\'' +
                ", optionalParam='" + optionalParam + '\'' +
                ", description='" + description + '\'' +
                ", createTableSql='" + createTableSql + '\'' +
                ", dynamicItem=" + Arrays.toString(dynamicItem) +
                '}';
    }
}
