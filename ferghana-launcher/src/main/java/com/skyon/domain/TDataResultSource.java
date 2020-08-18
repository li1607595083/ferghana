package com.skyon.domain;

import java.util.Date;

/**
 * 【请填写功能名称】对象 t_data_result_source
 *
 * @author ruoyi
 * @date 2020-05-31
 */
public class TDataResultSource extends BaseEntity {
    private static final long serialVersionUID = 1L;

    /**
     * id
     */
    private Long dataResultSourceId;

    /**
     * 数据源名称
     */
    private String dataSourceName;

    /**
     * 数据源类型
     */
    private String dataSourceType;

    /**
     * 连接器类型
     */
    private String connectorType;

    /**
     * 数据来源
     */
    private String dataSource;

    /**
     * topic名
     */
    private String topicName;

    /**
     * 表名
     */
    private String tableName;

    /**
     * 消费组
     */
    private String consumerGroup;

    /**
     * 消费模式
     */
    private String consumerMode;

    /**
     * zookeeper地址
     */
    private String zookeeperAddress;

    /**
     * kafka地址
     */
    private String kafkaAddress;

    /**
     * schama
     */
    private String schemaDefine;

    /**
     * 数据类型
     */
    private String dataBaseType;

    /**
     * 描述
     */
    private String description;

    /**
     * 修改时间
     */
    private Date modifyTime;

    private Object[] dynamicItem;

    public void setDataResultSourceId(Long dataResultSourceId) {
        this.dataResultSourceId = dataResultSourceId;
    }

    public Long getDataResultSourceId() {
        return dataResultSourceId;
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

    public void setSchemaDefine(String schemaDefine) {
        this.schemaDefine = schemaDefine;
    }

    public String getSchemaDefine() {
        return schemaDefine;
    }

    public void setDataBaseType(String dataBaseType) {
        this.dataBaseType = dataBaseType;
    }

    public String getDataBaseType() {
        return dataBaseType;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getDescription() {
        return description;
    }

    public Object[] getDynamicItem() {
        return dynamicItem;
    }

    public void setDynamicItem(Object[] dynamicItem) {
        this.dynamicItem = dynamicItem;
    }

    public void setModifyTime(Date modifyTime) {
        this.modifyTime = modifyTime;
    }

    public Date getModifyTime() {
        return modifyTime;
    }

}
