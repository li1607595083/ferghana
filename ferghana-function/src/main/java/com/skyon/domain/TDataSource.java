package com.skyon.domain;

import java.util.Date;

/**
 * 【请填写功能名称】对象 t_data_source
 *
 * @author ruoyi
 * @date 2020-05-21
 */
public class TDataSource extends BaseEntity {
    private static final long serialVersionUID = 1L;

    /**
     * id
     */
    private Long dataSourceId;

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
     * schema
     */
    private String schemaDefine;

    /**
     * dataBaseType
     */
    private String dataBaseType;


    /**
     * 描述
     */
    private String description;

    private Object[] dynamicItem;


//    /** 创建时间 */
//    @Excel(name = "创建时间", width = 30, dateFormat = "yyyy-MM-dd HH:mm:ss", type = Type.EXPORT)
//    private Date createTime;
//
    /**
     * 修改时间
     */
    private Date modifyTime;

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

    public Date getModifyTime() {
        return modifyTime;
    }

    public void setModifyTime(Date modifyTime) {
        this.modifyTime = modifyTime;
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
                ", schema='" + schemaDefine + '\'' +
                ", description='" + description + '\'' +
                ", createTime=" + getCreateTime() +
                ", modifyTime=" + modifyTime +
                '}';
    }
}
