package com.skyon.project.system.domain;

import com.skyon.framework.aspectj.lang.annotation.Excel;
import com.skyon.framework.aspectj.lang.annotation.Excel.Type;
import com.skyon.framework.web.domain.BaseEntity;
import org.springframework.data.annotation.Transient;

import java.util.Date;

/**
 * 对象 t_data_source
 *
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
     * 描述
     */
    @Excel(name = "描述")
    private String description;

    // 建表sql
    private String createTableSql;

    @Transient
    private Object[] dynamicItem;


//    /** 创建时间 */
//    @Excel(name = "创建时间", width = 30, dateFormat = "yyyy-MM-dd HH:mm:ss", type = Type.EXPORT)
//    private Date createTime;
//
    /**
     * 修改时间
     */
    @Excel(name = "修改时间", width = 30, dateFormat = "yyyy-MM-dd HH:mm:ss", type = Type.EXPORT)
    private Date modifyTime;

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

    public Date getModifyTime() {
        return modifyTime;
    }

    public void setModifyTime(Date modifyTime) {
        this.modifyTime = modifyTime;
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
