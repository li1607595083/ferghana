package com.skyon.project.system.domain;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import com.skyon.framework.aspectj.lang.annotation.Excel;
import com.skyon.framework.web.domain.BaseEntity;
import org.springframework.data.annotation.Transient;

import java.util.Date;

/**
 * 【请填写功能名称】对象 t_data_result_source
 * 
 *
 * @date 2020-05-31
 */
public class TDataResultSource extends BaseEntity
{
    private static final long serialVersionUID = 1L;

    /** id */
    private Long dataResultSourceId;

    /** 数据源名称 */
    @Excel(name = "数据源名称")
    private String dataSourceName;

    /** 表名 */
    @Excel(name = "表名")
    private String tableName;

    /** 数据源类型 */
    @Excel(name = "数据源类型")
    private String dataSourceType;

    /** 连接器类型 */
    @Excel(name = "连接器类型")
    private String connectorType;

    /** 数据来源 */
    @Excel(name = "数据来源")
    private String dataSource;

    /** topic名 */
    @Excel(name = "topic名")
    private String topicName;


    /** 消费组 */
    @Excel(name = "消费组")
    private String consumerGroup;

    /** 消费模式 */
    @Excel(name = "消费模式")
    private String consumerMode;

    /** zookeeper地址 */
    @Excel(name = "zookeeper地址")
    private String zookeeperAddress;

    /** kafka地址 */
    @Excel(name = "kafka地址")
    private String kafkaAddress;

    /** schama */
    @Excel(name = "schama")
    private String schemaDefine;

    /** 数据类型 */
    @Excel(name = "数据类型")
    private String dataBaseType;



    // jdbc 部分
    /** jdbcURL地址 */
    private String jdbcUrlAddress;

    /** jdbc驱动类 */
    private String jdbcDrive;

    /** jdbc用户名 */
    private String jdbcUserName;

    /** jdbc用户密码 */
    private String jdbcUserPwd;

    // hbase部分
    private String hbaseZKAddress;

    //
    private String esAddress;

    /** 描述 */
    @Excel(name = "描述")
    private String description;

    @Transient
    private Object[] dynamicItem;

    public String getEsAddress() {
        return esAddress;
    }

    public void setEsAddress(String esAddress) {
        this.esAddress = esAddress;
    }

    public String getHbaseZKAddress() {
        return hbaseZKAddress;
    }

    public void setHbaseZKAddress(String hbaseZKAddress) {
        this.hbaseZKAddress = hbaseZKAddress;
    }

    public String getJdbcUrlAddress() {
        return jdbcUrlAddress;
    }

    public void setJdbcUrlAddress(String jdbcUrlAddress) {
        this.jdbcUrlAddress = jdbcUrlAddress;
    }

    public String getJdbcDrive() {
        return jdbcDrive;
    }

    public void setJdbcDrive(String jdbcDrive) {
        this.jdbcDrive = jdbcDrive;
    }

    public String getJdbcUserName() {
        return jdbcUserName;
    }

    public void setJdbcUserName(String jdbcUserName) {
        this.jdbcUserName = jdbcUserName;
    }

    public String getJdbcUserPwd() {
        return jdbcUserPwd;
    }

    public void setJdbcUserPwd(String jdbcUserPwd) {
        this.jdbcUserPwd = jdbcUserPwd;
    }

    public void setDataResultSourceId(Long dataResultSourceId)
    {
        this.dataResultSourceId = dataResultSourceId;
    }

    public Long getDataResultSourceId() 
    {
        return dataResultSourceId;
    }
    public void setDataSourceName(String dataSourceName) 
    {
        this.dataSourceName = dataSourceName;
    }

    public String getDataSourceName() 
    {
        return dataSourceName;
    }
    public void setDataSourceType(String dataSourceType) 
    {
        this.dataSourceType = dataSourceType;
    }

    public String getDataSourceType() 
    {
        return dataSourceType;
    }
    public void setConnectorType(String connectorType) 
    {
        this.connectorType = connectorType;
    }

    public String getConnectorType() 
    {
        return connectorType;
    }
    public void setDataSource(String dataSource) 
    {
        this.dataSource = dataSource;
    }

    public String getDataSource() 
    {
        return dataSource;
    }
    public void setTopicName(String topicName) 
    {
        this.topicName = topicName;
    }

    public String getTopicName() 
    {
        return topicName;
    }
    public void setTableName(String tableName) 
    {
        this.tableName = tableName;
    }

    public String getTableName() 
    {
        return tableName;
    }
    public void setConsumerGroup(String consumerGroup) 
    {
        this.consumerGroup = consumerGroup;
    }

    public String getConsumerGroup() 
    {
        return consumerGroup;
    }
    public void setConsumerMode(String consumerMode) 
    {
        this.consumerMode = consumerMode;
    }

    public String getConsumerMode() 
    {
        return consumerMode;
    }
    public void setZookeeperAddress(String zookeeperAddress) 
    {
        this.zookeeperAddress = zookeeperAddress;
    }

    public String getZookeeperAddress() 
    {
        return zookeeperAddress;
    }
    public void setKafkaAddress(String kafkaAddress) 
    {
        this.kafkaAddress = kafkaAddress;
    }

    public String getKafkaAddress() 
    {
        return kafkaAddress;
    }
    public void setSchemaDefine(String schemaDefine) 
    {
        this.schemaDefine = schemaDefine;
    }

    public String getSchemaDefine() 
    {
        return schemaDefine;
    }
    public void setDataBaseType(String dataBaseType) 
    {
        this.dataBaseType = dataBaseType;
    }

    public String getDataBaseType() 
    {
        return dataBaseType;
    }
    public void setDescription(String description) 
    {
        this.description = description;
    }

    public String getDescription() 
    {
        return description;
    }

    public Object[] getDynamicItem() {
        return dynamicItem;
    }

    public void setDynamicItem(Object[] dynamicItem) {
        this.dynamicItem = dynamicItem;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this,ToStringStyle.MULTI_LINE_STYLE)
            .append("dataResultSourceId", getDataResultSourceId())
            .append("dataSourceName", getDataSourceName())
            .append("dataSourceType", getDataSourceType())
            .append("connectorType", getConnectorType())
            .append("dataSource", getDataSource())
            .append("topicName", getTopicName())
            .append("tableName", getTableName())
            .append("consumerGroup", getConsumerGroup())
            .append("consumerMode", getConsumerMode())
            .append("zookeeperAddress", getZookeeperAddress())
            .append("kafkaAddress", getKafkaAddress())
            .append("schemaDefine", getSchemaDefine())
            .append("dataBaseType", getDataBaseType())
            .append("description", getDescription())
            .append("createTime", getCreateTime())
            .toString();
    }
}
