package com.skyon.project.system.domain;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import com.skyon.framework.aspectj.lang.annotation.Excel;
import com.skyon.framework.web.domain.BaseEntity;
import org.springframework.data.annotation.Transient;

import java.util.Date;

/**
 * 数据维对象 t_dimension_table
 * 
 *
 * @date 2020-07-22
 */
public class TDimensionTable extends BaseEntity
{
    private static final long serialVersionUID = 1L;

    /** 主键 */
    private Long dimensionId;

    /** 维度表名 */
    @Excel(name = "维度表名")
    private String dimensionName;

    // 维表中文名
    private String dimensionNameZH;

    /** 连接器类型 */
    @Excel(name = "连接器类型")
    private String connectorType;

    /** 数据来源 */
    @Excel(name = "数据来源")
    private String dataSource;

    /** 描述 */
    @Excel(name = "描述")
    private String description;

    /** redis地址 */
    private String redisAddress;

    private String clusterName;

    /** redis数据类型 */
    private String redisDataType;

    /** redisKey */
    private String redisKey;

    /** redisKeyField */
    private String redisKeyField;

    private String redisSchemaDefine;

    /** redis运行模式 */
    private String mode;

    /** jdbcURL地址 */
    private String jdbcUrlAddress;

    /** jdbc驱动类 */
    private String jdbcDrive;

    /** jdbc用户名 */
    private String jdbcUserName;

    /** jdbc用户密码 */
    private String jdbcUserPwd;

    private String jdbcPrimaryKey;

    /** es地址 */
    private String esUrlAddress;

    /** es索引 */
    private String esIndex;

    /** es主键 */
    private String esPrimaryKey;

    private String esCreateSql;

    /** $column.columnComment */
    private String schemaDefine;

    private String jdbcCreateSql;

    /**
     * 可选参数
     */
    private String optionalParam;

    /** zookeeper地址 */
    private String zookeeperAddress;


    /** 表名 */
    private String hbaseTableName;

    private String rowkey;

    private String hbaseSchemaDefine;

    private String hbaseCreateSql;

    @Transient
    private Object[] redisDynamicItem;
    @Transient
    private Object[] jdbcDynamicItem;
    @Transient
    private Object[] esDynamicItem;
    @Transient
    private Object[] hbaseItem;


    public String getOptionalParam() {
        return optionalParam;
    }

    public void setOptionalParam(String optionalParam) {
        this.optionalParam = optionalParam;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getDimensionNameZH() {
        return dimensionNameZH;
    }

    public void setDimensionNameZH(String dimensionNameZH) {
        this.dimensionNameZH = dimensionNameZH;
    }


    public String getHbaseCreateSql() {
        return hbaseCreateSql;
    }

    public void setHbaseCreateSql(String hbaseCreateSql) {
        this.hbaseCreateSql = hbaseCreateSql;
    }

    public String getJdbcCreateSql() {
        return jdbcCreateSql;
    }

    public void setJdbcCreateSql(String jdbcCreateSql) {
        this.jdbcCreateSql = jdbcCreateSql;
    }

    public void setDimensionId(Long dimensionId)
    {
        this.dimensionId = dimensionId;
    }

    public Long getDimensionId() 
    {
        return dimensionId;
    }
    public void setDimensionName(String dimensionName) 
    {
        this.dimensionName = dimensionName;
    }

    public String getDimensionName() 
    {
        return dimensionName;
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
    public void setDescription(String description) 
    {
        this.description = description;
    }

    public String getDescription() 
    {
        return description;
    }
    public void setRedisAddress(String redisAddress) 
    {
        this.redisAddress = redisAddress;
    }

    public String getRedisAddress() 
    {
        return redisAddress;
    }
    public void setRedisDataType(String redisDataType) 
    {
        this.redisDataType = redisDataType;
    }

    public String getRedisDataType() 
    {
        return redisDataType;
    }
    public void setRedisKey(String redisKey) 
    {
        this.redisKey = redisKey;
    }

    public String getRedisKey() 
    {
        return redisKey;
    }
    public void setRedisKeyField(String redisKeyField) 
    {
        this.redisKeyField = redisKeyField;
    }

    public String getRedisKeyField() 
    {
        return redisKeyField;
    }
    public void setJdbcUrlAddress(String jdbcUrlAddress) 
    {
        this.jdbcUrlAddress = jdbcUrlAddress;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public String getJdbcUrlAddress()
    {
        return jdbcUrlAddress;
    }
    public void setJdbcDrive(String jdbcDrive) 
    {
        this.jdbcDrive = jdbcDrive;
    }

    public String getJdbcDrive() 
    {
        return jdbcDrive;
    }
    public void setJdbcUserName(String jdbcUserName) 
    {
        this.jdbcUserName = jdbcUserName;
    }

    public String getJdbcUserName() 
    {
        return jdbcUserName;
    }
    public void setJdbcUserPwd(String jdbcUserPwd) 
    {
        this.jdbcUserPwd = jdbcUserPwd;
    }

    public String getJdbcUserPwd() 
    {
        return jdbcUserPwd;
    }
    public void setSchemaDefine(String schemaDefine) 
    {
        this.schemaDefine = schemaDefine;
    }

    public String getEsUrlAddress()
    {
        return esUrlAddress;
    }
    public void setEsUrlAddress(String esUrlAddress)
    {
        this.esUrlAddress = esUrlAddress;
    }

    public String getEsIndex()
    {
        return esIndex;
    }
    public void setEsIndex(String esIndex)
    {
        this.esIndex = esIndex;
    }

    public String getEsPrimaryKey()
    {
        return esPrimaryKey;
    }
    public void setEsPrimaryKey(String esPrimaryKey)
    {
        this.esPrimaryKey = esPrimaryKey;
    }

    public String getEsCreateSql()
    {
        return esCreateSql;
    }
    public void setEsCreateSql(String esCreateSql)
    {
        this.esCreateSql = esCreateSql;
    }

    public String getSchemaDefine() 
    {
        return schemaDefine;
    }
    public void setZookeeperAddress(String zookeeperAddress) 
    {
        this.zookeeperAddress = zookeeperAddress;
    }

    public String getZookeeperAddress() 
    {
        return zookeeperAddress;
    }
    public void setHbaseTableName(String hbaseTableName) 
    {
        this.hbaseTableName = hbaseTableName;
    }

    public String getHbaseTableName()
    {
        return hbaseTableName;
    }

    public Object[] getRedisDynamicItem() {
        return redisDynamicItem;
    }

    public void setRedisDynamicItem(Object[] redisDynamicItem) {
        this.redisDynamicItem = redisDynamicItem;
    }

    public Object[] getJdbcDynamicItem() {
        return jdbcDynamicItem;
    }

    public void setJdbcDynamicItem(Object[] jdbcDynamicItem) {
        this.jdbcDynamicItem = jdbcDynamicItem;
    }

    public Object[] getEsDynamicItem() {
        return esDynamicItem;
    }

    public void setEsDynamicItem(Object[] esDynamicItem) {
        this.esDynamicItem = esDynamicItem;
    }

    public String getHbaseSchemaDefine() {
        return hbaseSchemaDefine;
    }

    public void setHbaseSchemaDefine(String hbaseSchemaDefine) {
        this.hbaseSchemaDefine = hbaseSchemaDefine;
    }

    public String getRowkey() {
        return rowkey;
    }

    public void setRowkey(String rowkey) {
        this.rowkey = rowkey;
    }

    public Object[] getHbaseItem() {
        return hbaseItem;
    }

    public void setHbaseItem(Object[] hbaseItem) {
        this.hbaseItem = hbaseItem;
    }

    public String getRedisSchemaDefine() {
        return redisSchemaDefine;
    }

    public void setRedisSchemaDefine(String redisSchemaDefine) {
        this.redisSchemaDefine = redisSchemaDefine;
    }

    public String getJdbcPrimaryKey() {
        return jdbcPrimaryKey;
    }

    public void setJdbcPrimaryKey(String jdbcPrimaryKey) {
        this.jdbcPrimaryKey = jdbcPrimaryKey;
    }


    @Override
    public String toString() {
        return new ToStringBuilder(this,ToStringStyle.MULTI_LINE_STYLE)
            .append("dimensionId", getDimensionId())
            .append("dimensionName", getDimensionName())
            .append("connectorType", getConnectorType())
            .append("dataSource", getDataSource())
            .append("description", getDescription())
            .append("redisAddress", getRedisAddress())
            .append("redisDataType", getRedisDataType())
            .append("redisKey", getRedisKey())
            .append("redisKeyField", getRedisKeyField())
            .append("jdbcUrlAddress", getJdbcUrlAddress())
            .append("jdbcDrive", getJdbcDrive())
            .append("jdbcUserName", getJdbcUserName())
            .append("jdbcUserPwd", getJdbcUserPwd())
            .append("esUrlAddress", getEsUrlAddress())
            .append("esIndex", getEsIndex())
            .append("esPrimaryKey", getEsPrimaryKey())
            .append("schemaDefine", getSchemaDefine())
            .append("zookeeperAddress", getZookeeperAddress())
            .append("optionalParam", getOptionalParam())
            .append("hbaseTableName", getHbaseTableName())
            .append("createTime", getCreateTime())
            .toString();
    }
}
