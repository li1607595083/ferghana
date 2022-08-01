package com.skyon.project.system.domain;

import com.skyon.framework.web.domain.BaseEntity;

import java.util.Date;

/**
 * 对象 t_datasource_field
 *
 *
 * @date 2021-02-25
 */
public class TDatasourceField extends BaseEntity {
    private static final long serialVersionUID = 1L;

    /**
     * id
     */
    private Long id;

    private String datasourceTableName; // 表名

    private String tableType; // 表类型  01 数据源表 02 数据维表

    private String fieldName; // 字段

    private String fieldType; // 字段类型

    private String isUsed; // 是否被使用 1是 0否

    private String usedVariableId; // 被使用的变量id 用逗号分隔

    private Date createTime;

    private Date modifyTime;

    public String getTableType() {
        return tableType;
    }

    public void setTableType(String tableType) {
        this.tableType = tableType;
    }

    public String getUsedVariableId() {
        return usedVariableId;
    }

    public void setUsedVariableId(String usedVariableId) {
        this.usedVariableId = usedVariableId;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getDatasourceTableName() {
        return datasourceTableName;
    }

    public void setDatasourceTableName(String datasourceTableName) {
        this.datasourceTableName = datasourceTableName;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getFieldType() {
        return fieldType;
    }

    public void setFieldType(String fieldType) {
        this.fieldType = fieldType;
    }

    public String getIsUsed() {
        return isUsed;
    }

    public void setIsUsed(String isUsed) {
        this.isUsed = isUsed;
    }

    @Override
    public Date getCreateTime() {
        return createTime;
    }

    @Override
    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getModifyTime() {
        return modifyTime;
    }

    public void setModifyTime(Date modifyTime) {
        this.modifyTime = modifyTime;
    }


}
