package com.skyon.project.system.domain;

import com.skyon.framework.web.domain.BaseEntity;

import java.util.Date;

/**
 * 对象 t_dimension_field
 *
 *
 * @date 2021-02-25
 */
public class TDimensionField extends BaseEntity {
    private static final long serialVersionUID = 1L;

    /**
     * id
     */
    private Long id;

    private String dimensionTableName; // 数据维表表名

    private String connectorType; // 连接器类型

    private String fieldName; // 字段

    private String fieldType; // 字段类型

    private String isUsed; // 是否被使用 1是 0否

    private String usedVariableId; // 被使用的变量id 用逗号分隔

    private Date createTime;

    private Date modifyTime;

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

    public String getDimensionTableName() {
        return dimensionTableName;
    }

    public void setDimensionTableName(String dimensionTableName) {
        this.dimensionTableName = dimensionTableName;
    }

    public String getConnectorType() {
        return connectorType;
    }

    public void setConnectorType(String connectorType) {
        this.connectorType = connectorType;
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
