package com.skyon.project.system.domain;

import com.skyon.framework.aspectj.lang.annotation.Excel;
import com.skyon.framework.web.domain.BaseEntity;

import java.util.Date;
import java.util.List;

/**
 * 变量分类对象 t_variable_classification
 *
 *
 * @date 2020-08-21
 */
public class TVariableClassification extends BaseEntity
{
    private static final long serialVersionUID = 1L;

    /** 变量分类id */
    private Long variableClassificationId;

    /** 变量分类名 */
    @Excel(name = "变量分类名")
    private String variableClassificationName;

    /** 关联数据源表 */
    @Excel(name = "关联数据源表")
    private String sourceDabRelation;

    /** 关联数据源表(二) */
    @Excel(name = "关联数据源表(二)")
    private String sourceTwoDabRelation;

//    /** 关联数据维表 */
//    @Excel(name = "关联数据源表")
//    private String dimensionDabRelation;

    /** 备注 */
    @Excel(name = "备注")
    private String description;

    // 主键
    private String schemaPrimaryKey;

    // 数据源表关联字段
    private String sourceDabField;

    /** 关联数据维表 */
    @Excel(name = "关联数据维表")
    private Object dimensionRelation;

    /** 关联数据源表字段 */
    @Excel(name = "关联数据源表字段")
    private Object sourceRelation;

    //---------------------关联表字段------------------------

    //    private String dimensionName;
    private String schemaDefine;
    private String connectorType;
//    private String connectorType;
//    private String dimensionJdbcSchemaDefine;
//    private String hbaseSchemaDefine;


    // ----------------------页面参数------------------------------
    private List selfDefineDimensionField;

    public String getConnectorType() {
        return connectorType;
    }

    public void setConnectorType(String connectorType) {
        this.connectorType = connectorType;
    }

    public String getSchemaDefine() {
        return schemaDefine;
    }

    public void setSchemaDefine(String schemaDefine) {
        this.schemaDefine = schemaDefine;
    }

    public List getSelfDefineDimensionField() {
        return selfDefineDimensionField;
    }

    public void setSelfDefineDimensionField(List selfDefineDimensionField) {
        this.selfDefineDimensionField = selfDefineDimensionField;
    }

//    public String getDimensionDabRelation() {
//        return dimensionDabRelation;
//    }
//
//    public void setDimensionDabRelation(String dimensionDabRelation) {
//        this.dimensionDabRelation = dimensionDabRelation;
//    }

    public String getSourceDabField() {
        return sourceDabField;
    }

    public void setSourceDabField(String sourceDabField) {
        this.sourceDabField = sourceDabField;
    }

    public Object getDimensionRelation() {
        return dimensionRelation;
    }

    public void setDimensionRelation(Object dimensionRelation) {
        this.dimensionRelation = dimensionRelation;
    }

    public Object getSourceRelation() {
        return sourceRelation;
    }

    public void setSourceRelation(Object sourceRelation) {
        this.sourceRelation = sourceRelation;
    }

    public String getSchemaPrimaryKey() {
        return schemaPrimaryKey;
    }

    public void setSchemaPrimaryKey(String schemaPrimaryKey) {
        this.schemaPrimaryKey = schemaPrimaryKey;
    }

    public void setVariableClassificationId(Long variableClassificationId)
    {
        this.variableClassificationId = variableClassificationId;
    }

    public Long getVariableClassificationId()
    {
        return variableClassificationId;
    }
    public void setVariableClassificationName(String variableClassificationName)
    {
        this.variableClassificationName = variableClassificationName;
    }

    public String getVariableClassificationName()
    {
        return variableClassificationName;
    }
    public void setSourceDabRelation(String sourceDabRelation)
    {
        this.sourceDabRelation = sourceDabRelation;
    }

    public String getSourceDabRelation()
    {
        return sourceDabRelation;
    }

    public void setSourceTwoDabRelation(String sourceTwoDabRelation)
    {
        this.sourceTwoDabRelation = sourceTwoDabRelation;
    }

    public String getSourceTwoDabRelation()
    {
        return sourceTwoDabRelation;
    }

    public void setDescription(String description)
    {
        this.description = description;
    }

    public String getDescription()
    {
        return description;
    }

    @Override
    public String toString() {
        return "TVariableClassification{" +
                "variableClassificationId=" + variableClassificationId +
                ", variableClassificationName='" + variableClassificationName + '\'' +
                ", sourceDabRelation='" + sourceDabRelation + '\'' +
                ", sourceTwoDabRelation='" + sourceTwoDabRelation + '\'' +
                ", dimensionRelation='" + dimensionRelation + '\''+
                ", sourceRelation='" + sourceRelation + '\''+
                ", description='" + description + '\'' +
                '}';
    }
}