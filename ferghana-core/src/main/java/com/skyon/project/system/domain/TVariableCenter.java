package com.skyon.project.system.domain;

import com.skyon.framework.web.domain.BaseEntity;

import java.util.Date;

/**
 * 变量管理中心对象 t_variable_center
 *
 *
 * @date 2020-08-06
 */
public class TVariableCenter extends BaseEntity {
    private static final long serialVersionUID = 1L;

    /**
     * $column.columnComment
     */
    private Long variableId;

    /**
     * 变量名称
     */
    private String variableName;

    /**
     * 变量英文名称
     */
    private String variableNameEn;

    // 变量分类
    private Long variableClassification;

    /**
     * 变量类型 01基础变量 02 派生变量
     */
    private String variableType;

    // 版本号
    private String versionNum;

    // 版本展示 1展示 0不展示
    private String versionShow;

    /**
     * 变量备注
     */
    private String description;

    /**
     * 模板类型 01 普通查询 02 统计查询 03 数据加工
     */
    private String variableModelType;

    /**
     * 变量因子
     */
    private String variableFactor;
    // 变量因子的作用函数
    private Object statisticsSelfFunctionItem;

    // 普通-redis方法
    private String redisFunction;

    // 普通redis_key
    private String redisKey;
    // redis的key的作用函数
    private Object redisSelfFunctionItem;

    // 普通redis_value
    private String redisValue;

    private String redisElementDistinct;

    // 统计-分组
    private Object statisticsGroupItem;

    // 统计-计算模板
    private String statisticsCountModel;
    // 统计模式 01 滚动窗口  02 滑动窗口
    private String statisticsModel;

    // 全局窗口开始时间
    private String globalVariableStartTime;
    // 全局窗口结束时间
    private String globalVariableStopTime;
    // 统计时间
    private String statisticsNum;
    // 统计周期  01秒 02分 03小时 04天 05周 06月 07年
    private String statisticsCycle;
    // 统计条件选项
    private String statisticsConditionOption;
    // 统计条件集合
    private String statisticsConditions;
    // 加工数据模板
    private Long processModel;
    // 加工输入数据
    private String processInputParams;

    private String userDefinedSql;  // 自定义的sql

    /**
     * 集群名
     */
    private String clusterName;

    // 派生变量模板类型 01四则运算 02 计算引擎
    private String deriveVariableModelType;

    // 决策模板类型
    private Long deriveProcessModel;

    // 决策输入字段
    private String deriveInputParams;



    // 水印字段
    private String watermark;

    // 拼接sql
    private String sqlContext;
    // 派生的基础变量
    private String deriveBaseVariable;

    // 测试的数据源表字段
    private Object testSourceTableCol;
    // 测试的数据源维表字段
    private Object testDimensionTableCol;



    /**
     * 修改时间
     */
    private Date modifyTime;

    // 数据源表名
    private String sourceTableName;

    private String variableClassificationName;



    //------------------------页面参数------------------------------------------------------------
    private String selfFunctionNameCn;

    private String deriveVariableSql;  // 派生变量sql

    private Object sourceTableValue; // 变量测试数据源表值

    private Object dimensionTableValue; // 变量测试数据维表值

    private Object testDimdata; // 变量测试数据维表值

    private Object variableFactorType; // 变量因子的数据类型

    public String getGlobalVariableStartTime() {
        return globalVariableStartTime;
    }

    public void setGlobalVariableStartTime(String globalVariableStartTime) {
        this.globalVariableStartTime = globalVariableStartTime;
    }

    public String getGlobalVariableStopTime() {
        return globalVariableStopTime;
    }

    public void setGlobalVariableStopTime(String globalVariableStopTime) {
        this.globalVariableStopTime = globalVariableStopTime;
    }

    public String getDeriveVariableModelType() {
        return deriveVariableModelType;
    }

    public void setDeriveVariableModelType(String deriveVariableModelType) {
        this.deriveVariableModelType = deriveVariableModelType;
    }

    public Long getDeriveProcessModel() {
        return deriveProcessModel;
    }

    public void setDeriveProcessModel(Long deriveProcessModel) {
        this.deriveProcessModel = deriveProcessModel;
    }

    public void setDeriveInputParams(String deriveInputParams) {
        this.deriveInputParams = deriveInputParams;
    }

    public void setDeriveBaseVariable(String deriveBaseVariable) {
        this.deriveBaseVariable = deriveBaseVariable;
    }

    public void setDeriveVariableSql(String deriveVariableSql) {
        this.deriveVariableSql = deriveVariableSql;
    }

    public String getDeriveInputParams() {
        return deriveInputParams;
    }


    public Object getStatisticsSelfFunctionItem() {
        return statisticsSelfFunctionItem;
    }

    public void setStatisticsSelfFunctionItem(Object statisticsSelfFunctionItem) {
        this.statisticsSelfFunctionItem = statisticsSelfFunctionItem;
    }

    public String getRedisElementDistinct() {
        return redisElementDistinct;
    }

    public void setRedisElementDistinct(String redisElementDistinct) {
        this.redisElementDistinct = redisElementDistinct;
    }

    public Object getStatisticsGroupItem() {
        return statisticsGroupItem;
    }

    public void setStatisticsGroupItem(Object statisticsGroupItem) {
        this.statisticsGroupItem = statisticsGroupItem;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getVersionShow() {
        return versionShow;
    }

    public void setVersionShow(String versionShow) {
        this.versionShow = versionShow;
    }

    public String getVersionNum() {
        return versionNum;
    }

    public void setVersionNum(String versionNum) {
        this.versionNum = versionNum;
    }

    public Object getRedisSelfFunctionItem() {
        return redisSelfFunctionItem;
    }

    public void setRedisSelfFunctionItem(Object redisSelfFunctionItem) {
        this.redisSelfFunctionItem = redisSelfFunctionItem;
    }

    public Object getTestDimdata() {
        return testDimdata;
    }

    public void setTestDimdata(Object testDimdata) {
        this.testDimdata = testDimdata;
    }

    public Object getVariableFactorType() {
        return variableFactorType;
    }

    public void setVariableFactorType(Object variableFactorType) {
        this.variableFactorType = variableFactorType;
    }

    public Object getTestSourceTableCol() {
        return testSourceTableCol;
    }

    public void setTestSourceTableCol(Object testSourceTableCol) {
        this.testSourceTableCol = testSourceTableCol;
    }

    public Object getTestDimensionTableCol() {
        return testDimensionTableCol;
    }

    public void setTestDimensionTableCol(Object testDimensionTableCol) {
        this.testDimensionTableCol = testDimensionTableCol;
    }

    private String sourceKey;

    public String getSourceKey() {
        return sourceKey;
    }

    public void setSourceKey(String sourceKey) {
        this.sourceKey = sourceKey;
    }

    public Object getDimensionTableValue() {
        return dimensionTableValue;
    }

    public void setDimensionTableValue(Object dimensionTableValue) {
        this.dimensionTableValue = dimensionTableValue;
    }

    public Object getSourceTableValue() {
        return sourceTableValue;
    }

    public void setSourceTableValue(Object sourceTableValue) {
        this.sourceTableValue = sourceTableValue;
    }

    public Object getDeriveBaseVariable() {
        return deriveBaseVariable;
    }

    public String getDeriveVariableSql() {
        return deriveVariableSql;
    }

    public String getUserDefinedSql() {
        return userDefinedSql;
    }

    public void setUserDefinedSql(String userDefinedSql) {
        this.userDefinedSql = userDefinedSql;
    }

    public String getSelfFunctionNameCn() {
        return selfFunctionNameCn;
    }

    public void setSelfFunctionNameCn(String selfFunctionNameCn) {
        this.selfFunctionNameCn = selfFunctionNameCn;
    }

    public String getRedisFunction() {
        return redisFunction;
    }

    public void setRedisFunction(String redisFunction) {
        this.redisFunction = redisFunction;
    }

    public String getRedisKey() {
        return redisKey;
    }

    public void setRedisKey(String redisKey) {
        this.redisKey = redisKey;
    }

    public String getRedisValue() {
        return redisValue;
    }

    public void setRedisValue(String redisValue) {
        this.redisValue = redisValue;
    }

    public String getVariableClassificationName() {
        return variableClassificationName;
    }

    public void setVariableClassificationName(String variableClassificationName) {
        this.variableClassificationName = variableClassificationName;
    }

    public String getSourceTableName() {
        return sourceTableName;
    }

    public void setSourceTableName(String sourceTableName) {
        this.sourceTableName = sourceTableName;
    }

    public String getSqlContext() {
        return sqlContext;
    }

    public void setSqlContext(String sqlContext) {
        this.sqlContext = sqlContext;
    }

    public String getWatermark() {
        return watermark;
    }

    public void setWatermark(String watermark) {
        this.watermark = watermark;
    }

    public void setVariableId(Long variableId) {
        this.variableId = variableId;
    }

    public Long getVariableId() {
        return variableId;
    }

    public void setVariableName(String variableName) {
        this.variableName = variableName;
    }

    public String getVariableName() {
        return variableName;
    }

    public void setVariableNameEn(String variableNameEn) {
        this.variableNameEn = variableNameEn;
    }

    public String getVariableNameEn() {
        return variableNameEn;
    }

    public void setVariableType(String variableType) {
        this.variableType = variableType;
    }

    public String getVariableType() {
        return variableType;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getDescription() {
        return description;
    }

    public String getVariableModelType() {
        return variableModelType;
    }

    public void setVariableModelType(String variableModelType) {
        this.variableModelType = variableModelType;
    }



    public Long getVariableClassification() {
        return variableClassification;
    }

    public void setVariableClassification(Long variableClassification) {
        this.variableClassification = variableClassification;
    }

    public String getVariableFactor() {
        return variableFactor;
    }

    public void setVariableFactor(String variableFactor) {
        this.variableFactor = variableFactor;
    }


    public void setModifyTime(Date modifyTime) {
        this.modifyTime = modifyTime;
    }

    public Date getModifyTime() {
        return modifyTime;
    }

    public String getStatisticsCountModel() {
        return statisticsCountModel;
    }

    public void setStatisticsCountModel(String statisticsCountModel) {
        this.statisticsCountModel = statisticsCountModel;
    }

    public String getStatisticsModel() {
        return statisticsModel;
    }

    public void setStatisticsModel(String statisticsModel) {
        this.statisticsModel = statisticsModel;
    }

    public String getStatisticsNum() {
        return statisticsNum;
    }

    public void setStatisticsNum(String statisticsNum) {
        this.statisticsNum = statisticsNum;
    }

    public String getStatisticsCycle() {
        return statisticsCycle;
    }

    public void setStatisticsCycle(String statisticsCycle) {
        this.statisticsCycle = statisticsCycle;
    }

    public String getStatisticsConditionOption() {
        return statisticsConditionOption;
    }

    public void setStatisticsConditionOption(String statisticsConditionOption) {
        this.statisticsConditionOption = statisticsConditionOption;
    }

    public String getStatisticsConditions() {
        return statisticsConditions;
    }

    public void setStatisticsConditions(String statisticsConditions) {
        this.statisticsConditions = statisticsConditions;
    }

    public Long getProcessModel() {
        return processModel;
    }

    public void setProcessModel(Long processModel) {
        this.processModel = processModel;
    }

    public String getProcessInputParams() {
        return processInputParams;
    }

    public void setProcessInputParams(String processInputParams) {
        this.processInputParams = processInputParams;
    }

    @Override
    public String toString() {
        return "TVariableCenter{" +
                "variableId=" + variableId +
                ", variableName='" + variableName + '\'' +
                ", variableNameEn='" + variableNameEn + '\'' +
                ", variableClassification='" + variableClassification + '\'' +
                ", variableType='" + variableType + '\'' +
                ", description='" + description + '\'' +
                ", variableModelType='" + variableModelType + '\'' +
                ", variableFactor='" + variableFactor + '\'' +
                ", redisFunction='" + redisFunction + '\'' +
                ", redisKey='" + redisKey + '\'' +
                ", redisValue='" + redisValue + '\'' +
                ", statisticsCountModel='" + statisticsCountModel + '\'' +
                ", statisticsModel='" + statisticsModel + '\'' +
                ", statisticsNum='" + statisticsNum + '\'' +
                ", statisticsCycle='" + statisticsCycle + '\'' +
                ", statisticsConditionOption='" + statisticsConditionOption + '\'' +
                ", statisticsConditions='" + statisticsConditions + '\'' +
                ", processModel='" + processModel + '\'' +
                ", processInputParams='" + processInputParams + '\'' +
                ", watermark='" + watermark + '\'' +
                ", sqlContext='" + sqlContext + '\'' +
                ", deriveBaseVariable='" + deriveBaseVariable + '\'' +
                ", modifyTime=" + modifyTime +
                ", sourceTableName='" + sourceTableName + '\'' +
                ", variableClassificationName='" + variableClassificationName + '\'' +
                ", selfFunctionNameCn='" + selfFunctionNameCn + '\'' +
                ", userDefinedSql='" + userDefinedSql + '\'' +
                ", deriveVariableSql='" + deriveVariableSql + '\'' +
                ", sourceTableValue=" + sourceTableValue +
                ", dimensionTableValue=" + dimensionTableValue +
                ", variableFactorType=" + variableFactorType +
                ", sourceKey='" + sourceKey + '\'' +
                '}';
    }
}
