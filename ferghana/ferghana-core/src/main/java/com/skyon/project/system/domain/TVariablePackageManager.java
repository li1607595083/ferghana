package com.skyon.project.system.domain;

import com.skyon.framework.aspectj.lang.annotation.Excel;
import com.skyon.framework.web.domain.BaseEntity;

import java.util.Date;

/**
 * 变量包管理对象 t_variable_package_manager
 * 
 *
 * @date 2020-08-24
 */
public class TVariablePackageManager extends BaseEntity
{
    private static final long serialVersionUID = 1L;

    /** 主键id */
    private Long variablePackId;

    /** 名称 */
    @Excel(name = "名称")
    private String variablePackName;

    /** 英文名 */
    private String variablePackEn;

    private String versionNum;

    private String versionShow;

    /** 运行状态 */
    @Excel(name = "运行状态")
    private String runingState;

    /** 变量分类 */
    @Excel(name = "变量分类")
    private Long variableClassification;

    /** 备注 */
    private String description;

    /** 变量id */
    private Object variableId;

    private String selectVariableNoteForm;

    /** 建表sql */
    private String createTableSql;

    /** 运行sql */
    private String runingSql;
    //配置类型
    private String configurationType;
    // 并发数
    private String concurrency;
    // jobManager堆内存
    private String jobMemory;
    // taskManager堆内存
    private String taskMemory;

    // 关联结果表
    private String resultTable;
    // 结果表sql
    private String resultTableSql;

    private Object applicationId;

    private Object jobId;

    private String varDir;
    // 原始字段
    private Object originalVariable;
    // 原始字段sql
    private String originalVariableSql;
    // 启动参数编码
    private String startParamBase;

    private String savePointDir;

    //-----------------------页面参数---------------------------------
    // 数据源表名
    private String sourceTableName;
    private String dimensionId;
    private Object sourceTableValue; // 变量测试数据源表值

    private Object dimensionTableValue; // 变量测试数据维表值

    private String sourceKey; // 变量测试数据维表值

    private Object testResultItem;

    private Object testDimdata;

    public String getSavePointDir() {
        return savePointDir;
    }

    public void setSavePointDir(String savePointDir) {
        this.savePointDir = savePointDir;
    }

    public String getStartParamBase() {
        return startParamBase;
    }

    public void setStartParamBase(String startParamBase) {
        this.startParamBase = startParamBase;
    }

    public String getVersionNum() {
        return versionNum;
    }

    public void setVersionNum(String versionNum) {
        this.versionNum = versionNum;
    }

    public String getVersionShow() {
        return versionShow;
    }

    public void setVersionShow(String versionShow) {
        this.versionShow = versionShow;
    }

    public Object getOriginalVariable() {
        return originalVariable;
    }

    public void setOriginalVariable(Object originalVariable) {
        this.originalVariable = originalVariable;
    }

    public String getOriginalVariableSql() {
        return originalVariableSql;
    }

    public void setOriginalVariableSql(String originalVariableSql) {
        this.originalVariableSql = originalVariableSql;
    }

    public String getVarDir() {
        return varDir;
    }

    public void setVarDir(String varDir) {
        this.varDir = varDir;
    }

    public Object getTestDimdata() {
        return testDimdata;
    }

    public void setTestDimdata(Object testDimdata) {
        this.testDimdata = testDimdata;
    }

    public String getSelectVariableNoteForm() {
        return selectVariableNoteForm;
    }

    public void setSelectVariableNoteForm(String selectVariableNoteForm) {
        this.selectVariableNoteForm = selectVariableNoteForm;
    }

    public Object getTestResultItem() {
        return testResultItem;
    }

    public void setTestResultItem(Object testResultItem) {
        this.testResultItem = testResultItem;
    }

    public String getSourceKey() {
        return sourceKey;
    }

    public void setSourceKey(String sourceKey) {
        this.sourceKey = sourceKey;
    }

    public Object getSourceTableValue() {
        return sourceTableValue;
    }

    public void setSourceTableValue(Object sourceTableValue) {
        this.sourceTableValue = sourceTableValue;
    }

    public Object getDimensionTableValue() {
        return dimensionTableValue;
    }

    public void setDimensionTableValue(Object dimensionTableValue) {
        this.dimensionTableValue = dimensionTableValue;
    }

    public String getResultTable() {
        return resultTable;
    }

    public void setResultTable(String resultTable) {
        this.resultTable = resultTable;
    }

    public String getResultTableSql() {
        return resultTableSql;
    }

    public void setResultTableSql(String resultTableSql) {
        this.resultTableSql = resultTableSql;
    }

    public String getDimensionId() {
        return dimensionId;
    }

    public void setDimensionId(String dimensionId) {
        this.dimensionId = dimensionId;
    }

    public Object getApplicationId() {
        return applicationId;
    }

    public void setApplicationId(Object applicationId) {
        this.applicationId = applicationId;
    }

    public Object getJobId() {
        return jobId;
    }

    public void setJobId(Object jobId) {
        this.jobId = jobId;
    }

    public String getConfigurationType() {
        return configurationType;
    }

    public void setConfigurationType(String configurationType) {
        this.configurationType = configurationType;
    }

    public String getConcurrency() {
        return concurrency;
    }

    public void setConcurrency(String concurrency) {
        this.concurrency = concurrency;
    }

    public String getJobMemory() {
        return jobMemory;
    }

    public void setJobMemory(String jobMemory) {
        this.jobMemory = jobMemory;
    }

    public String getTaskMemory() {
        return taskMemory;
    }

    public void setTaskMemory(String taskMemory) {
        this.taskMemory = taskMemory;
    }

    public String getSourceTableName() {
        return sourceTableName;
    }

    public void setSourceTableName(String sourceTableName) {
        this.sourceTableName = sourceTableName;
    }

    /** 修改时间 */
    @Excel(name = "修改时间", width = 30, dateFormat = "yyyy-MM-dd")
    private Date modifyTime;

    private String variableClassificationName;

    public String getVariableClassificationName() {
        return variableClassificationName;
    }

    public void setVariableClassificationName(String variableClassificationName) {
        this.variableClassificationName = variableClassificationName;
    }

    public void setVariablePackId(Long variablePackId)
    {
        this.variablePackId = variablePackId;
    }

    public Long getVariablePackId() 
    {
        return variablePackId;
    }
    public void setVariablePackName(String variablePackName) 
    {
        this.variablePackName = variablePackName;
    }

    public String getVariablePackName() 
    {
        return variablePackName;
    }
    public void setVariablePackEn(String variablePackEn) 
    {
        this.variablePackEn = variablePackEn;
    }

    public String getVariablePackEn() 
    {
        return variablePackEn;
    }
    public void setRuningState(String runingState) 
    {
        this.runingState = runingState;
    }

    public String getRuningState() 
    {
        return runingState;
    }

    public Long getVariableClassification() {
        return variableClassification;
    }

    public void setVariableClassification(Long variableClassification) {
        this.variableClassification = variableClassification;
    }

    public Object getVariableId() {
        return variableId;
    }

    public void setVariableId(Object variableId) {
        this.variableId = variableId;
    }

    public void setDescription(String description)
    {
        this.description = description;
    }

    public String getDescription() 
    {
        return description;
    }
    public void setCreateTableSql(String createTableSql)
    {
        this.createTableSql = createTableSql;
    }

    public String getCreateTableSql() 
    {
        return createTableSql;
    }
    public void setRuningSql(String runingSql) 
    {
        this.runingSql = runingSql;
    }

    public String getRuningSql() 
    {
        return runingSql;
    }
    public void setModifyTime(Date modifyTime) 
    {
        this.modifyTime = modifyTime;
    }

    public Date getModifyTime() 
    {
        return modifyTime;
    }

}
