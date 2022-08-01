package com.skyon.domain;


/**
 * 自定义函数对象 t_self_function
 * 
 * @author ruoyi
 * @date 2020-06-18
 */
public class TSelfFunction extends BaseEntity
{
    private static final long serialVersionUID = 1L;

    /** 主键 */
    private Long selfFunctionId;

    /** 函数中文名 */
    private String selfFunctionNameCn;

    /** 模板类型 */
    private String moduleType;

    /** 函数名 */
    private String functionName;

    /** 函数路径 */
    private String functionPackagePath;

    /** 函数包路径 */
    private String filePath;

    /** 函数参数 */
    private String functionParams;

    /** 函数描述 */
    private String selfFunctionDesc;
    // 輸入參數
    private String inputParam;
    // 輸出參數
    private String outputParam;

    private Object[] dynamicItem;

    private Object[] dynamicItemOut;


    public void setSelfFunctionId(Long selfFunctionId) 
    {
        this.selfFunctionId = selfFunctionId;
    }

    public Long getSelfFunctionId() 
    {
        return selfFunctionId;
    }
    public void setSelfFunctionNameCn(String selfFunctionNameCn) 
    {
        this.selfFunctionNameCn = selfFunctionNameCn;
    }

    public String getSelfFunctionNameCn() 
    {
        return selfFunctionNameCn;
    }
    public void setModuleType(String moduleType) 
    {
        this.moduleType = moduleType;
    }

    public String getModuleType() 
    {
        return moduleType;
    }
    public void setFunctionName(String functionName) 
    {
        this.functionName = functionName;
    }

    public String getFunctionName() 
    {
        return functionName;
    }
    public void setFunctionPackagePath(String functionPackagePath) 
    {
        this.functionPackagePath = functionPackagePath;
    }

    public String getFunctionPackagePath() 
    {
        return functionPackagePath;
    }
    public void setFilePath(String filePath) 
    {
        this.filePath = filePath;
    }

    public String getFilePath() 
    {
        return filePath;
    }
    public void setFunctionParams(String functionParams) 
    {
        this.functionParams = functionParams;
    }

    public String getFunctionParams() 
    {
        return functionParams;
    }
    public void setSelfFunctionDesc(String selfFunctionDesc) 
    {
        this.selfFunctionDesc = selfFunctionDesc;
    }

    public String getSelfFunctionDesc() 
    {
        return selfFunctionDesc;
    }

    public Object[] getDynamicItem() {
        return dynamicItem;
    }

    public void setDynamicItem(Object[] dynamicItem) {
        this.dynamicItem = dynamicItem;
    }

    public String getInputParam() {
        return inputParam;
    }

    public void setInputParam(String inputParam) {
        this.inputParam = inputParam;
    }

    public String getOutputParam() {
        return outputParam;
    }

    public void setOutputParam(String outputParam) {
        this.outputParam = outputParam;
    }

    public Object[] getDynamicItemOut() {
        return dynamicItemOut;
    }

    public void setDynamicItemOut(Object[] dynamicItemOut) {
        this.dynamicItemOut = dynamicItemOut;
    }

    @Override
    public String toString() {
        return "TSelfFunction{" +
                "selfFunctionId=" + selfFunctionId +
                ", selfFunctionNameCn='" + selfFunctionNameCn + '\'' +
                ", moduleType='" + moduleType + '\'' +
                ", functionName='" + functionName + '\'' +
                ", functionPackagePath='" + functionPackagePath + '\'' +
                ", filePath='" + filePath + '\'' +
                ", functionParams='" + functionParams + '\'' +
                ", selfFunctionDesc='" + selfFunctionDesc + '\'' +
                ", inputParam='" + inputParam + '\'' +
                ", outputParam='" + outputParam + '\'' +
                '}';
    }
}
