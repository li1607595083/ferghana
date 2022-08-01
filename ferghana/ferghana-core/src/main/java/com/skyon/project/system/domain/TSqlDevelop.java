package com.skyon.project.system.domain;

import com.skyon.framework.aspectj.lang.annotation.Excel;
import com.skyon.framework.web.domain.BaseEntity;
import java.util.Date;

/**
 * SQL任务开发对象 t_sql_develop
 * 
 *
 * @date 2020-06-04
 */
public class TSqlDevelop extends BaseEntity
{
    private static final long serialVersionUID = 1L;

    /** $column.columnComment */
    private Long sqlDevelopId;

    /** 作业名称 */
    @Excel(name = "作业名称")
    private String sqlTaskName;

    /** sql */
    @Excel(name = "sql")
    private String sqlContent;

    /** 版本 */
    @Excel(name = "版本")
    private String sqlTaskVersion;

    /** 数据源表id */
    private Long sourceTableId;

    /** 结果表id */
    private String resultTableIds;


    private String applicationId;

    private String jobId;

    /** 运行状态（0停止 1启用） */
    @Excel(name = "运行状态", readConverterExp = "0=停止,1=启用")
    private String runStatus;

    /** 启动时间 */
    @Excel(name = "启动时间", width = 30, dateFormat = "yyyy-MM-dd")
    private Date startTime;

    /** 停用时间 */
    @Excel(name = "停用时间", width = 30, dateFormat = "yyyy-MM-dd")
    private Date stopTime;

    public void setSqlDevelopId(Long sqlDevelopId) 
    {
        this.sqlDevelopId = sqlDevelopId;
    }

    public Long getSqlDevelopId() 
    {
        return sqlDevelopId;
    }
    public void setSqlTaskName(String sqlTaskName) 
    {
        this.sqlTaskName = sqlTaskName;
    }

    public String getSqlTaskName() 
    {
        return sqlTaskName;
    }
    public void setSqlContent(String sqlContent) 
    {
        this.sqlContent = sqlContent;
    }

    public String getSqlContent() 
    {
        return sqlContent;
    }
    public void setSqlTaskVersion(String sqlTaskVersion) 
    {
        this.sqlTaskVersion = sqlTaskVersion;
    }

    public String getSqlTaskVersion() 
    {
        return sqlTaskVersion;
    }
    public void setRunStatus(String runStatus) 
    {
        this.runStatus = runStatus;
    }

    public String getRunStatus() 
    {
        return runStatus;
    }
    public void setStartTime(Date startTime) 
    {
        this.startTime = startTime;
    }

    public Date getStartTime() 
    {
        return startTime;
    }
    public void setStopTime(Date stopTime) 
    {
        this.stopTime = stopTime;
    }

    public Date getStopTime() 
    {
        return stopTime;
    }

    public Long getSourceTableId() {
        return sourceTableId;
    }

    public void setSourceTableId(Long sourceTableId) {
        this.sourceTableId = sourceTableId;
    }

    public String getResultTableIds() {
        return resultTableIds;
    }

    public void setResultTableIds(String resultTableIds) {
        this.resultTableIds = resultTableIds;
    }

    public String getApplicationId() {
        return applicationId;
    }

    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    @Override
    public String toString() {
        return "TSqlDevelop{" +
                "sqlDevelopId=" + sqlDevelopId +
                ", sqlTaskName='" + sqlTaskName + '\'' +
                ", sqlContent='" + sqlContent + '\'' +
                ", sqlTaskVersion='" + sqlTaskVersion + '\'' +
                ", sourceTableId=" + sourceTableId +
                ", resultTableIds='" + resultTableIds + '\'' +
                ", applicationId='" + applicationId + '\'' +
                ", jobId='" + jobId + '\'' +
                ", runStatus='" + runStatus + '\'' +
                ", startTime=" + startTime +
                ", stopTime=" + stopTime +
                '}';
    }
}
