package com.skyon.domain;

import java.util.Date;

/**
 * SQL任务开发对象 t_sql_develop
 * 
 * @author ruoyi
 * @date 2020-06-04
 */
public class TSqlDevelop extends BaseEntity {
    private static final long serialVersionUID = 1L;

    /**
     * $column.columnComment
     */
    private Long sqlDevelopId;

    /**
     * 作业名称
     */
    private String sqlTaskName;

    /**
     * sql
     */
    private String sqlContent;

    /**
     * 版本
     */
    private String sqlTaskVersion;

    /**
     * 运行状态（0停止 1启用）
     */
    private String runStatus;

    /**
     * 启动时间
     */
    private Date startTime;

    /**
     * 停用时间
     */
    private Date stopTime;

    public void setSqlDevelopId(Long sqlDevelopId) {
        this.sqlDevelopId = sqlDevelopId;
    }

    public Long getSqlDevelopId() {
        return sqlDevelopId;
    }

    public void setSqlTaskName(String sqlTaskName) {
        this.sqlTaskName = sqlTaskName;
    }

    public String getSqlTaskName() {
        return sqlTaskName;
    }

    public void setSqlContent(String sqlContent) {
        this.sqlContent = sqlContent;
    }

    public String getSqlContent() {
        return sqlContent;
    }

    public void setSqlTaskVersion(String sqlTaskVersion) {
        this.sqlTaskVersion = sqlTaskVersion;
    }

    public String getSqlTaskVersion() {
        return sqlTaskVersion;
    }

    public void setRunStatus(String runStatus) {
        this.runStatus = runStatus;
    }

    public String getRunStatus() {
        return runStatus;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStopTime(Date stopTime) {
        this.stopTime = stopTime;
    }

    public Date getStopTime() {
        return stopTime;
    }

}