package com.skyon.project.system.domain;

import java.util.Date;

public class OperationMonitor {
    private static final long serialVersionUID = 1L;

    /** 运维监控id */
    private Long monitorId;

    /** 监控类型 */
    private String monitorType;

    /** 监控时间时间戳 */
    private Long monitorTime;

    /** 监控值 */
    private double monitorValue;

    /** job_id */
    private String jobId;

    /** 监控查询时间 */
    private Date createTime;

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public Long getMonitorId() {
        return monitorId;
    }

    public void setMonitorId(Long monitorId) {
        this.monitorId = monitorId;
    }

    public String getMonitorType() {
        return monitorType;
    }

    public void setMonitorType(String monitorType) {
        this.monitorType = monitorType;
    }

    public Long getMonitorTime() {
        return monitorTime;
    }

    public void setMonitorTime(Long monitorTime) {
        this.monitorTime = monitorTime;
    }

    public double getMonitorValue() {
        return monitorValue;
    }

    public void setMonitorValue(double monitorValue) {
        this.monitorValue = monitorValue;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    @Override
    public String toString() {
        return "OperationMonitor{" +
                "monitorId=" + monitorId +
                ", monitorType='" + monitorType + '\'' +
                ", monitorTime=" + monitorTime +
                ", monitorValue=" + monitorValue +
                ", jobId='" + jobId + '\'' +
                ", createTime=" + createTime +
                '}';
    }
}
