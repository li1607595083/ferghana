package com.skyon.project.system.domain;

// 作业请求主类
public class JobRes {

    private String name; // 作业名称

    private String applicationId; // applicationId

    private String jobId; // jobId

    private Object startTime;  // 开始时间

    private Object endTime; // 结束时间

    private Object duration; // 运行时长

    private String state;  // 状态

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Object getStartTime() {
        return startTime;
    }

    public void setStartTime(Object startTime) {
        this.startTime = startTime;
    }

    public Object getEndTime() {
        return endTime;
    }

    public void setEndTime(Object endTime) {
        this.endTime = endTime;
    }

    public Object getDuration() {
        return duration;
    }

    public void setDuration(Object duration) {
        this.duration = duration;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getApplicationId() {
        return applicationId;
    }

    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

    @Override
    public String toString() {
        return "JobRes{" +
                "name='" + name + '\'' +
                ", applicationId='" + applicationId + '\'' +
                ", jobId='" + jobId + '\'' +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                ", duration=" + duration +
                ", state='" + state + '\'' +
                '}';
    }
}
