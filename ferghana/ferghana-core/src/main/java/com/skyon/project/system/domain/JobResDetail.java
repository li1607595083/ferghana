package com.skyon.project.system.domain;

public class JobResDetail {

    private String name; // 物理逻辑

    private String status; // 状态

    private Object bytes_received;  // 接收字节

    private Object records_received; // 接受记录

    private Object bytes_send; // 发送字节

    private Object records_send; // 发送记录

    private Object parallelism; // 并发数

    private Object startTime;  // 开始时间

    private Object endTime; // 结束时间

    private Object duration; // 运行时长

    private Object tasks; // 任务数

    private Object verticesId; //

    private String applicationId;

    private String jobId;


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Object getBytes_received() {
        return bytes_received;
    }

    public void setBytes_received(Object bytes_received) {
        this.bytes_received = bytes_received;
    }

    public Object getRecords_received() {
        return records_received;
    }

    public void setRecords_received(Object records_received) {
        this.records_received = records_received;
    }

    public Object getBytes_send() {
        return bytes_send;
    }

    public void setBytes_send(Object bytes_send) {
        this.bytes_send = bytes_send;
    }

    public Object getRecords_send() {
        return records_send;
    }

    public void setRecords_send(Object records_send) {
        this.records_send = records_send;
    }

    public Object getParallelism() {
        return parallelism;
    }

    public void setParallelism(Object parallelism) {
        this.parallelism = parallelism;
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

    public Object getTasks() {
        return tasks;
    }

    public void setTasks(Object tasks) {
        this.tasks = tasks;
    }

    public Object getVerticesId() {
        return verticesId;
    }

    public void setVerticesId(Object verticesId) {
        this.verticesId = verticesId;
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
}
