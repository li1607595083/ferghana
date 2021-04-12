package com.skyon.project.system.domain;


// 作业相应实体
public class JobResTaskManager {

    private Object host; // ip

    private Object applicationId; // ip

    private Object readBytes; // 子任务接收字节

    private Object readRecords; // 子任务接收记录

    private Object writeBytes; // 子任务发送字节

    private Object writeRecords; // 子任务发送记录

    private Object startTime; // 子任务开始时间

    private Object sndTime; // 子任务结束时间

    private Object duration; // 子任务时长

    private Object runing; // 状态

    private Object taskmanagerId; //

    public Object getHost() {
        return host;
    }

    public void setHost(Object host) {
        this.host = host;
    }

    public Object getReadBytes() {
        return readBytes;
    }

    public void setReadBytes(Object readBytes) {
        this.readBytes = readBytes;
    }

    public Object getReadRecords() {
        return readRecords;
    }

    public void setReadRecords(Object readRecords) {
        this.readRecords = readRecords;
    }

    public Object getWriteBytes() {
        return writeBytes;
    }

    public void setWriteBytes(Object writeBytes) {
        this.writeBytes = writeBytes;
    }

    public Object getWriteRecords() {
        return writeRecords;
    }

    public void setWriteRecords(Object writeRecords) {
        this.writeRecords = writeRecords;
    }

    public Object getStartTime() {
        return startTime;
    }

    public void setStartTime(Object startTime) {
        this.startTime = startTime;
    }

    public Object getSndTime() {
        return sndTime;
    }

    public void setSndTime(Object sndTime) {
        this.sndTime = sndTime;
    }

    public Object getDuration() {
        return duration;
    }

    public void setDuration(Object duration) {
        this.duration = duration;
    }

    public Object getRuning() {
        return runing;
    }

    public void setRuning(Object runing) {
        this.runing = runing;
    }

    public Object getTaskmanagerId() {
        return taskmanagerId;
    }

    public void setTaskmanagerId(Object taskmanagerId) {
        this.taskmanagerId = taskmanagerId;
    }

    public Object getApplicationId() {
        return applicationId;
    }

    public void setApplicationId(Object applicationId) {
        this.applicationId = applicationId;
    }
}
