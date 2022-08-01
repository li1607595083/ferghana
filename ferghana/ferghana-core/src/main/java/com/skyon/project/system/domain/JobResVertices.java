package com.skyon.project.system.domain;


// 作业相应实体
public class JobResVertices {

    private String name; // 物理逻辑

    private Object parallelism; // 并发数

    private Object subtask; // 子任务

    private Object subtaskId; // 子任务Id

    private Object status; // 子任务状态

    private Object attempt; // 攻击

    private Object host; // ip

    private Object subStartTime; // 子任务开始时间

    private Object subEndTime; // 子任务结束时间

    private Object duration; // 子任务时长

    private Object readBytes; // 子任务接收字节

    private Object readRecords; // 子任务接收记录

    private Object writeBytes; // 子任务发送字节

    private Object writeRecords; // 子任务发送记录


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Object getParallelism() {
        return parallelism;
    }

    public void setParallelism(Object parallelism) {
        this.parallelism = parallelism;
    }

    public Object getSubtask() {
        return subtask;
    }

    public void setSubtask(Object subtask) {
        this.subtask = subtask;
    }

    public Object getSubtaskId() {
        return subtaskId;
    }

    public void setSubtaskId(Object subtaskId) {
        this.subtaskId = subtaskId;
    }

    public Object getStatus() {
        return status;
    }

    public void setStatus(Object status) {
        this.status = status;
    }

    public Object getAttempt() {
        return attempt;
    }

    public void setAttempt(Object attempt) {
        this.attempt = attempt;
    }

    public Object getHost() {
        return host;
    }

    public void setHost(Object host) {
        this.host = host;
    }

    public Object getSubStartTime() {
        return subStartTime;
    }

    public void setSubStartTime(Object subStartTime) {
        this.subStartTime = subStartTime;
    }

    public Object getSubEndTime() {
        return subEndTime;
    }

    public void setSubEndTime(Object subEndTime) {
        this.subEndTime = subEndTime;
    }

    public Object getDuration() {
        return duration;
    }

    public void setDuration(Object duration) {
        this.duration = duration;
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
}
