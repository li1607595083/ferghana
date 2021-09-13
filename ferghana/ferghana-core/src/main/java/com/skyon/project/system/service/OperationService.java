package com.skyon.project.system.service;

import com.skyon.project.system.domain.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface OperationService {

    // 获取applicationId对应的作业详情
    public List getJobDetail(List<TVariablePackageManager> list);

    // 转换json
    public List<JobRes> parseJobRes(List result);

    // 获取某个作业的详情
    public String getDetailByApplicationIdAndJobId(String application, String jobId);

    // 解析某个作业的详情
    public List<JobResDetail> parseJobResDetail(String result, String applicationId, String jobId);

    // 获取某个作业的vertices
    public String getVerticesByApplicationIdAndJobIdAndVerticesID(String application, String jobId, String verticeId);

    // 转换json
    public List<JobResVertices> parseJobVertices(String result);


    // 获取某个作业的taskManagers
    public String getTaskManagersByApplicationIdAndJobIdAndVerticesID(String application, String jobId, String verticeId);

    // 转换json
    public List<JobResTaskManager> parseJobTaskManager(String applicationId, String result);

    // 获取某个作业的日志列表
    public String getTaskManagerLogList(String application, String taskmanagerId);

    // 解析某个作业的详情
    public List<TaskManagerLogList> parseTaskManagerLog(String result,String applicationId, String taskmanagerId);

    // 根据日志名称获取对应的日志详情
    public String getLogDetailByName(String application, String taskmanagerId, String name);

    // 获取自定义的日志记录
    public String getStdoutLog(String application, String taskmanagerId);

    // 获取运行中、停止作业数量
    public Map getRuningJob();

    // 获取运行中的作业
    public List<TVariablePackageManager> getRuningJobList() throws IOException, InterruptedException;

    // 获取CPU资源、内存资源信息
    public Map getCoreAndMemoryInfo() throws IOException, InterruptedException;

    // 插入运维监控数据
    public int insertOperationMonitor(OperationMonitor operationMonitor);

    // 分组查询作业当前检控数据
    public Map<String, List<OperationMonitor>> selectOperationMonitor(OperationMonitor operationMonitor);

    // 清空过期数据
    public int deleteOperationMonitor();
}
