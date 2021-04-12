package com.skyon.project.system.service;

import com.skyon.project.system.domain.*;

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
}
