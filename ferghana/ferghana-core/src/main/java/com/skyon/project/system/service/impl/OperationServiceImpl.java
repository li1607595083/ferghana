package com.skyon.project.system.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.skyon.project.system.domain.*;
import com.skyon.project.system.service.OperationService;
import com.skyon.project.system.util.HttpUtil;
import com.skyon.project.system.util.PropertiesUtil;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class OperationServiceImpl implements OperationService {


    private static final String FLINKWEBADDRESS = "flink_web_address"; // 启动脚本路径

    @Override
    public List getJobDetail(List<TVariablePackageManager> list) {
        List listResult = new ArrayList();
        if (list != null && list.size() > 0) {

            for (int i = 0; i < list.size(); i++) {
                TVariablePackageManager tVariablePackageManager = list.get(i);
                String appId = tVariablePackageManager.getApplicationId().toString();
                String url = PropertiesUtil.getPro(FLINKWEBADDRESS) + appId + "/jobs/overview";
                String result = HttpUtil.httpConn(url) + "@_@" + appId;
                listResult.add(result);
            }
        }
        return listResult;
    }

    // 转换json
    @Override
    public List<JobRes> parseJobRes(List resultList) {
        List<JobRes> list = new ArrayList<>();
        for (int i = 0; i < resultList.size(); i++) {
            String result = resultList.get(i).toString();
            String[] split = result.split("@_@");
            JSONObject jsonObject = JSON.parseObject(split[0]);
            if (jsonObject != null) {
                List jobs = (List) jsonObject.get("jobs");
                Map s = (Map) jobs.get(0);
                JobRes jobRes = new JobRes();
                jobRes.setApplicationId(split[1]);
                jobRes.setJobId((String) s.get("jid"));
                jobRes.setName((String) s.get("name"));
                jobRes.setStartTime(s.get("start-time"));
                jobRes.setEndTime(s.get("end-time").equals(-1) ? "-" : s.get("end-time"));
                jobRes.setDuration(timeTran(s.get("duration")));
                jobRes.setState((String) s.get("state"));
                list.add(jobRes);
            }
        }
        return list;
    }

    @Override
    public String getDetailByApplicationIdAndJobId(String application, String jobId) {
        String url = PropertiesUtil.getPro(FLINKWEBADDRESS) + application + "/jobs/" + jobId;
        return HttpUtil.httpConn(url);
    }

    @Override
    public List<JobResDetail> parseJobResDetail(String result, String applicationId, String jobId) {
        List<JobResDetail> list = new ArrayList<>();
        JSONObject jsonObject = JSON.parseObject(result);
        JSONArray vertices = (JSONArray) jsonObject.get("vertices");
        for (int i = 0; i < vertices.size(); i++) {
            JobResDetail jobResDetail = new JobResDetail();
            JSONObject o = (JSONObject) vertices.get(i);
            jobResDetail.setApplicationId(applicationId);
            jobResDetail.setJobId(jobId);
            jobResDetail.setName((String) o.get("name"));
            jobResDetail.setVerticesId(o.get("id"));
            jobResDetail.setParallelism(o.get("parallelism"));
            jobResDetail.setStatus((String) o.get("status"));
            jobResDetail.setStartTime(o.get("start-time"));
            jobResDetail.setEndTime(o.get("end-time").equals(-1) ? "-" : o.get("end-time"));
            jobResDetail.setDuration(timeTran(o.get("duration")));
            JSONObject metrics = (JSONObject) o.get("metrics");
            jobResDetail.setBytes_received(metrics.get("read-bytes"));
            jobResDetail.setRecords_received(metrics.get("read-records"));
            jobResDetail.setBytes_send(metrics.get("write-bytes"));
            jobResDetail.setRecords_send(metrics.get("write-records"));
            JSONObject tasks = (JSONObject) o.get("tasks");
            jobResDetail.setTasks(tasks.get("RUNNING"));
            list.add(jobResDetail);
        }
        return list;
    }

    @Override
    public String getVerticesByApplicationIdAndJobIdAndVerticesID(String application, String jobId, String verticeId) {
//        http://liqiang:8088/proxy/application_1594196184598_0007/jobs/097b52e630c04d529e8c3fdaafa688da/vertices/cbc357ccb763df2852fee8c4fc7d55f2
        String url = PropertiesUtil.getPro(FLINKWEBADDRESS) + application + "/jobs/" + jobId + "/vertices/" + verticeId;
        return HttpUtil.httpConn(url);
    }

    @Override
    public List<JobResVertices> parseJobVertices(String result) {
        List list = new ArrayList();
        JSONObject jsonObject = JSON.parseObject(result);
        JSONArray subtasks = (JSONArray) jsonObject.get("subtasks");
        for (int i = 0; i < subtasks.size(); i++) {
            JobResVertices jobResVertices = new JobResVertices();
            JSONObject o = (JSONObject) subtasks.get(i);
            jobResVertices.setSubtaskId(o.get("subtask"));
            jobResVertices.setStatus(o.get("status"));
            jobResVertices.setAttempt(o.get("attempt"));
            jobResVertices.setHost(o.get("host"));
            jobResVertices.setSubStartTime(o.get("start-time"));
            jobResVertices.setSubEndTime(o.get("end-time").equals(-1) ? "-" : o.get("end-time"));
            jobResVertices.setDuration(timeTran(o.get("duration")));
            JSONObject metrics = (JSONObject) o.get("metrics");
            jobResVertices.setReadBytes(metrics.get("read-bytes"));
            jobResVertices.setReadRecords(metrics.get("read-records"));
            jobResVertices.setWriteBytes(metrics.get("write-bytes"));
            jobResVertices.setWriteRecords(metrics.get("write-records"));
            list.add(jobResVertices);
        }
        return list;
    }

    @Override
    public String getTaskManagersByApplicationIdAndJobIdAndVerticesID(String application, String jobId, String verticeId) {
        String url = PropertiesUtil.getPro(FLINKWEBADDRESS) + application + "/jobs/" + jobId + "/vertices/" + verticeId + "/taskmanagers";
        return HttpUtil.httpConn(url);
    }

    @Override
    public List<JobResTaskManager> parseJobTaskManager(String application, String result) {
        List list = new ArrayList();
        JSONObject jsonObject = JSON.parseObject(result);
        JSONArray subtasks = (JSONArray) jsonObject.get("taskmanagers");
        for (int i = 0; i < subtasks.size(); i++) {
            JobResTaskManager jobResTaskManager = new JobResTaskManager();
            JSONObject o = (JSONObject) subtasks.get(i);
            jobResTaskManager.setHost(o.get("host"));
            jobResTaskManager.setApplicationId(application);
            jobResTaskManager.setStartTime(o.get("start-time"));
            jobResTaskManager.setSndTime(o.get("end-time").equals(-1) ? "-" : o.get("end-time"));
            jobResTaskManager.setDuration(timeTran(o.get("duration")));
            jobResTaskManager.setTaskmanagerId(o.get("taskmanager-id"));
            JSONObject metrics = (JSONObject) o.get("metrics");
            jobResTaskManager.setReadBytes(metrics.get("read-bytes"));
            jobResTaskManager.setReadRecords(metrics.get("read-records"));
            jobResTaskManager.setWriteBytes(metrics.get("write-bytes"));
            jobResTaskManager.setWriteRecords(metrics.get("write-records"));
            JSONObject statusCounts = (JSONObject) o.get("status-counts");
            jobResTaskManager.setRuning(statusCounts.get("RUNNING"));
            list.add(jobResTaskManager);
        }
        return list;
    }

    @Override
    public String getTaskManagerLogList(String application, String taskmanagerId) {
//        http://liqiang:8088/proxy/application_1595834195230_0076/taskmanagers/container_1595834195230_0076_01_000002/logs
        String url = PropertiesUtil.getPro(FLINKWEBADDRESS) + application + "/taskmanagers/" + taskmanagerId + "/logs";
        return HttpUtil.httpConn(url);
    }

    @Override
    public List<TaskManagerLogList> parseTaskManagerLog(String result,String applicationId, String taskmanagerId) {
        List list = new ArrayList();
        JSONObject jsonObject = JSON.parseObject(result);
        JSONArray logs = (JSONArray) jsonObject.get("logs");
        if (logs != null && logs.size() > 0) {
            for (int i = 0; i < logs.size(); i++) {
                TaskManagerLogList taskManagerLogListJob = new TaskManagerLogList();
                JSONObject o = (JSONObject) logs.get(i);
                taskManagerLogListJob.setApplicationId(applicationId);
                taskManagerLogListJob.setTaskmanagerId(taskmanagerId);
                taskManagerLogListJob.setName(o.get("name"));
                BigDecimal size = new BigDecimal(o.get("size").toString());
                taskManagerLogListJob.setSize(size.compareTo(new BigDecimal("0")) == 0 ?
                        new BigDecimal("0") : size.divide(new BigDecimal("1000"),3,1));
                list.add(taskManagerLogListJob);
            }
        }
        return list;
    }

    @Override
    public String getLogDetailByName(String application, String taskmanagerId, String name) {
//        http://liqiang:8088/proxy/application_1595834195230_0076/taskmanagers/container_1595834195230_0076_01_000002/logs
        String url = PropertiesUtil.getPro(FLINKWEBADDRESS) + application + "/taskmanagers/" + taskmanagerId + "/logs/" + name;
        return HttpUtil.httpConnLog(url);
    }

    @Override
    public String getStdoutLog(String application, String taskmanagerId) {
//        http://liqiang:8088/proxy/application_1595834195230_0077/taskmanagers/container_1595834195230_0077_01_000002/stdout
        String url = PropertiesUtil.getPro(FLINKWEBADDRESS) + application + "/taskmanagers/" + taskmanagerId + "/stdout";
        return HttpUtil.httpConnLog(url);
    }

    // 将时间搓的转换为 0天7时31分23秒 的 格式
    private String timeTran(Object timeString) {
        Long duration = new Long(timeString.toString());
        return duration / (3600000 * 24) + "天" + (duration % (3600000 * 24)) / 3600000 + "时"
                + ((duration % (3600000 * 24)) % 3600000) / 60000 + "分"
                + (((duration % (3600000 * 24)) % 3600000) % 60000) / 1000 + "秒";
    }
}
