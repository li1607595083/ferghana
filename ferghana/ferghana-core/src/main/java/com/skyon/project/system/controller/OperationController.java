package com.skyon.project.system.controller;

import com.skyon.framework.web.controller.BaseController;
import com.skyon.framework.web.domain.AjaxResult;
import com.skyon.framework.web.page.TableDataInfo;
import com.skyon.project.system.domain.*;
import com.skyon.project.system.service.ITSqlDevelopService;
import com.skyon.project.system.service.ITVariablePackageManagerService;
import com.skyon.project.system.service.OperationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/taskdevelop/operation")
public class OperationController extends BaseController {

    @Autowired
    private ITSqlDevelopService tSqlDevelopService;
    @Autowired
    private OperationService operationService;
    @Autowired
    private ITVariablePackageManagerService pkService;

    /**
     * 查询已启动作业列表
     */
    @GetMapping()
    public TableDataInfo list() {
        startPage();

        // 查询已启动作业的applicationId
//        Map map = tSqlDevelopService.selectTSqlDevelopByRunStatus();
        TVariablePackageManager pk = new TVariablePackageManager();
        pk.setRuningState("1");
        List<TVariablePackageManager> managers = pkService.selectTVariablePackageManagerList(pk);

        // 请求获取applicationId返回的filk信息
        List result = operationService.getJobDetail(managers);
//        List result = new ArrayList();
//        String s = "{\"jobs\":[{\"jid\":\"87938da56a6d17da363ffca981debcff\",\"name\":\"sql_test\",\"state\":\"RUNNING\",\"start-time\":1595228098129,\"end-time\":-1,\"duration\":8095,\"last-modification\":1595228098223,\"tasks\":{\"total\":2,\"created\":0,\"scheduled\":2,\"deploying\":0,\"running\":0,\"finished\":0,\"canceling\":0,\"canceled\":0,\"failed\":0,\"reconciling\":0}}]}@_@111\n";
//        result.add(s);
        // 解析返回信息
        List<JobRes> jobRes = operationService.parseJobRes(result);
        return getDataTable(jobRes);
    }

    /**
     * 查询作业的详细信息
     */
    @PostMapping("/detail")
    public TableDataInfo getOperationDetail(
            @RequestParam("applicationId") String applicationId,
            @RequestParam("jobId") String jobId) {

//        http://liqiang:8088/proxy/application_1593331128602_0040/jobs/2efdf049e3d37c506094ebf46be29a27

        // 获取信息
        String result = operationService.getDetailByApplicationIdAndJobId(applicationId, jobId);
//        String result ="{\"jid\":\"801dcf560ac7a5171e2599edc4ced838\",\"name\":\"sql_test\",\"isStoppable\":false,\"state\":\"RUNNING\",\"start-time\":1595227425758,\"end-time\":-1,\"duration\":21251,\"now\":1595227447009,\"timestamps\":{\"CREATED\":1595227425758,\"SUSPENDED\":0,\"FAILING\":0,\"FAILED\":0,\"FINISHED\":0,\"RESTARTING\":0,\"RECONCILING\":0,\"CANCELLING\":0,\"CANCELED\":0,\"RUNNING\":1595227425842},\"vertices\":[{\"id\":\"cbc357ccb763df2852fee8c4fc7d55f2\",\"name\":\"Source: Kafka011TableSource(name) -> SourceConversion(table=[default_catalog.default_database.source_table, source: [Kafka011TableSource(name)]], fields=[name]) -> SinkConversionToRow -> Sink: Kafka011TableSink(name)\",\"parallelism\":2,\"status\":\"RUNNING\",\"start-time\":1595227434485,\"end-time\":-1,\"duration\":12524,\"tasks\":{\"DEPLOYING\":0,\"FAILED\":0,\"RUNNING\":2,\"RECONCILING\":0,\"FINISHED\":0,\"SCHEDULED\":0,\"CANCELING\":0,\"CANCELED\":0,\"CREATED\":0},\"metrics\":{\"read-bytes\":0,\"read-bytes-complete\":false,\"write-bytes\":0,\"write-bytes-complete\":false,\"read-records\":0,\"read-records-complete\":false,\"write-records\":0,\"write-records-complete\":false}}],\"status-counts\":{\"DEPLOYING\":0,\"FAILED\":0,\"RUNNING\":1,\"RECONCILING\":0,\"FINISHED\":0,\"SCHEDULED\":0,\"CANCELING\":0,\"CANCELED\":0,\"CREATED\":0},\"plan\":{\"jid\":\"801dcf560ac7a5171e2599edc4ced838\",\"name\":\"sql_test\",\"nodes\":[{\"id\":\"cbc357ccb763df2852fee8c4fc7d55f2\",\"parallelism\":2,\"operator\":\"\",\"operator_strategy\":\"\",\"description\":\"Source: Kafka011TableSource(name) -&gt; SourceConversion(table=[default_catalog.default_database.source_table, source: [Kafka011TableSource(name)]], fields=[name]) -&gt; SinkConversionToRow -&gt; Sink: Kafka011TableSink(name)\",\"optimizer_properties\":{}}]}}\n";

        // 解析json
        List<JobResDetail> jobResDetails = operationService.parseJobResDetail(result, applicationId, jobId);

        return getDataTable(jobResDetails);
    }


    /**
     * 查询作业的详细信息
     */
    @RequestMapping(value = "/vertices", method = RequestMethod.GET)
    @ResponseBody
    public TableDataInfo getOperationDetails(@RequestParam(value = "applicationId") String applicationId,
                                             @RequestParam(value = "jobId") String jobId,
                                             @RequestParam(value = "verticesId") String verticesId) {
//        http://liqiang:8088/proxy/application_1594196184598_0007/jobs/097b52e630c04d529e8c3fdaafa688da/vertices/:verticesId
//        http://liqiang:8088/proxy/application_1594196184598_0007/jobs/097b52e630c04d529e8c3fdaafa688da/vertices/cbc357ccb763df2852fee8c4fc7d55f2

        // 获取信息
        String result = operationService.getVerticesByApplicationIdAndJobIdAndVerticesID(applicationId, jobId, verticesId);
//        String result = "{\"id\":\"cbc357ccb763df2852fee8c4fc7d55f2\",\"name\":\"Source: Kafka011TableSource(name) -> SourceConversion(table=[default_catalog.default_database.source_table, source: [Kafka011TableSource(name)]], fields=[name]) -> SinkConversionToRow -> Sink: Kafka011TableSink(name)\",\"parallelism\":2,\"now\":1595227450773,\"subtasks\":[{\"subtask\":0,\"status\":\"RUNNING\",\"attempt\":0,\"host\":\"liqiang\",\"start-time\":1595227434485,\"end-time\":-1,\"duration\":16289,\"metrics\":{\"read-bytes\":0,\"read-bytes-complete\":true,\"write-bytes\":0,\"write-bytes-complete\":true,\"read-records\":0,\"read-records-complete\":true,\"write-records\":0,\"write-records-complete\":true},\"taskmanager-id\":\"container_1594196184598_0016_01_000002\",\"start_time\":1595227434485},{\"subtask\":1,\"status\":\"RUNNING\",\"attempt\":0,\"host\":\"liqiang\",\"start-time\":1595227434488,\"end-time\":-1,\"duration\":16286,\"metrics\":{\"read-bytes\":0,\"read-bytes-complete\":true,\"write-bytes\":0,\"write-bytes-complete\":true,\"read-records\":0,\"read-records-complete\":true,\"write-records\":0,\"write-records-complete\":true},\"taskmanager-id\":\"container_1594196184598_0016_01_000002\",\"start_time\":1595227434488}]}\n";

        // 解析json
        List<JobResVertices> jobResVertices = operationService.parseJobVertices(result);

        return getDataTable(jobResVertices);
    }


    /**
     * 查询作业的详细信息的TaskManagers
     */
    @RequestMapping(value = "/taskManagers", method = RequestMethod.GET)
    @ResponseBody
    public TableDataInfo getOperationTaskManagers(@RequestParam(value = "applicationId") String applicationId,
                                                  @RequestParam(value = "jobId") String jobId,
                                                  @RequestParam(value = "verticesId") String verticesId) {
        System.out.println("-------------------");

//        http://liqiang:8088/proxy/application_1594196184598_0022/jobs/d11851562d50c14f7f4e39dae0623823/vertices/cbc357ccb763df2852fee8c4fc7d55f2/taskmanagers

        // 获取信息
        String result = operationService.getTaskManagersByApplicationIdAndJobIdAndVerticesID(applicationId, jobId, verticesId);
//        String result = "{\"id\": \"cbc357ccb763df2852fee8c4fc7d55f2\",\"name\": \"Source: Kafka011TableSource(name) -> SourceConversion(table=[default_catalog.default_database.source_table, source: [Kafka011TableSource(name)]], fields=[name]) -> SinkConversionToRow -> Sink: Kafka011TableSink(name)\",\"now\": 1595234648413,\"taskmanagers\": [{\"host\": \"liqiang:56194\",\"status\": \"RUNNING\",\"start-time\": 1595234582401,\"end-time\": -1,\"duration\": 66012,\"metrics\": {\"read-bytes\": 0,\"read-bytes-complete\": true,\"write-bytes\": 0,\"write-bytes-complete\": true,\"read-records\": 0,\"read-records-complete\": true,\"write-records\": 0,\"write-records-complete\": true},\"status-counts\": {\"RUNNING\": 2,\"CREATED\": 0,\"CANCELED\": 0,\"DEPLOYING\": 0,\"CANCELING\": 0,\"SCHEDULED\": 0,\"RECONCILING\": 0,\"FINISHED\": 0,\"FAILED\": 0},\"taskmanager-id\": \"container_1594196184598_0022_01_000002\"}]}";
        // 解析json
        List<JobResTaskManager> jobResVertices = operationService.parseJobTaskManager(applicationId, result);


        return getDataTable(jobResVertices);
    }


    /**
     * 查询作业的日志
     */
    @PostMapping("/logList")
    public TableDataInfo getTaskManagerLog(
            @RequestParam("applicationId") String applicationId,
            @RequestParam("taskmanagerId") String taskmanagerId) {

//        http://liqiang:8088/proxy/application_1593331128602_0040/jobs/2efdf049e3d37c506094ebf46be29a27

        // 获取信息
        String result = operationService.getTaskManagerLogList(applicationId, taskmanagerId);
//        String result ="{\"jid\":\"801dcf560ac7a5171e2599edc4ced838\",\"name\":\"sql_test\",\"isStoppable\":false,\"state\":\"RUNNING\",\"start-time\":1595227425758,\"end-time\":-1,\"duration\":21251,\"now\":1595227447009,\"timestamps\":{\"CREATED\":1595227425758,\"SUSPENDED\":0,\"FAILING\":0,\"FAILED\":0,\"FINISHED\":0,\"RESTARTING\":0,\"RECONCILING\":0,\"CANCELLING\":0,\"CANCELED\":0,\"RUNNING\":1595227425842},\"vertices\":[{\"id\":\"cbc357ccb763df2852fee8c4fc7d55f2\",\"name\":\"Source: Kafka011TableSource(name) -> SourceConversion(table=[default_catalog.default_database.source_table, source: [Kafka011TableSource(name)]], fields=[name]) -> SinkConversionToRow -> Sink: Kafka011TableSink(name)\",\"parallelism\":2,\"status\":\"RUNNING\",\"start-time\":1595227434485,\"end-time\":-1,\"duration\":12524,\"tasks\":{\"DEPLOYING\":0,\"FAILED\":0,\"RUNNING\":2,\"RECONCILING\":0,\"FINISHED\":0,\"SCHEDULED\":0,\"CANCELING\":0,\"CANCELED\":0,\"CREATED\":0},\"metrics\":{\"read-bytes\":0,\"read-bytes-complete\":false,\"write-bytes\":0,\"write-bytes-complete\":false,\"read-records\":0,\"read-records-complete\":false,\"write-records\":0,\"write-records-complete\":false}}],\"status-counts\":{\"DEPLOYING\":0,\"FAILED\":0,\"RUNNING\":1,\"RECONCILING\":0,\"FINISHED\":0,\"SCHEDULED\":0,\"CANCELING\":0,\"CANCELED\":0,\"CREATED\":0},\"plan\":{\"jid\":\"801dcf560ac7a5171e2599edc4ced838\",\"name\":\"sql_test\",\"nodes\":[{\"id\":\"cbc357ccb763df2852fee8c4fc7d55f2\",\"parallelism\":2,\"operator\":\"\",\"operator_strategy\":\"\",\"description\":\"Source: Kafka011TableSource(name) -&gt; SourceConversion(table=[default_catalog.default_database.source_table, source: [Kafka011TableSource(name)]], fields=[name]) -&gt; SinkConversionToRow -&gt; Sink: Kafka011TableSink(name)\",\"optimizer_properties\":{}}]}}\n";

        // 解析json
        List<TaskManagerLogList> taskManagerLogLists = operationService.parseTaskManagerLog(result, applicationId, taskmanagerId);

        return getDataTable(taskManagerLogLists);
    }


    /**
     * 根据日志名称查询对应的日志详情
     */
    @RequestMapping(value = "/logname", method = RequestMethod.GET)
    @ResponseBody
    public AjaxResult getLogDetailByLogName(@RequestParam(value = "applicationId") String applicationId,
                                            @RequestParam(value = "taskmanagerId") String taskmanagerId,
                                            @RequestParam(value = "name") String name) {
//        http://liqiang:8088/proxy/application_1595834195230_0077/taskmanagers/container_1595834195230_0077_01_000002/logs/taskmanager.log
        // 获取信息
        String result = operationService.getLogDetailByName(applicationId, taskmanagerId, name);
        return AjaxResult.success(result);
    }

    /**
     *  查询自定义的日志
     */
    @RequestMapping(value = "/stdout", method = RequestMethod.GET)
    @ResponseBody
    public AjaxResult getStdout(@RequestParam(value = "applicationId") String applicationId,
                                            @RequestParam(value = "taskmanagerId") String taskmanagerId) {
//        http://liqiang:8088/proxy/application_1595834195230_0077/taskmanagers/container_1595834195230_0077_01_000002/stdout
        // 获取信息
        String result = operationService.getStdoutLog(applicationId, taskmanagerId);

        return AjaxResult.success(result);
    }

}
