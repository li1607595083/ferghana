package com.skyon.project.system.controller;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Throwables;
import com.skyon.common.exception.CustomException;
import com.skyon.common.utils.poi.ExcelUtil;
import com.skyon.framework.aspectj.lang.annotation.Log;
import com.skyon.framework.aspectj.lang.enums.BusinessType;
import com.skyon.framework.web.controller.BaseController;
import com.skyon.framework.web.domain.AjaxResult;
import com.skyon.framework.web.page.TableDataInfo;
import com.skyon.project.system.domain.SQLTestParams;
import com.skyon.project.system.domain.TDataResultSource;
import com.skyon.project.system.domain.TDataSource;
import com.skyon.project.system.domain.TSqlDevelop;
import com.skyon.project.system.service.ITDataResultSourceService;
import com.skyon.project.system.service.ITDataSourceService;
import com.skyon.project.system.service.ITSqlDevelopService;
import com.skyon.project.system.tuil.KafkaUtil;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.calcite.avatica.util.Quoting.BACK_TICK;

/**
 * SQL任务开发Controller
 *
 *
 * @date 2020-06-04
 */
@RestController
@RequestMapping("/taskdevelop/sqldevelop")
public class TSqlDevelopController extends BaseController {
    @Autowired
    private ITSqlDevelopService tSqlDevelopService;
    @Autowired
    private ITDataSourceService tDataSourceService;
    @Autowired
    private ITDataResultSourceService tDataResultSourceService;


    /**
     * 查询SQL任务开发列表
     */
    @PreAuthorize("@ss.hasPermi('taskdevelop:sqldevelop:list')")
    @GetMapping("/list")
    public TableDataInfo list(TSqlDevelop tSqlDevelop) {
        startPage();
        List<TSqlDevelop> list = tSqlDevelopService.selectTSqlDevelopList(tSqlDevelop);
        return getDataTable(list);
    }

    /**
     * 导出SQL任务开发列表
     */
    @PreAuthorize("@ss.hasPermi('taskdevelop:sqldevelop:export')")
    @Log(title = "SQL任务开发", businessType = BusinessType.EXPORT)
    @GetMapping("/export")
    public AjaxResult export(TSqlDevelop tSqlDevelop) {
        List<TSqlDevelop> list = tSqlDevelopService.selectTSqlDevelopList(tSqlDevelop);
        ExcelUtil<TSqlDevelop> util = new ExcelUtil<TSqlDevelop>(TSqlDevelop.class);
        return util.exportExcel(list, "develop");
    }

    /**
     * 获取SQL任务开发详细信息
     */
    @PreAuthorize("@ss.hasPermi('taskdevelop:sqldevelop:query')")
    @GetMapping(value = "/{sqlDevelopId}")
    public AjaxResult getInfo(@PathVariable("sqlDevelopId") Long sqlDevelopId) {
        return AjaxResult.success(tSqlDevelopService.selectTSqlDevelopById(sqlDevelopId));
    }

    /**
     * 新增SQL任务开发
     */
    @PreAuthorize("@ss.hasPermi('taskdevelop:sqldevelop:add')")
    @Log(title = "SQL任务开发", businessType = BusinessType.INSERT)
    @PostMapping
    public AjaxResult add(@RequestBody TSqlDevelop tSqlDevelop) {
        // 保存前先校验作业名称是否已经存在
        tSqlDevelopService.decideTaskNameBySqlTaskName(tSqlDevelop);
        return toAjax(tSqlDevelopService.insertTSqlDevelop(tSqlDevelop));
    }

    /**
     * 更新SQL任务开发
     * 启动任务入口
     */
    @PreAuthorize("@ss.hasPermi('taskdevelop:sqldevelop:edit')")
    @Log(title = "SQL任务开发", businessType = BusinessType.UPDATE)
    @PutMapping
    public AjaxResult edit(@RequestBody TSqlDevelop tSqlDevelop) {
        // 启动的时候检查同一作业名称是否有启动了的，有则不能启动，需要停止一启动的作业
        tSqlDevelopService.selectBooleanStart(tSqlDevelop);
        Map<String, String> map = new HashMap<>();
        if ("1".equals(tSqlDevelop.getRunStatus())) { // 启动
            map = tSqlDevelopService.jobSubmit(tSqlDevelop.getSourceTableId().toString(),
                    tSqlDevelop.getResultTableIds(), tSqlDevelop.getSqlContent(), tSqlDevelop.getSqlTaskName(), "0");
        } else if ("0".equals(tSqlDevelop.getRunStatus())) {  // 停用
            tSqlDevelopService.jobKill(tSqlDevelop.getApplicationId());
        }
        // 启动作业
        return toAjax(tSqlDevelopService.updateTSqlDevelop(tSqlDevelop, map));
    }

    /**
     * 升级SQL任务开发
     */
    @PutMapping(value = "/developHigh")
    public AjaxResult developHigh(@RequestBody TSqlDevelop tSqlDevelop) {
        String s = tSqlDevelopService.insertTSqlDevelopHigh(tSqlDevelop);
        return AjaxResult.success(s);
    }

    /**
     * 删除SQL任务开发
     */
    @PreAuthorize("@ss.hasPermi('taskdevelop:sqldevelop:remove')")
    @Log(title = "SQL任务开发", businessType = BusinessType.DELETE)
    @DeleteMapping("/{sqlDevelopIds}")
    public AjaxResult remove(@PathVariable Long[] sqlDevelopIds) {
        return toAjax(tSqlDevelopService.deleteTSqlDevelopByIds(sqlDevelopIds));
    }


    /**
     * 解析 flink sql 语句
     *
     * @param sqlString
     * @return
     */
    @GetMapping("/sqlCheck/{sqlString}")
    public AjaxResult parseSql(@PathVariable String sqlString) {

        SqlParser parser = null;
        try {
            parser = SqlParser.create(sqlString, SqlParser.configBuilder()
                    .setParserFactory(FlinkSqlParserImpl.FACTORY)
                    .setQuoting(BACK_TICK)
                    .setUnquotedCasing(Casing.TO_UPPER)
                    .setQuotedCasing(Casing.UNCHANGED)
                    .setConformance(FlinkSqlConformance.DEFAULT)
                    .build()
            );
            parser.parseStmtList();
        } catch (Exception e) {
            String stack = Throwables.getStackTraceAsString(e);
            return AjaxResult.success(formatAndCutStack(stack));
        }
        return AjaxResult.success("校验通过");
    }

    // 异常处理，只抓取 Caused by
    private String formatAndCutStack(String stack) {
        StringBuilder errors = new StringBuilder();
        String regex = "\r\n";
        String[] rows = stack.split(regex);
        for (String eachRow : rows) {
            if (eachRow.contains("Caused by")) {
                errors.append(eachRow).append(regex);
            }
        }
        String error = errors.toString();
        if (error.length() > 3000) {
            error = error.substring(0, 3000);
        }
        return error;
    }

    /**
     * 调试sql
     * 调试->杀进程->删topic
     * sqlTestParams的参数都在前端校验了为必输！
     *
     * @param sqlTestParams
     * @return
     */
    @PostMapping("/sqlRun")
    public AjaxResult sqlRun(@RequestBody List<SQLTestParams> sqlTestParams) {
        String resultMessage = "";

        try {
            // 只有1个
            SQLTestParams param = sqlTestParams.get(0);
            // 输出结果表id 多个用逗号分隔
            StringBuilder sinkTableIds = new StringBuilder();
            for (int i = 0; i < param.getResultTables().length; i++) {
                sinkTableIds.append(param.getResultTables()[i]).append(",");
            }
            sinkTableIds = new StringBuilder(sinkTableIds.substring(0, sinkTableIds.length() - 1));
            // 输入参数
            List<Map> values = param.getValues();
            String[] arr = new String[values.size()];
            for (int i = 0; i < values.size(); i++) {
                Map map = values.get(i);
                String s = JSON.toJSONString(map);
                arr[i] = s;
            }

            TDataSource tDataSource = tDataSourceService.selectTDataSourceById(new Long(param.getSourceTableId()));
            // 获取 sourceTableId 对应的实体 list
            String[] split = sinkTableIds.toString().split(",");
            Long[] longs = new Long[split.length];
            for (int i = 0; i < split.length; i++) {
                longs[i] = new Long(split[i]);
            }
            List<TDataResultSource> tDataResultSources = tDataResultSourceService.selectDataResultSourceByIds(longs);

            // 调试
            Map map = tSqlDevelopService.testRun(param.getSourceTableId(), sinkTableIds.toString(), param.getSqlString(),
                    param.getJobName(), "1", arr, tDataSource, tDataResultSources);

            // 若正常启动了，启动后需杀死进程
            if (map != null && map.size() > 0) {
                if (map.get("applicationId") != null) {
                    tSqlDevelopService.jobKill(map.get("applicationId").toString());

                    // 调试结束，删除topic
                    KafkaUtil.deleteKafkaTopic(tDataSource.getZookeeperAddress(),
                            "upwisdom_test_" + tDataSource.getTopicName());
                    for (int i = 0; i < tDataResultSources.size(); i++) {
                        TDataResultSource tDataResultSource = tDataResultSources.get(i);
                        KafkaUtil.deleteKafkaTopic(tDataResultSource.getZookeeperAddress(),
                                "upwisdom_test_" + tDataResultSource.getTopicName());
                    }
                }
                if (map.get("message") != null) {
                    resultMessage = (String) map.get("message");
                }
                if (map.get("exception") != null) {
                    resultMessage = (String) map.get("exception");
                }
            } else {
                throw new CustomException("该调试出现系统异常！");
            }
        } catch (NumberFormatException e) {
            e.printStackTrace();
            throw new CustomException("该调试出现系统异常！");
        }
        return AjaxResult.success(resultMessage);
    }

    public static void main(String[] args) {
        String resultMessage = "{'name':'zhangsan','sex','1'},{'name':'zhangsan','sex','1'}";
        List list  = new ArrayList();
        list.add("{‘name’:'张三'}");
        list.add("{‘name’:'张三'}");

        System.out.println(JSON.toJSONString(list));
    }


//    /**
//     * 启用状态修改
//     */
//    @Log(title = "状态修改", businessType = BusinessType.UPDATE)
//    @PutMapping("/changeRunStatus")
//    public AjaxResult changeStatus(@RequestBody TSqlDevelop tSqlDevelop)
//    {
//        return toAjax(1);
//    }
}
