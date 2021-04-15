package com.skyon.project.system.controller;

import java.math.BigDecimal;
import java.util.*;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.skyon.project.system.domain.TDataResultSource;
import com.skyon.project.system.domain.TVariableCenter;
import com.skyon.project.system.domain.TVariablePackageOperateLog;
import com.skyon.project.system.mapper.TDataResultSourceMapper;
import com.skyon.project.system.service.*;
import com.skyon.project.system.service.impl.TSqlDevelopServiceImpl;
import com.skyon.project.system.tuil.ApplicationStatusUtil;
import com.skyon.project.system.tuil.PropertiesUtil;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import com.skyon.framework.aspectj.lang.annotation.Log;
import com.skyon.framework.aspectj.lang.enums.BusinessType;
import com.skyon.project.system.domain.TVariablePackageManager;
import com.skyon.framework.web.controller.BaseController;
import com.skyon.framework.web.domain.AjaxResult;
import com.skyon.common.utils.poi.ExcelUtil;
import com.skyon.framework.web.page.TableDataInfo;

import static org.apache.flink.optimizer.Optimizer.LOG;

/**
 * 变量包管理Controller
 *
 * @date 2020-08-24
 */
@RestController
@RequestMapping("/variable/package-manager")
public class TVariablePackageManagerController extends BaseController {

    @Autowired
    private ITVariablePackageManagerService tVariablePackageManagerService;

    @Autowired
    private ITSqlDevelopService tSqlDevelopService;

    @Autowired
    private ITDataResultSourceService resultSourceService;

    @Autowired
    private ITVariableCenterService tVariableCenterService;

    @Autowired
    private ITVariablePackageOperateLogService logService;


    /**
     * 查询变量包管理列表
     */
    @GetMapping("/list")
    @Transactional
    public TableDataInfo list(TVariablePackageManager tVariablePackageManager) {
        // 展示前。先去查询下哪些application在启动中
        ArrayList<String> appList = ApplicationStatusUtil.yarnAllRunningApiAndQueueResource("default");

        startPage();
        List<TVariablePackageManager> list = tVariablePackageManagerService.selectTVariablePackageManagerList(tVariablePackageManager);
        ArrayList uplist = new ArrayList();
        for (TVariablePackageManager manager : list) {
            if ("1".equals(manager.getRuningState()) && !appList.contains(manager.getApplicationId())) { // 已启动 ，但并没有在集合applist里 说明被中途停了
                manager.setRuningState("0");
                uplist.add(manager.getVariablePackId());
            }
        }
        // 把这些包数据库的状态修改过来
        if (uplist.size()>0) tVariablePackageManagerService.updateRuningState(uplist);
        return getDataTable(list);
    }

    @GetMapping("/versionList")
    public TableDataInfo Versionlist(TVariablePackageManager tVariablePackageManager) {
        List<TVariablePackageManager> list = tVariablePackageManagerService.selectTVariablePackageManagerVersionList(tVariablePackageManager);
        return getDataTable(list);
    }

    /**
     * 导出变量包管理列表
     */
    @Log(title = "变量包管理", businessType = BusinessType.EXPORT)
    @GetMapping("/export")
    public AjaxResult export(TVariablePackageManager tVariablePackageManager) {
        List<TVariablePackageManager> list = tVariablePackageManagerService.selectTVariablePackageManagerList(tVariablePackageManager);
        ExcelUtil<TVariablePackageManager> util = new ExcelUtil<TVariablePackageManager>(TVariablePackageManager.class);
        return util.exportExcel(list, "manager");
    }

    /**
     * 获取变量包管理详细信息
     */
    @GetMapping(value = "/{variablePackId}")
    public AjaxResult getInfo(@PathVariable("variablePackId") Long variablePackId) {
        return AjaxResult.success(tVariablePackageManagerService.selectTVariablePackageManagerById(variablePackId));
    }

    /**
     * 新增变量包管理
     */
    @Log(title = "变量包管理", businessType = BusinessType.INSERT)
    @PostMapping
    public AjaxResult add(@RequestBody TVariablePackageManager tVariablePackageManager) {
        Map map = tVariablePackageManagerService.getKeyByVariableId(tVariablePackageManager.getVariableClassification());

        TVariablePackageOperateLog log = new TVariablePackageOperateLog();
        log.setVariablePackNameEn(tVariablePackageManager.getVariablePackEn());
        log.setOperateType("新增");
        logService.insertTVariablePackageOperateLog(log);

        tVariablePackageManager.setVersionNum("1.0");
        tVariablePackageManager.setVersionShow("1");

        return toAjax(tVariablePackageManagerService.insertTVariablePackageManager(map, tVariablePackageManager, "start"));
    }

    /**
     * 修改变量包管理
     */
    @Log(title = "变量包管理", businessType = BusinessType.UPDATE)
    @PutMapping
    @Transactional
    public AjaxResult edit(@RequestBody TVariablePackageManager tVariablePackageManager) {
        Map map = tVariablePackageManagerService.getKeyByVariableId(tVariablePackageManager.getVariableClassification());
        TVariablePackageManager old = tVariablePackageManagerService.selectTVariablePackageManagerByVersion(tVariablePackageManager);
        boolean b = compareVariablePackage(tVariablePackageManager, old);
        int num = 0;
        if (b) {
            num = tVariablePackageManagerService.updateTVariablePackageManager(map, tVariablePackageManager);
        } else {
            // 修改上一版本
            tVariablePackageManagerService.updatePackageVersion(old);

            // 新增版本
            BigDecimal bigDecimal = new BigDecimal(old.getVersionNum()).add(new BigDecimal("1.0"));
            tVariablePackageManager.setVersionNum(bigDecimal.toString());
            tVariablePackageManager.setVersionShow("1");
            tVariablePackageManager.setModifyTime(null);
            num = tVariablePackageManagerService.insertTVariablePackageManager(map, tVariablePackageManager, "start");
            // 日志记录
            TVariablePackageOperateLog log = new TVariablePackageOperateLog();
            log.setVariablePackNameEn(tVariablePackageManager.getVariablePackEn());
            log.setOperateType("修改");
            logService.insertTVariablePackageOperateLog(log);
        }
        return toAjax(num);
    }

    private boolean compareVariablePackage(TVariablePackageManager newPk, TVariablePackageManager oldPk) {
        boolean flag = false;
        if ((newPk.getResultTable() == null && oldPk.getResultTable() == null)
                || (newPk.getResultTable() != null && newPk.getResultTable().equals(oldPk.getResultTable()))) { // redis条件
            if ((newPk.getVariableId() == null && oldPk.getVariableId() == null)
                    || (newPk.getVariableId() != null && JSON.toJSONString(newPk.getVariableId()).equals(oldPk.getVariableId()))) { // 统计分组

                if ((newPk.getOriginalVariable() == null && oldPk.getOriginalVariable() == null)
                        || (newPk.getOriginalVariable() != null && JSON.toJSONString(newPk.getOriginalVariable()).equals(oldPk.getOriginalVariable()))) { // 统计分组

                    flag = true; // 都没变
                }
            }
        }
        return flag;
    }

    /**
     * 测试变量管理中心
     */
    @PostMapping("/test")
    public AjaxResult test(@RequestBody TVariablePackageManager pk) {
        // 日志记录
        TVariablePackageOperateLog log = new TVariablePackageOperateLog();
        log.setVariablePackNameEn(pk.getVariablePackEn());
        log.setOperateType("变量包测试");
        logService.insertTVariablePackageOperateLog(log);
        System.out.println("-------------------------1");
        //根据变量分类-数据源表-主键

        Map mapParam = tVariablePackageManagerService.getKeyByVariableId(pk.getVariableClassification());
        LOG.info("----1："+mapParam.toString());
        LOG.info("----2----");
        List<TVariableCenter> variableListByIds = tVariablePackageManagerService.getVariableListByIds(pk.getVariableId());
        String millis = "topic" + System.currentTimeMillis();
        // 组装测试参数
        String variableTest = tVariablePackageManagerService.variableTest(pk, mapParam, variableListByIds, "test", millis);
        LOG.info("----3:" + variableTest);
        // 获取测试结果
        List result = tVariableCenterService.testRun(variableTest, millis);
        LOG.info("----4:" + result);
        LOG.info("----5");
        // 顺序
        ArrayList sourceTableValue = (ArrayList) pk.getSourceTableValue();
        ArrayList testResultItem = (ArrayList) pk.getTestResultItem();
        String s = "";
        List list = new ArrayList();
        if (result.size() > 0) {
            for (int i = 0; i < sourceTableValue.size(); i++) {
                for (int j = 0; j < result.size(); j++) {
                    Map map = new HashMap();
                    Map o = (Map) sourceTableValue.get(i); // 页面的
                    Object o2 = result.get(j); // 结果的
                    JSONObject jsonObject = JSON.parseObject(o2.toString());
                    Object tmp2 = jsonObject.get(pk.getSourceKey());
                    Object tmp = o.get(pk.getSourceKey());  // 主键的值
                    if (tmp.equals(tmp2)) {
                        map.put(pk.getSourceKey(), tmp); // 主键的值
                        for (int k = 0; k < testResultItem.size(); k++) {
                            map.put(testResultItem.get(k), jsonObject.get(testResultItem.get(k)));
                        }
                        list.add(map);
                    }
                }
            }
            s = JSON.toJSONString(list);
        } else {
            s = "no success";
        }

        // 值
        LOG.info("----result:" + s);
        return AjaxResult.success(s);
    }


    // 启动变量包
    @PutMapping("/start")
    public AjaxResult startVariablePackage(@RequestBody TVariablePackageManager pkManager) {
        // 日志记录
        TVariablePackageOperateLog log = new TVariablePackageOperateLog();
        log.setVariablePackNameEn(pkManager.getVariablePackEn());
        log.setOperateType("变量包启动");
        logService.insertTVariablePackageOperateLog(log);
        //变量包名字、SQL（以分号拼接）、字段个数、主键名称、运行or测试、资源配置情况（以分号拼接）并发数、taskmanager内存、jobmanager内存

        //根据变量分类-数据源表-主键
        LOG.info("-----------package start------------");
        Map map = tVariablePackageManagerService.getKeyByVariableId(pkManager.getVariableClassification());
        // sql 拼接 ： 建表sql + 变量运行sql ; 中间用分号连接;  用变量id用查对应的sql
        LOG.info("-----------1111111111111------------");

        List<TVariableCenter> variableListByIds = tVariablePackageManagerService.getVariableListByIds(pkManager.getVariableId());
        LOG.info("-----------222222222222------------");

        String[] pathArray = tVariablePackageManagerService.joinPath(map, pkManager, variableListByIds);
        LOG.info("-----------pathArray1------------" + pathArray[1]);
        LOG.info("-----------pathArray2------------" + pathArray[2]);

        Map mapResult = tVariablePackageManagerService.exe(pathArray);
        LOG.info("--------------33333333333333---------------");

        int i = 0;
        Object jobId = mapResult.get("jobId");
        Object applicationId = mapResult.get("applicationId");
        if (mapResult.size() > 0 && jobId != null) {
            pkManager.setApplicationId(applicationId);
            pkManager.setJobId(jobId);
            String[] s = pathArray[2].split(" ");
            pkManager.setStartParamBase(s[0]);
            pkManager.setSavePointDir("");
            i = tVariablePackageManagerService.updateTVariablePackageManagerOnApplication(pkManager);
            LOG.info("--------------update applicationId: " + applicationId);
            LOG.info("--------------update jobId: " + jobId);
        }

        return AjaxResult.success(i > 0 ? "success" : "failure");
    }

    // 停止变量包任务
    @PutMapping("/stop")
    public AjaxResult stop(@RequestBody TVariablePackageManager pkManager) {
        // 日志记录
        TVariablePackageOperateLog log = new TVariablePackageOperateLog();
        log.setVariablePackNameEn(pkManager.getVariablePackEn());
        log.setOperateType("变量包停止");
        logService.insertTVariablePackageOperateLog(log);
        LOG.info("----------start----stop---------------");
        // 停止任务
        Map<String, String> map = tVariablePackageManagerService.jobKill(pkManager);
        int i = 0;
        if (map.size() > 0) {
            if ("success".equals(map.get("result"))) { // 成功杀死
                // 不清除 applicationId and jobId
//                pkManager.setApplicationId("");
//                pkManager.setJobId("");
                // 修改状态
                pkManager.setRuningState("0");
                pkManager.setSavePointDir(map.get("savepoint"));
                i = tVariablePackageManagerService.updateTVariablePackageManagerOnApplication(pkManager);
                LOG.info("--------------成功删除---------------");
            } else if ("unsuccess".equals(map.get("result"))) {
                pkManager.setSavePointDir(map.get("savepoint") == null ? "" : map.get("savepoint"));
                tVariablePackageManagerService.updateTVariablePackageManagerOnApplication(pkManager);
            }
        }
        return AjaxResult.success(i > 0 ? "success" : "failure");
    }

    public static void main(String[] args) {
        String s = "Cancelled job 0ee12ec47420bb5aaefaa19237a04a77. Savepoint stored in hdfs://master:9000/flink/savepoints/test100/savepoint-0ee12e-d04eec0d948c.";
        int i = s.indexOf("Savepoint stored in ");
        String substring = s.substring(i + 20, s.length() - 1);
        System.out.println(substring);
    }

    /**
     * 删除变量包管理
     */
    @Log(title = "变量包管理", businessType = BusinessType.DELETE)
    @DeleteMapping("/{variablePackIds}")
    public AjaxResult remove(@PathVariable Long[] variablePackIds) {
        return toAjax(tVariablePackageManagerService.deleteTVariablePackageManagerByIds(variablePackIds));
    }
}
