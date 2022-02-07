package com.skyon.project.system.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.skyon.common.utils.poi.ExcelUtil;
import com.skyon.framework.aspectj.lang.annotation.Log;
import com.skyon.framework.aspectj.lang.enums.BusinessType;
import com.skyon.framework.web.controller.BaseController;
import com.skyon.framework.web.domain.AjaxResult;
import com.skyon.framework.web.page.TableDataInfo;
import com.skyon.project.system.domain.TVariableCenter;
import com.skyon.project.system.domain.TVariableOperateLog;
import com.skyon.project.system.service.*;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.optimizer.Optimizer.LOG;

/**
 * 变量管理中心Controller
 *
 * @author zhaoy
 * @date 2020-08-06
 */
@RestController
@RequestMapping("/variable/manager")
public class TVariableCenterController extends BaseController {
    @Autowired
    private ITVariableCenterService tVariableCenterService;

    @Autowired
    private ITVariablePackageManagerService tVariablePackageManagerService;

    @Autowired
    private ITVariableOperateLogService logService;

    @Autowired
    private ITDatasourceFieldService datasourceFieldService;

    @Autowired
    private ITdimensionFieldService tdimensionFieldService;

    /**
     * 查询变量管理中心列表
     */
    @GetMapping("/list")
    @PreAuthorize("@ss.hasPermi('variable:variable:list')")
    public TableDataInfo list(TVariableCenter tVariableCenter) {
        startPage();
        List<TVariableCenter> list = tVariableCenterService.selectTVariableCenterList(tVariableCenter);
        return getDataTable(list);
    }

    /**
     * 查询变量管理中心列表
     */
    @GetMapping("/listVersion")
    public TableDataInfo listVersion(TVariableCenter tVariableCenter) {
        List<TVariableCenter> list = tVariableCenterService.selectVariableVersionList(tVariableCenter);
        return getDataTable(list);
    }

    // 分组查询变量中心
    @GetMapping("/listGroup")
    public TableDataInfo listGroup(TVariableCenter tVariableCenter) {
        List<TVariableCenter> list = tVariableCenterService.selectTVariableCenterList(tVariableCenter);
        return getDataTable(list);
    }

    /**
     * 导出变量管理中心列表
     */
//    @PreAuthorize("@ss.hasPermi('variable:manager:export')")
    @Log(title = "变量管理中心", businessType = BusinessType.EXPORT)
    @GetMapping("/export")
    public AjaxResult export(TVariableCenter tVariableCenter) {
        List<TVariableCenter> list = tVariableCenterService.selectTVariableCenterList(tVariableCenter);
        ExcelUtil<TVariableCenter> util = new ExcelUtil<TVariableCenter>(TVariableCenter.class);
        return util.exportExcel(list, "center");
    }

    /**
     * 获取变量管理中心详细信息
     */
    @PreAuthorize("@ss.hasPermi('variable:variable:query')")
    @GetMapping(value = "/{variableId}")
    public AjaxResult getInfo(@PathVariable("variableId") Long variableId) {
        return AjaxResult.success(tVariableCenterService.selectTVariableCenterById(variableId));
    }

    /**
     * 新增变量管理中心
     */
    @PreAuthorize("@ss.hasPermi('variable:variable:add')")
    @Log(title = "变量管理中心", businessType = BusinessType.INSERT)
    @PostMapping
    public AjaxResult add(@RequestBody TVariableCenter tVariableCenter) {

        TVariableOperateLog log = new TVariableOperateLog();
        log.setVariableNameEn(tVariableCenter.getVariableNameEn());
        log.setOperateType("新增");
        logService.insertTVariableOperateLog(log);

        // 新增的时候，记录数据源表已使用的字段
        datasourceFieldService.updateFiledIsUsed(tVariableCenter);

        // 新增的版本为 1.0
        tVariableCenter.setVersionNum("1.0");
        tVariableCenter.setVersionShow("1");
        return toAjax(tVariableCenterService.insertTVariableCenter(tVariableCenter));
    }

    /**
     * 修改变量管理中心或者版本升级
     */
    @PreAuthorize("@ss.hasPermi('variable:variable:edit')")
    @Log(title = "变量管理中心", businessType = BusinessType.UPDATE)
    @PutMapping
    @Transactional
    public AjaxResult edit(@RequestBody TVariableCenter tVariableCenter) {
        TVariableCenter old = tVariableCenterService.selectTVariableCenterVersion(tVariableCenter);
        boolean b = compareVariable(tVariableCenter, old);
        int num = 0;
        if (b) { // 修改
            num = tVariableCenterService.updateTVariableCenter(tVariableCenter);
        } else { // 版本升级
            // 修改以前版本
            tVariableCenterService.updateVersionShow(old);
            // 新增版本
            BigDecimal bigDecimal = new BigDecimal(old.getVersionNum()).add(new BigDecimal("1.0"));
            tVariableCenter.setVersionNum(bigDecimal.toString());
            tVariableCenter.setVersionShow("1");
            num = tVariableCenterService.insertTVariableCenter(tVariableCenter);
            // 新增日志
            TVariableOperateLog log = new TVariableOperateLog();
            log.setVariableNameEn(old.getVariableNameEn());
            log.setOperateType("修改");
            logService.insertTVariableOperateLog(log);

            // 修改的时候，记录使用的字段
            datasourceFieldService.updateFieldAndIsUsed(tVariableCenter);
        }
        return toAjax(num);
    }

    // 比较2个变量
    private boolean compareVariable(TVariableCenter newVa, TVariableCenter oldVa) {
        boolean flag = false;
        if (newVa.getVariableType().equals("01")) { // 基础变量
            if (newVa.getVariableModelType().equals("01")) { // 普通查询
                if ((newVa.getRedisKey() == null && oldVa.getRedisKey() == null)
                        || (newVa.getRedisKey() != null && newVa.getRedisKey().equals(oldVa.getRedisKey()))) { // key
                    if ((newVa.getRedisSelfFunctionItem() == null && oldVa.getRedisSelfFunctionItem() == null)
                            || (newVa.getRedisSelfFunctionItem() != null && JSON.toJSONString(newVa.getRedisSelfFunctionItem()).equals(JSON.toJSONString(oldVa.getRedisSelfFunctionItem())))) { // redis作用函数
                        if ((newVa.getRedisValue() == null && oldVa.getRedisValue() == null)
                                || (newVa.getRedisValue() != null && newVa.getRedisValue().equals(oldVa.getRedisValue()))) { // redis value
                            if ((newVa.getRedisFunction() == null && oldVa.getRedisFunction() == null)
                                    || (newVa.getRedisFunction() != null && newVa.getRedisFunction().equals(oldVa.getRedisFunction()))) { // redis方法
                                if ((newVa.getRedisAddr() == null && oldVa.getRedisAddr() == null)
                                        || (newVa.getRedisAddr() != null && newVa.getRedisAddr().equals(oldVa.getRedisAddr()))) { // redis集群名
                                    if ((newVa.getStatisticsConditions() == null && oldVa.getStatisticsConditions() == null)
                                            || (newVa.getStatisticsConditions() != null && newVa.getStatisticsConditions().equals(oldVa.getStatisticsConditions()))) { // redis条件
                                        flag = true; // 都没变
                                    }
                                }
                            }
                        }
                    }
                }
            } else if (newVa.getVariableModelType().equals("02")) { // 统计查询
                if (newVa.getVariableFactor().equals(oldVa.getVariableFactor())
                        && newVa.getStatisticsCountModel().equals(oldVa.getStatisticsCountModel())) { // 变量因子  统计计算模板
                    if ((newVa.getStatisticsModel() == null && oldVa.getStatisticsModel() == null)
                            || (newVa.getStatisticsModel() != null && newVa.getStatisticsModel().equals(oldVa.getStatisticsModel()))) { // 统计周期
                        if ((newVa.getStatisticsNum() == null && oldVa.getStatisticsNum() == null)
                                || (newVa.getStatisticsNum() != null && newVa.getStatisticsNum().equals(oldVa.getStatisticsNum()))) { // 统计周期
                            if ((newVa.getStatisticsCycle() == null && oldVa.getStatisticsCycle() == null)
                                    || (newVa.getStatisticsCycle() != null && newVa.getStatisticsCycle().equals(oldVa.getStatisticsCycle()))) { // 统计周期
                                if ((newVa.getStatisticsConditions() == null && oldVa.getStatisticsConditions() == null)
                                        || (newVa.getStatisticsConditions() != null && newVa.getStatisticsConditions().equals(oldVa.getStatisticsConditions()))) { // 组合条件
                                        flag = true; // 都没变
                                }
                            }
                        }
                    }
                }
            } else if (newVa.getVariableModelType().equals("03")) { // 数据加工
                if ((newVa.getProcessModel() == null && oldVa.getProcessModel() == null)
                        || (newVa.getProcessModel() != null && newVa.getProcessModel().equals(oldVa.getProcessModel()))) { // 作用函数
                    if ((newVa.getProcessInputParams() == null && oldVa.getProcessInputParams() == null)
                            || (newVa.getProcessInputParams() != null && newVa.getProcessInputParams().equals(oldVa.getProcessInputParams()))) { // 作用函数
                        flag = true; // 都没变
                    }
                }
            } else if (newVa.getVariableModelType().equals("04")) { // 自定义查询
                if ((newVa.getUserDefinedSql() == null && oldVa.getUserDefinedSql() == null)
                        || (newVa.getUserDefinedSql() != null && newVa.getUserDefinedSql().equals(oldVa.getUserDefinedSql()))) { // 作用函数
                    flag = true; // 都没变
                }
            }

        } else if (newVa.getVariableType().equals("02")) { // 派生变量
            if ((newVa.getDeriveVariableSql() == null && oldVa.getDeriveVariableSql() == null)
                    || (newVa.getDeriveVariableSql() != null && newVa.getDeriveVariableSql().equals(oldVa.getDeriveVariableSql()))) { // 派生变量
                flag = true; // 都没变
            }
        }
        return flag;
    }

    /**
     * 测试变量管理中心
     */
    @PostMapping("/test")
    public AjaxResult test(@RequestBody TVariableCenter tVariableCenter) {
        System.out.println("-------------------------1");
        //根据变量分类-数据源表-主键
        Map mapValue = tVariablePackageManagerService.getKeyByVariableId(tVariableCenter.getVariableClassification());
        LOG.info("----2:" + mapValue);
        String millis = "topic" + System.currentTimeMillis();
        // 组装测试参数
        String variableTest = tVariableCenterService.variableTest(tVariableCenter, mapValue, millis);

        LOG.info("----3:" + variableTest);
        // 获取测试结果
        List result = tVariableCenterService.testRun(variableTest, millis);
        LOG.info("----4:" + result);
        LOG.info("----5");
        // 顺序
        ArrayList sourceTableValue = (ArrayList) tVariableCenter.getSourceTableValue();
        String s = "";
        List list = new ArrayList();
        if (result.size() > 0) {
            for (int i = 0; i < sourceTableValue.size(); i++) {
                for (int j = 0; j < result.size(); j++) {
                    Map map = new HashMap();
                    Map o = (Map) sourceTableValue.get(i); // 页面的
                    Object o2 = result.get(j); // 结果的
                    JSONObject jsonObject = JSON.parseObject(o2.toString());
                    Object tmp2 = jsonObject.get(tVariableCenter.getSourceKey());
                    Object tmp = o.get(tVariableCenter.getSourceKey());  // 主键的值
                    if (tmp.equals(tmp2)) {
                        map.put(tVariableCenter.getSourceKey(), tmp);
                        map.put(tVariableCenter.getVariableNameEn(), jsonObject.get(tVariableCenter.getVariableNameEn()));
                        list.add(map);
                    }
                }
            }
            s = JSON.toJSONString(list);
        } else {
            s = "no success";
        }
        // 记录日志
        TVariableOperateLog log = new TVariableOperateLog();
        log.setVariableNameEn(tVariableCenter.getVariableNameEn());
        log.setOperateType("测试");
        logService.insertTVariableOperateLog(log);
        // 值
        LOG.info("----result:" + s);
        return AjaxResult.success(s);
    }


    // 根据变量引文名获取其 测试的数据源表字段
    @PostMapping("/testCol")
    public TableDataInfo getTestColByName(String names) {
        System.out.println("--------------" + names);
        return getDataTable(tVariableCenterService.getCol(names));
    }


    /**
     * 删除变量管理中心
     */
    @PreAuthorize("@ss.hasPermi('variable:variable:remove')")
    @Log(title = "变量管理中心", businessType = BusinessType.DELETE)
    @DeleteMapping("/{variableIds}")
    @Transactional
    public AjaxResult remove(@PathVariable Long[] variableIds) {

        // 删除的时候，把 t_datasource_field 对应的字段 更新掉
        List<TVariableCenter> tVariableCenters = tVariableCenterService.selectVariableCenterByIds(variableIds);
        for (int i = 0; i < tVariableCenters.size(); i++) {
            TVariableCenter tVariableCenter = tVariableCenters.get(i);
            datasourceFieldService.updateIsUsed(tVariableCenter);
        }

        return toAjax(tVariableCenterService.deleteTVariableCenterByIds(variableIds));
    }
}
