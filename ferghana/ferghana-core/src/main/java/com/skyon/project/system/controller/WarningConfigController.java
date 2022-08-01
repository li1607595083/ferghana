package com.skyon.project.system.controller;

import com.skyon.framework.web.domain.AjaxResult;
import com.skyon.framework.web.page.TableDataInfo;
import com.skyon.project.system.domain.SysUser;
import com.skyon.project.system.domain.TVariablePackageManager;
import com.skyon.project.system.domain.WarningConfig;
import com.skyon.project.system.service.ISysUserService;
import com.skyon.project.system.service.ITVariablePackageManagerService;
import com.skyon.project.system.service.WarningConfigService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;
import com.skyon.framework.web.controller.BaseController;

import java.util.ArrayList;
import java.util.List;

/**
 * 预警配置
 *
 * @date 2021-06-02
 */
@RestController
@RequestMapping("/warning/config")
public class WarningConfigController extends BaseController
{

    @Autowired
    private WarningConfigService warningConfigService;

    @Autowired
    private ISysUserService userService;

    @Autowired
    private ITVariablePackageManagerService tVariablePackageManagerService;

    /**
     * 查询预警列表
     */
    @PreAuthorize("@ss.hasPermi('warning:config:list')")
    @GetMapping("/list")
    public TableDataInfo list(WarningConfig warningConfig)
    {
        startPage();
        List<WarningConfig> list = warningConfigService.selectWarningConfigList(warningConfig);
        return getDataTable(list);
    }

    /**
     *  获取已经启动的变量包列表
     */
    @GetMapping("/variablepackagelist")
    public TableDataInfo VariablePackageList(TVariablePackageManager tVariablePackageManager){
        List<TVariablePackageManager> arr = tVariablePackageManagerService.selectTVariablePackageManagerList(tVariablePackageManager);
        List<TVariablePackageManager> list = new ArrayList<>();
        for(int i=0;i<arr.size();i++){
            if("1".equals(arr.get(i).getRuningState())){
                list.add(arr.get(i));
            }
        }
        return getDataTable(list);
    }

    /**
     *  启动预警
     */
    @PutMapping("/run")
    public AjaxResult warningRun(@RequestBody WarningConfig warningConfig){
        int i = warningConfigService.runWarningConfig(warningConfig.getWarningId());
        return AjaxResult.success(i > 0 ? "success" : "failure");
    }

    /**
     *  停止预警
     */
    @PutMapping("/stop")
    public AjaxResult warningStop(@RequestBody WarningConfig warningConfig){
        int i = warningConfigService.stopWarningConfig(warningConfig.getWarningId());
        return AjaxResult.success(i > 0 ? "success" : "failure");
    }

    /**
     *  获取预警联系人
     */
    @GetMapping("/getuserlist")
    public TableDataInfo getUserList(WarningConfig warningConfig)
    {
        List<SysUser> list = userService.selectUserList(new SysUser());
        return getDataTable(list);
    }

    /**
     *  添加预警规则
     */
    @PreAuthorize("@ss.hasPermi('warning:config:add')")
    @PostMapping("/add")
    public AjaxResult addWarningConfig(@RequestBody WarningConfig warningConfig)
    {
        int i = warningConfigService.insertWarningConfig(warningConfig);
        return AjaxResult.success(i > 0 ? "success" : "failure");
    }

    /**
     *  修改预警规则
     */
    @PreAuthorize("@ss.hasPermi('warning:config:edit')")
    @PostMapping("/update")
    public AjaxResult updateWarningConfig(@RequestBody WarningConfig warningConfig)
    {
        int i = warningConfigService.updateWarningConfig(warningConfig);
        return AjaxResult.success(i > 0 ? "success" : "failure");
    }

    /**
     *  获取预警详情
     */
    @PreAuthorize("@ss.hasPermi('warning:config:query')")
    @GetMapping(value = "/{warningId}")
    public AjaxResult selectWarningConfigById(@PathVariable("warningId") Long warningId) {
        return AjaxResult.success(warningConfigService.selectWarningConfigById(warningId));
    }

    /**
     *  删除预警规则
     */
    @PreAuthorize("@ss.hasPermi('warning:config:remove')")
    @DeleteMapping(value = "/{warningIds}")
    public AjaxResult deleteWarningConfigByIds(@PathVariable("warningIds") Long[] warningIds){
        int i = warningConfigService.deleteWarningConfigByIds(warningIds);
        return AjaxResult.success(i > 0 ? "success" : "failure");
    }
}
