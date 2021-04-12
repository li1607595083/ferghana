package com.skyon.project.system.controller;

import com.skyon.framework.web.controller.BaseController;
import com.skyon.framework.web.page.TableDataInfo;
import com.skyon.project.system.domain.TVariableOperateLog;
import com.skyon.project.system.domain.TVariablePackageOperateLog;
import com.skyon.project.system.service.ITVariableOperateLogService;
import com.skyon.project.system.service.ITVariablePackageOperateLogService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * 变量管理中心Controller
 *
 * @author zhaoy
 * @date 2020-08-06
 */
@RestController
@RequestMapping("/variablePackage/operate")
public class TVariablePackageOperateLogController extends BaseController {
    @Autowired
    private ITVariablePackageOperateLogService logService;


    /**
     * 查询变量管理中心列表
     */
    @GetMapping("/list/{variablePackEn}")
    public TableDataInfo list(@PathVariable("variablePackEn") String variablePackEn) {
        List<TVariablePackageOperateLog> list = logService.selectTVariablePackageOperateLogList(variablePackEn);
        return getDataTable(list);
    }
}
