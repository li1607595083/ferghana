package com.skyon.project.system.controller;

import com.skyon.framework.web.controller.BaseController;
import com.skyon.framework.web.page.TableDataInfo;
import com.skyon.project.system.domain.WarningLog;
import com.skyon.project.system.service.WarningLogService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * 预警日志
 *
 * @date 2021-06-07
 */
@RestController
@RequestMapping("/warning/log")
public class WarningLogController extends BaseController {

    @Autowired
    private WarningLogService warningLogService;

    /**
     * 查询预警日志列表
     */
    @PreAuthorize("@ss.hasPermi('warning:log:list')")
    @GetMapping("/list")
    public TableDataInfo list(WarningLog warningLog)
    {
        startPage();
        List<WarningLog> list = warningLogService.selectWarningLogList(warningLog);
        return getDataTable(list);
    }

}
