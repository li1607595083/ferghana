package com.skyon.project.system.controller;

import com.skyon.framework.web.controller.BaseController;
import com.skyon.framework.web.page.TableDataInfo;
import com.skyon.project.system.domain.TVariableOperateLog;
import com.skyon.project.system.service.ITVariableOperateLogService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
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
@RequestMapping("/variable/operate")
public class TVariableOperateLogController extends BaseController {
    @Autowired
    private ITVariableOperateLogService itVariableOperateLogService;


    /**
     * 查询变量管理中心列表
     */
    @GetMapping("/list")
    public TableDataInfo list(TVariableOperateLog tVariableOperateLog) {
        List<TVariableOperateLog> list = itVariableOperateLogService.selectTVariableOperateLogList(tVariableOperateLog);
        return getDataTable(list);
    }
}
