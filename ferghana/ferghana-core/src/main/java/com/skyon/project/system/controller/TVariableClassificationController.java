package com.skyon.project.system.controller;

import java.util.List;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.beans.factory.annotation.Autowired;
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
import com.skyon.project.system.domain.TVariableClassification;
import com.skyon.project.system.service.ITVariableClassificationService;
import com.skyon.framework.web.controller.BaseController;
import com.skyon.framework.web.domain.AjaxResult;
import com.skyon.common.utils.poi.ExcelUtil;
import com.skyon.framework.web.page.TableDataInfo;

/**
 * 变量分类Controller
 * 
 *
 * @date 2020-08-21
 */
@RestController
@RequestMapping("/variable/classification")
public class TVariableClassificationController extends BaseController
{
    @Autowired
    private ITVariableClassificationService tVariableClassificationService;

    /**
     * 查询变量分类列表
     */
    @GetMapping("/list")
    public TableDataInfo list(TVariableClassification tVariableClassification)
    {
        startPage();
        List<TVariableClassification> list = tVariableClassificationService.selectTVariableClassificationList(tVariableClassification);
        return getDataTable(list);
    }

    /**
     * 导出变量分类列表
     */
//    @PreAuthorize("@ss.hasPermi('system:classification:export')")
    @Log(title = "变量分类", businessType = BusinessType.EXPORT)
    @GetMapping("/export")
    public AjaxResult export(TVariableClassification tVariableClassification)
    {
        List<TVariableClassification> list = tVariableClassificationService.selectTVariableClassificationList(tVariableClassification);
        ExcelUtil<TVariableClassification> util = new ExcelUtil<TVariableClassification>(TVariableClassification.class);
        return util.exportExcel(list, "classification");
    }

    /**
     * 获取变量分类详细信息
     */
    @GetMapping(value = "/{variableClassificationId}")
    public AjaxResult getInfo(@PathVariable("variableClassificationId") Long variableClassificationId)
    {
        return AjaxResult.success(tVariableClassificationService.selectTVariableClassificationById(variableClassificationId));
    }

    /**
     * 新增变量分类
     */
//    @PreAuthorize("@ss.hasPermi('system:classification:add')")
    @Log(title = "变量分类", businessType = BusinessType.INSERT)
    @PostMapping
    public AjaxResult add(@RequestBody TVariableClassification tVariableClassification)
    {
        return toAjax(tVariableClassificationService.insertTVariableClassification(tVariableClassification));
    }

    /**
     * 修改变量分类
     */
    @Log(title = "变量分类", businessType = BusinessType.UPDATE)
    @PutMapping
    public AjaxResult edit(@RequestBody TVariableClassification tVariableClassification)
    {
        return toAjax(tVariableClassificationService.updateTVariableClassification(tVariableClassification));
    }

    /**
     * 删除变量分类
     */
    @Log(title = "变量分类", businessType = BusinessType.DELETE)
	@DeleteMapping("/{variableClassificationIds}")
    public AjaxResult remove(@PathVariable Long[] variableClassificationIds)
    {
        return toAjax(tVariableClassificationService.deleteTVariableClassificationByIds(variableClassificationIds));
    }
}
