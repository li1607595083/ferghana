package com.skyon.project.system.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.skyon.common.utils.poi.ExcelUtil;
import com.skyon.framework.aspectj.lang.annotation.Log;
import com.skyon.framework.aspectj.lang.enums.BusinessType;
import com.skyon.framework.web.controller.BaseController;
import com.skyon.framework.web.domain.AjaxResult;
import com.skyon.framework.web.page.TableDataInfo;
import com.skyon.project.system.domain.TDataSource;
import com.skyon.project.system.service.ITDataSourceService;
import com.skyon.project.system.service.ITDatasourceFieldService;
import joptsimple.internal.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 数据源表 Controller
 *
 * @date 2020-05-21
 */
@RestController
@RequestMapping("/source/manage")
public class TDataSourceController extends BaseController {
    @Autowired
    private ITDataSourceService tDataSourceService;

    @Autowired
    private ITDatasourceFieldService fieldService;

    /**
     * 查询数据源表列表
     */
    @PreAuthorize("@ss.hasPermi('source:manage:list')")
    @GetMapping("/list")
    public TableDataInfo list(TDataSource tDataSource) {
        startPage();
        List<TDataSource> list = tDataSourceService.selectTDataSourceList(tDataSource);
        return getDataTable(list);
    }

    /**
     * 导出数据源表列表
     */
    @PreAuthorize("@ss.hasPermi('source:manage:export')")
    @Log(title = "【请填写功能名称】", businessType = BusinessType.EXPORT)
    @GetMapping("/export")
    public AjaxResult export(TDataSource tDataSource) {
        List<TDataSource> list = tDataSourceService.selectTDataSourceList(tDataSource);
        ExcelUtil<TDataSource> util = new ExcelUtil<TDataSource>(TDataSource.class);
        return util.exportExcel(list, "source");
    }

    /**
     * 获取数据源表详细信息
     */
    @PreAuthorize("@ss.hasPermi('source:manage:query')")
    @GetMapping(value = "/{dataSourceId}")
    public AjaxResult getInfo(@PathVariable("dataSourceId") Long dataSourceId) {
        return AjaxResult.success(tDataSourceService.selectTDataSourceById(dataSourceId));
    }

    /**
     * 获取数据源表的schema字段
     */
    @RequestMapping(value = "/querySchema", method = RequestMethod.GET)
    @ResponseBody
    public TableDataInfo getInfoSchema(@RequestParam("dataSourceName") String dataSourceName) {
        List list = new ArrayList();
        List list1 = new ArrayList();
        // 字段
        TDataSource tDataSource = tDataSourceService.selectTDataSourceByTableName(dataSourceName);
        String schemaDefine = tDataSource.getSchemaDefine();
        JSONArray parse = (JSONArray) JSONObject.parse(schemaDefine);
        for (int i = 0; i < parse.size(); i++) {
            JSONObject object = (JSONObject) parse.get(i);
            Map map = new HashMap();
            map.put("key", object.get("schemaDefine"));
            map.put("value", object.get("dataBaseType"));
            list1.add(map);
        }
        String markName = tDataSource.getWaterMarkName();
        if (!Strings.isNullOrEmpty(markName)) {
            Map map = new HashMap();
            map.put("key", markName);
            map.put("value", "TIMESTAMP(3)");
            list1.add(map);
        }
        // 水印 主键
        List list2 = new ArrayList();
        String waterMarkName = tDataSource.getWaterMarkName();
        String schemaPrimaryKey = tDataSource.getSchemaPrimaryKey();
        list2.add(waterMarkName);
        list2.add(schemaPrimaryKey);
        list.add(list1);
        list.add(list2);

        return getDataTable(list);
    }


    /**
     * 新增数据源表
     */
    @PreAuthorize("@ss.hasPermi('source:manage:add')")
    @Log(title = "【请填写功能名称】", businessType = BusinessType.INSERT)
    @PostMapping
    @Transactional
    public AjaxResult add(@RequestBody TDataSource tDataSource) {

        // 新增时，需往t_datasource_field添加字段 , 01 代表 数据源表
        fieldService.insertTDatasourceField(tDataSource.getTableName(), tDataSource.getDynamicItem(), "01");

        return toAjax(tDataSourceService.insertTDataSource(tDataSource));
    }

    /**
     * 修改数据源表
     */
    @PreAuthorize("@ss.hasPermi('source:manage:edit')")
    @Log(title = "【请填写功能名称】", businessType = BusinessType.UPDATE)
    @PutMapping
    @Transactional
    public AjaxResult edit(@RequestBody TDataSource tDataSource) {

        // 修改的字段 同时把t_datasource_field里的字段给修改了 01 : 数据源表
        fieldService.updatefieldName(tDataSource.getDynamicItem(), "01");

        return toAjax(tDataSourceService.updateTDataSource(tDataSource));
    }

    /**
     * 删除数据源表
     */
    @PreAuthorize("@ss.hasPermi('source:manage:remove')")
    @Log(title = "【请填写功能名称】", businessType = BusinessType.DELETE)
    @DeleteMapping("/{dataSourceIds}")
    public AjaxResult remove(@PathVariable Long[] dataSourceIds) {
        // 先查询tableNames
        List<TDataSource> tDataSources = tDataSourceService.selectTDataSourceListByIds(dataSourceIds);
        String[] tableNames = new String[tDataSources.size()];
        for (int i = 0; i < tDataSources.size(); i++) {
            tableNames[i] = tDataSources.get(i).getTableName();
        }
        // 根据tablename 删除 t_datasource_field里的数据 01 :数据源表
        fieldService.deleteTDatasourceField(tableNames, "01");

        return toAjax(tDataSourceService.deleteTDataSourceByIds(dataSourceIds));
    }
}
