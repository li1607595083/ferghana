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
import com.skyon.project.system.domain.TDimensionTable;
import com.skyon.project.system.service.ITDatasourceFieldService;
import com.skyon.project.system.service.ITDimensionTableService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 数据维Controller
 *
 * @date 2020-07-22
 */
@RestController
@RequestMapping("/system/Dimension")
public class TDimensionTableController extends BaseController {
    @Autowired
    private ITDimensionTableService tDimensionTableService;
    @Autowired
    private ITDatasourceFieldService fieldService;

    /**
     * 查询数据维列表
     */
    @PreAuthorize("@ss.hasPermi('source:dimension:list')")
    @GetMapping("/list")
    public TableDataInfo list(TDimensionTable tDimensionTable) {
        startPage();
        List<TDimensionTable> list = tDimensionTableService.selectTDimensionTableList(tDimensionTable);
        return getDataTable(list);
    }

    /**
     * 查询数据维列表
     */
    @GetMapping("/typeQuery")
    public TableDataInfo typeQueryList() {
        List<Map> maps = tDimensionTableService.selectTDimensionTableListGroupByType();
        return getDataTable(maps);
    }

    /**
     * 根据连接器类型获取表名
     */
    @RequestMapping(value = "/tableNameQuery", method = RequestMethod.GET)
    @ResponseBody
    public TableDataInfo getTableNameInfo() {
        List<TDimensionTable> tDimensionTables = tDimensionTableService.selectTDimensionTableByConnType();
        return getDataTable(tDimensionTables);
    }


    /**
     * 获取数据源表的schema字段
     */
    @RequestMapping(value = "/querySchema", method = RequestMethod.GET)
    @ResponseBody
    public TableDataInfo getInfoSchema(@RequestParam("dimensionNames") String dimensionNames) {
        List list = new ArrayList();
        List<TDimensionTable> tdimensions = tDimensionTableService.getTdimensions(dimensionNames);
        if (tdimensions != null && tdimensions.size() > 0) {
            for (int j = 0; j < tdimensions.size(); j++) {
                Map maptmp = new HashMap();
                List list1 = new ArrayList();
                TDimensionTable table = tdimensions.get(j);
                if ("02".equals(table.getConnectorType())) { // jdbc
                    String schemaDefine = table.getSchemaDefine();
                    JSONArray parse = (JSONArray) JSONObject.parse(schemaDefine);
                    for (int i = 0; i < parse.size(); i++) {
                        JSONObject object = (JSONObject) parse.get(i);
                        Map map = new HashMap();
                        map.put("key", object.get("jdbcKey"));
                        map.put("value", object.get("jdbcType"));
                        list1.add(map);
                    }
                    maptmp.put(table.getDimensionName() + "&&" + table.getJdbcPrimaryKey() + "-主键" + "&&"
                            + table.getDimensionName() + "." + table.getJdbcPrimaryKey(), list1);
                } else if ("03".equals(table.getConnectorType())) { // hbase
                    String hbaseSchemaDefine = table.getHbaseSchemaDefine();
                    JSONArray objects = JSON.parseArray(hbaseSchemaDefine);
                    for (int k = 0; k < objects.size(); k++) {
                        JSONObject liezu = (JSONObject) objects.get(k); // 列族
                        JSONObject div1 = (JSONObject) liezu.get("div1");
                        JSONArray hbaseColumnFamilyArray = (JSONArray) div1.get("hbaseColumnItem");
                        JSONObject get0 = (JSONObject) hbaseColumnFamilyArray.get(0);
                        Object hbaseColumnFamily = get0.get("hbaseColumnFamily");
                        JSONObject div2 = (JSONObject) liezu.get("div2");
                        JSONArray hbaseDynamicItemArray = (JSONArray) div2.get("hbaseDynamicItem");
                        for (int i = 0; i < hbaseDynamicItemArray.size(); i++) {
                            JSONObject lie = (JSONObject) hbaseDynamicItemArray.get(i); // 列
                            Object hbaseKey = lie.get("hbaseKey");
                            Object hbaseType = lie.get("hbaseType");
                            Map map = new HashMap();
                            map.put("key", hbaseColumnFamily + "." + hbaseKey);
                            map.put("value", hbaseType);
                            list1.add(map);
                        }
                    }
                    maptmp.put(table.getDimensionName() + "&&" + table.getRowkey() + "-rowkey" + "&&"
                            + table.getDimensionName() + "." + table.getJdbcPrimaryKey(), list1);
                }
                else if("04".equals(table.getConnectorType()) || "05".equals(table.getConnectorType())) { // es
                    String schemaDefine = table.getSchemaDefine();
                    JSONArray parse = (JSONArray) JSONObject.parse(schemaDefine);
                    for (int i = 0; i < parse.size(); i++) {
                        JSONObject object = (JSONObject) parse.get(i);
                        Map map = new HashMap();
                        map.put("key", object.get("esKey"));
                        map.put("value", object.get("esType"));
                        list1.add(map);
                    }
                    maptmp.put(table.getDimensionName() + "&&" + table.getEsPrimaryKey() + "-主键" + "&&"
                            + table.getDimensionName() + "." + table.getEsPrimaryKey(), list1);
                }
                list.add(maptmp);
            }
        }
        return getDataTable(list);
    }


    /**
     * 导出数据维列表
     */
//    @PreAuthorize("@ss.hasPermi('system:Dimension:export')")
    @Log(title = "数据维", businessType = BusinessType.EXPORT)
    @GetMapping("/export")
    public AjaxResult export(TDimensionTable tDimensionTable) {
        List<TDimensionTable> list = tDimensionTableService.selectTDimensionTableList(tDimensionTable);
        ExcelUtil<TDimensionTable> util = new ExcelUtil<TDimensionTable>(TDimensionTable.class);
        return util.exportExcel(list, "table");
    }

    /**
     * 获取数据维详细信息
     */
    @PreAuthorize("@ss.hasPermi('source:dimension:query')")
    @GetMapping(value = "/{dimensionId}")
    public AjaxResult getInfo(@PathVariable("dimensionId") Long dimensionId) {
        return AjaxResult.success(tDimensionTableService.selectTDimensionTableById(dimensionId));
    }

    /**
     * 新增数据维
     */
    @PreAuthorize("@ss.hasPermi('source:dimension:add')")
    @Log(title = "数据维", businessType = BusinessType.INSERT)
    @PostMapping
    @Transactional
    public AjaxResult add(@RequestBody TDimensionTable tDimensionTable) {

        // 新增时，需往t_datasource_field添加字段 , 02 代表 数据维表
        if ("06".equals(tDimensionTable.getConnectorType())) {
            fieldService.insertTDatasourceField(tDimensionTable.getDimensionName(), tDimensionTable.getSchemaDefine(), "02", tDimensionTable.getConnectorType());
        }

        return toAjax(tDimensionTableService.insertTDimensionTable(tDimensionTable));
    }

    /**
     * 修改数据维
     */
    @PreAuthorize("@ss.hasPermi('source:dimension:edit')")
    @Log(title = "数据维", businessType = BusinessType.UPDATE)
    @PutMapping
    public AjaxResult edit(@RequestBody TDimensionTable tDimensionTable) {

        // 修改的字段 同时把t_datasource_field里的字段给修改了 02 : 数据维表
        fieldService.updatefieldName(tDimensionTable.getSchemaDefine(), "02",
                tDimensionTable.getConnectorType(), tDimensionTable.getDimensionName());

        return toAjax(tDimensionTableService.updateTDimensionTable(tDimensionTable));
    }

    /**
     * 删除数据维
     */
    @PreAuthorize("@ss.hasPermi('source:dimension:remove')")
    @Log(title = "数据维", businessType = BusinessType.DELETE)
    @DeleteMapping("/{dimensionIds}")
    @Transactional
    public AjaxResult remove(@PathVariable Long[] dimensionIds) {
        // 先查出来要删的数据维表
        List<TDimensionTable> tables = tDimensionTableService.getTDimensionTableListByIds(dimensionIds);
        String[] tableNames = new String[tables.size()];
        for (int i = 0; i < tables.size(); i++) {
            tableNames[i] = tables.get(i).getDimensionName();
        }
        // 根据tablename 删除 t_datasource_field里的数据 02 :数据维表
        fieldService.deleteTDatasourceField(tableNames,"02");

        return toAjax(tDimensionTableService.deleteTDimensionTableByIds(dimensionIds));
    }
}
