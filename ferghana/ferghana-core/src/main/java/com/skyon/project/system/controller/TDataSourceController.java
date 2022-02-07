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
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import java.util.*;

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
//        String markName = tDataSource.getWaterMarkName();
//        if (!Strings.isNullOrEmpty(markName)) {
//            Map map = new HashMap();
//            map.put("key", markName);
//            map.put("value", "TIMESTAMP(3)");
//            list1.add(map);
//        }
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
        fieldService.insertTDatasourceField(tDataSource.getTableName(), tDataSource.getDynamicItem(), "01",tDataSource.getConnectorType());

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
        fieldService.updatefieldName(tDataSource.getDynamicItem(), "01",
                tDataSource.getConnectorType(),tDataSource.getTableName());

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


    @RequestMapping(value = "/paramValidate/{optionalParam}", method = RequestMethod.GET)
    @ResponseBody
    public AjaxResult paramValidate(@PathVariable("optionalParam") String optionalParam) {
        String msg = "";
        String connectorFlag = optionalParam.substring(optionalParam.length() - 2);
        optionalParam = optionalParam.substring(0, optionalParam.length() - 2);

        if (optionalParam.startsWith(",") || optionalParam.endsWith(",")) {
            msg = "参数格式不正确";
            return AjaxResult.success(msg);
        }

        if ("00".equals(connectorFlag)) {
            return AjaxResult.success(msg);
        }

        String optionalParamContent = "{" + optionalParam + "}";
        optionalParamContent = optionalParamContent.replaceAll("\'", "\"");
        optionalParamContent = optionalParamContent.replaceAll("=", ":");
        //数据源表连接参数
        //kafka
        String[] kafkaRequiredParam = {"connector", "topic", "properties.bootstrap.servers", "properties.group.id", "scan.startup.mode", "format"};
        String[] kafkaOptionalParam = {"scan.startup.specific-offsets", "scan.startup.timestamp-millis"};
        //mysql-cdc
        String[] mysqlCdcRequiredParam = {"connector", "hostname", "username", "password", "database-name", "table-name", "port"};
        String[] mysqlCdcOptionalParam = {"server-time-zone", "server-id", "debezium.connect.keep.alive", "debezium.table.ignore.builtin", "debezium.database.ssl.mode", "debezium.binlog.buffer.size", "debezium.snapshot.mode", "debezium.snapshot.locking.mode", "debezium.snapshot.include.collection.list", "debezium.snapshot.select.statement.overrides", "debezium.min.row.count.to.stream.results", "debezium.heartbeat.interval.ms", "debezium.heartbeat.topics.prefix", "debezium.database.initial.statements", "debezium.snapshot.delay.ms", "debezium.snapshot.fetch.size", "debezium.snapshot.lock.timeout.ms", "debezium.enable.time.adjuster", "debezium.source.struct.version", "debezium.sanitize.field.names", "debezium.skipped.operations", "debezium.signal.data.collection", "debezium.incremental.snapshot.chunk.size", "debezium.read.only", "debezium.provide.transaction.metadata"};

        //数据维表连接参数
        //redis
        String[] redisRequiredParam = {"connector", "mode", "single-node", "cluster-nodes"};
        String[] redisOptionalParam = {"password", "database", "hashname", "lookup.cache.max-rows", "lookup.cache.ttl", "lookup.max-retries", "value.format"};
        //oracle and mysql
        String[] oracleAndMysqlRequiredParam = {"connector", "url", "table-name", "driver", "username", "password"};
        String[] oracleAndMysqlOptionalParam = {"lookup.cache.max-rows", "lookup.cache.ttl", "lookup.max-retries"};
        //hbase
        String[] hbaseRequiredParam = {"connector", "table-name", "zookeeper.quorum"};
        String[] hbaseOptionalParam = {"zookeeper.znode.parent"};


        if (StringUtils.isNotEmpty(optionalParam)) {
            //1、先判断是否为json格式；
            if (isJson(optionalParamContent)) {
                JSONObject jsonObject = JSON.parseObject(optionalParamContent);
                Iterator<String> keys = jsonObject.keySet().iterator();
                while (keys.hasNext()) {
                    String key = keys.next();
                    AjaxResult ajaxResult = null;
                    //2、再判断参数是否重复、是否正确；
                    if ("01".equals(connectorFlag)) {
                        ajaxResult = paramJudge(kafkaRequiredParam, kafkaOptionalParam, key);
                    } else if ("02".equals(connectorFlag)) {
                        ajaxResult = paramJudge(mysqlCdcRequiredParam, mysqlCdcOptionalParam, key);
                    } else if ("03".equals(connectorFlag)) {
                        ajaxResult = paramJudge(redisRequiredParam, redisOptionalParam, key);
                    } else if ("04".equals(connectorFlag)) {
                        ajaxResult = paramJudge(oracleAndMysqlRequiredParam, oracleAndMysqlOptionalParam, key);
                    } else if ("05".equals(connectorFlag)) {
                        ajaxResult = paramJudge(hbaseRequiredParam, hbaseOptionalParam, key);
                    }

                    if (null != ajaxResult) {
                        return ajaxResult;
                    }
                }
            } else {
                msg = "参数格式不正确";
            }
        }

        return AjaxResult.success(msg);
    }

    /**
     * 判断可选参数是否正确
     *
     * @param requiredParam
     * @param optionalParam
     * @param key
     * @return
     */
    public AjaxResult paramJudge(String[] requiredParam, String[] optionalParam, String key) {
        String msg;
        if (Arrays.asList(requiredParam).contains(key)) {
            msg = key + " 参数已添加";
        } else if (Arrays.asList(optionalParam).contains(key)) {
            return null;
        } else {
            msg = key + " 参数不存在";
        }
        return AjaxResult.success(msg);
    }

    /**
     * 判断字符串是否为json格式
     *
     * @param content
     * @return
     */
    public static boolean isJson(String content) {
        if (StringUtils.isEmpty(content)) {
            return false;
        }
        boolean isJsonObject = true;
        boolean isJsonArray = true;
        try {
            JSONObject.parseObject(content);
        } catch (Exception e) {
            isJsonObject = false;
        }
        try {
            JSONObject.parseArray(content);
        } catch (Exception e) {
            isJsonArray = false;
        }
        //不是json格式
        if (!isJsonObject && !isJsonArray) {
            return false;
        }
        return true;
    }
}
