package com.skyon.project.system.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Strings;
import com.skyon.common.utils.DateUtils;
import com.skyon.common.utils.SecurityUtils;
import com.skyon.common.utils.StringUtils;
import com.skyon.framework.aspectj.lang.annotation.DataScope;
import com.skyon.project.system.domain.TDatasourceField;
import com.skyon.project.system.domain.TDimensionTable;
import com.skyon.project.system.mapper.TDatasourceFieldMapper;
import com.skyon.project.system.mapper.TDimensionTableMapper;
import com.skyon.project.system.service.ITDimensionTableService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.security.Security;
import java.util.*;

/**
 * 数据维Service业务层处理
 *
 * @date 2020-07-22
 */
@Service
public class TDimensionTableServiceImpl implements ITDimensionTableService {
    @Autowired
    private TDimensionTableMapper tDimensionTableMapper;

    @Autowired
    private TDatasourceFieldMapper fieldMapper;


    public List<TDimensionTable> getTdimensions(String names) {
        String[] split = names.split(",");
        List<TDimensionTable> listByNames = new ArrayList<>();
        if (split != null && split.length > 0) {
            listByNames = tDimensionTableMapper.getTDimensionTableListByNames(split);
            if (listByNames != null && listByNames.size() > 0) {
                for (int i = 0; i < listByNames.size(); i++) {
                    TDimensionTable table = listByNames.get(i);
                    joinSchema(table);
                }
            }
        }
        return listByNames;
    }


    private void joinSchema(TDimensionTable tDimensionTable) {
        String redisSchemaDefine = tDimensionTable.getRedisSchemaDefine();
        if (!Strings.isNullOrEmpty(redisSchemaDefine)) {
            JSONArray parse = (JSONArray) JSONObject.parse(redisSchemaDefine);
            tDimensionTable.setRedisDynamicItem(parse.toArray());
        }
//        String redisSchemaDefine = tDimensionTable.getRedisKey();
//        if (!Strings.isNullOrEmpty(redisSchemaDefine)) {
//            List parse = (List) JSONObject.parse(redisSchemaDefine);
//            Object[] oejectArr = new Object[parse.size()];
//            final int[] j = {0};
//            for (int i = 0; i < parse.size(); i++) {
//                Map o = (Map) parse.get(i);
//                o.forEach((key, Value) -> {
//                    HashMap<String, Object> hashMap = new HashMap<>();
//                    hashMap.put("redisKey", key);
//                    hashMap.put("redisKeyField", Value);
//                    oejectArr[j[0]] = hashMap;
//                    j[0] = j[0] + 1;
//                });
//
//            }
//            tDimensionTable.setRedisDynamicItem(oejectArr);
//        }

        // 转换 jdbc 的json
        if ("02".equals(tDimensionTable.getConnectorType())) {
            // 查询数据源表字段的使用情况
            List<TDatasourceField> tDatasourceFields = fieldMapper.
                    selectTDatasourceFieldByName(tDimensionTable.getDimensionName(), "02");

            String jdbcSchemaDefine = tDimensionTable.getSchemaDefine();
            String jdbcPrimaryKey = tDimensionTable.getJdbcPrimaryKey();

            JSONArray array = JSON.parseArray(jdbcSchemaDefine);
            for (int i = 0; i < array.size(); i++) {
                JSONObject o = (JSONObject) array.get(i);
                Object schemaDefine = o.get("jdbcKey");
                if (tDatasourceFields != null) {
                    for (int j = 0; j < tDatasourceFields.size(); j++) {
                        TDatasourceField tDatasourceField = tDatasourceFields.get(i);
                        String fieldName = tDatasourceField.getFieldName();
                        if (schemaDefine.equals(fieldName)) {
                            o.put("isUsed", tDatasourceField.getIsUsed());
                            o.put("fieldId", tDatasourceField.getId());
                            break;
                        }
                    }
                }
            }
            tDimensionTable.setJdbcDynamicItem(array.toArray());
        }

        // 转换 es 的json
        else if ("04".equals(tDimensionTable.getConnectorType()) || "05".equals(tDimensionTable.getConnectorType())) {
            // 查询数据源表字段的使用情况
            List<TDatasourceField> tDatasourceFields = fieldMapper.
                    selectTDatasourceFieldByName(tDimensionTable.getDimensionName(), "02");

            String esSchemaDefine = tDimensionTable.getSchemaDefine();
            String esPrimaryKey = tDimensionTable.getJdbcPrimaryKey();

            JSONArray array = JSON.parseArray(esSchemaDefine);
            for (int i = 0; i < array.size(); i++) {
                JSONObject o = (JSONObject) array.get(i);
                if (!Strings.isNullOrEmpty(esPrimaryKey)) {
                    if (esPrimaryKey.equals(o.get("esKey"))) {
                        o.put("primaryKey", o.get("esKey"));
                    } else {
                        o.put("primaryKey", "");
                    }
                } else {
                    o.put("primaryKey", "");
                }
                Object schemaDefine = o.get("esKey");
                if (tDatasourceFields != null) {
                    for (int j = 0; j < tDatasourceFields.size(); j++) {
                        TDatasourceField tDatasourceField = tDatasourceFields.get(i);
                        String fieldName = tDatasourceField.getFieldName();
                        if (schemaDefine.equals(fieldName)) {
                            o.put("isUsed", tDatasourceField.getIsUsed());
                            o.put("fieldId", tDatasourceField.getId());
                            break;
                        }
                    }
                }
            }
            tDimensionTable.setEsDynamicItem(array.toArray());
        }


//        if (tDatasourceFields != null) {
//            for (int j = 0; j < array.size(); j++) {
//                JSONObject o = (JSONObject) array.get(j);
//                Object schemaDefine = o.get("schemaDefine");
//                for (int i = 0; i < tDatasourceFields.size(); i++) {
//                    TDatasourceField tDatasourceField = tDatasourceFields.get(i);
//                    String fieldName = tDatasourceField.getFieldName();
//                    if (schemaDefine.equals(fieldName)) {
//                        o.put("isUsed",tDatasourceField.getIsUsed());
//                        o.put("fieldId",tDatasourceField.getId());
//                        break;
//                    }
//                }
//            }
//        }


//        if (!Strings.isNullOrEmpty(jdbcSchemaDefine)) {
//            List parse = (List) JSONObject.parse(jdbcSchemaDefine);
//            Object[] oejectArr = new Object[parse.size()];
//            final int[] j = {0};
//            for (int i = 0; i < parse.size(); i++) {
//                Map o = (Map) parse.get(i);
//                o.forEach((key, Value) -> {
//                    HashMap<String, Object> hashMap = new HashMap<>();
//                    hashMap.put("jdbcKey", key);
//                    hashMap.put("jdbcType", Value);
//                    if (!Strings.isNullOrEmpty(jdbcPrimaryKey)) {
//                        if (jdbcPrimaryKey.equals(key)) {
//                            hashMap.put("primaryKey", key);
//                        } else {
//                            hashMap.put("primaryKey", "");
//                        }
//                    }
//                    oejectArr[j[0]] = hashMap;
//                    j[0] = j[0] + 1;
//                });
//
//            }
//            tDimensionTable.setJdbcDynamicItem(oejectArr);
//        }
        // 转换 hbase 的json
        String hbaseSchemaDefine = tDimensionTable.getHbaseSchemaDefine();
        if (!Strings.isNullOrEmpty(hbaseSchemaDefine)) {
            JSONArray parse = (JSONArray) JSONObject.parse(hbaseSchemaDefine);
            tDimensionTable.setHbaseItem(parse.toArray());
//            if (parse != null && parse.size() > 0) {
//                for (int i = 0; i < parse.size(); i++) {
//                    JSONObject jb = (JSONObject) parse.get(i);
//                    JSONObject div1 = (JSONObject)jb.get("div1");
//                    JSONArray hbaseColumnItem = (JSONArray) div1.get("hbaseColumnItem");
//                    for (int j = 0; j < hbaseColumnItem.size(); j++) {
//                        JSONObject hci = (JSONObject)hbaseColumnItem.get(j);
//                        Object hbaseColumnFamily = hci.get("hbaseColumnFamily");
//
//                        System.out.println("");
//                    }
//                    JSONObject div2 = (JSONObject)jb.get("div2");
//                    JSONArray hbaseDynamicItem = (JSONArray) div2.get("hbaseDynamicItem");
//                    for (int j = 0; j < hbaseDynamicItem.size(); j++) {
//                        JSONObject hdi = (JSONObject)hbaseDynamicItem.get(j);
//                        Object hbaseKey = hdi.get("hbaseKey");
//                        Object hbaseType = hdi.get("hbaseType");
//                        System.out.println("");
//                    }
//
//                }
//            }
//            System.out.println("");
        }
    }

    /**
     * 查询数据维
     *
     * @param dimensionId 数据维ID
     * @return 数据维
     */
    @Override
    public TDimensionTable selectTDimensionTableById(Long dimensionId) {
        // 转换redis的json
        TDimensionTable tDimensionTable = tDimensionTableMapper.selectTDimensionTableById(dimensionId);
        joinSchema(tDimensionTable);
        return tDimensionTable;
    }

    /**
     * 查询数据维列表
     *
     * @param tDimensionTable 数据维
     * @return 数据维
     */
    @Override
    @DataScope(serviceTable = true)
    public List<TDimensionTable> selectTDimensionTableList(TDimensionTable tDimensionTable) {
        return tDimensionTableMapper.selectTDimensionTableList(tDimensionTable);
    }

    @Override
    public List<Map> selectTDimensionTableListGroupByType() {
        return tDimensionTableMapper.selectTDimensionTableListGroupByType();
    }

    /**
     * 新增数据维
     *
     * @param tDimensionTable 数据维
     * @return 结果
     */
    @Override
    public int insertTDimensionTable(TDimensionTable tDimensionTable) {
        tDimensionTable.setCreateTime(DateUtils.getNowDate());
        tranSchemaJson(tDimensionTable);
        joinDimensionTableSql(tDimensionTable);
        tDimensionTable.setCreateBy(SecurityUtils.getUsername());
        tDimensionTable.setCreateId(SecurityUtils.getUserId());
        return tDimensionTableMapper.insertTDimensionTable(tDimensionTable);
    }

    // 拼接建表sql
    private void joinDimensionTableSql(TDimensionTable table) {
        String connectorType = table.getConnectorType();
        StringBuilder sb = new StringBuilder();
        if ("02".equals(connectorType)) { // jdbc
            sb.append(" CREATE TABLE `").append(table.getDimensionName())
                    .append("` ( ").append(schemaTransform(table.getSchemaDefine(), connectorType))
                    .append(" ,PRIMARY KEY (`").append(table.getJdbcPrimaryKey()).append("`)").append(" NOT ENFORCED) WITH ")
                    .append(" ('connector' = 'jdbc', 'url' = '").append(table.getJdbcUrlAddress()).append("',")
                    .append("'driver' = '").append(table.getJdbcDrive()).append("',")
                    .append("'username' = '").append(table.getJdbcUserName()).append("',")
                    .append("'password' = '").append(table.getJdbcUserPwd()).append("',")
                    .append("'table-name' = '").append(table.getDimensionName()).append("')");
            table.setJdbcCreateSql(sb.toString());
        } else if ("03".equals(connectorType)) { // hbase
            String[] split = table.getDimensionName().split(":");
            sb.append(" CREATE TABLE `").append(split[1]).append("`(`").append(table.getRowkey()).append("` STRING, ");
            String hbaseSchemaDefine = table.getHbaseSchemaDefine();
            JSONArray objects = JSON.parseArray(hbaseSchemaDefine);
            StringBuilder sb2 = new StringBuilder();
            for (int i = 0; i < objects.size(); i++) {
                JSONObject liezu = (JSONObject) objects.get(i); // 列族
                JSONObject div1 = (JSONObject) liezu.get("div1");
                JSONArray hbaseColumnFamilyArray = (JSONArray) div1.get("hbaseColumnItem");
                JSONObject get0 = (JSONObject) hbaseColumnFamilyArray.get(0);
                Object hbaseColumnFamily = get0.get("hbaseColumnFamily");
                JSONObject div2 = (JSONObject) liezu.get("div2");
                JSONArray hbaseDynamicItemArray = (JSONArray) div2.get("hbaseDynamicItem");
                sb2.append("'" + hbaseColumnFamily).append("' ROW(");
                String s = "";
                for (int j = 0; j < hbaseDynamicItemArray.size(); j++) {
                    JSONObject lie = (JSONObject) hbaseDynamicItemArray.get(j); // 列
                    Object hbaseKey = "`" + lie.get("hbaseKey") + "`";
                    Object hbaseType = lie.get("hbaseType");
                    s = s + hbaseKey + " " + hbaseType + ",";
                }
                sb2.append(s.substring(0, s.length() - 1)).append("),");

            }
            sb.append(sb2);
            sb.append(" primary key (").append(table.getRowkey())
                    .append(") NOT ENFORCED ) with ( 'connector' = 'hbase-1.4','table-name' = '")
                    .append(table.getHbaseTableName()).append("',")
                    .append("'zookeeper.quorum' = '").append(table.getZookeeperAddress()).append("')");
            table.setHbaseCreateSql(sb.toString());

        } else if ("04".equals(connectorType)) {
            sb.append(" CREATE TABLE `").append(table.getDimensionName())
                    .append("` ( ").append(schemaTransform(table.getSchemaDefine(), connectorType))
                    .append(" ,PRIMARY KEY (`").append(table.getEsPrimaryKey()).append("`)").append(" NOT ENFORCED) WITH ")
                    .append(" ('connector' = 'elasticsearch-6', 'hosts' = '").append(table.getEsUrlAddress()).append("',")
                    .append("'index' = '").append(table.getEsIndex()).append("')");
            table.setEsCreateSql(sb.toString());
        } else if ("05".equals(connectorType)) {
            sb.append(" CREATE TABLE `").append(table.getDimensionName())
                    .append("` ( ").append(schemaTransform(table.getSchemaDefine(), connectorType))
                    .append(" ,PRIMARY KEY (`").append(table.getEsPrimaryKey()).append("`)").append(" NOT ENFORCED) WITH ")
                    .append(" ('connector' = 'elasticsearch-7', 'hosts' = '").append(table.getEsUrlAddress()).append("',")
                    .append("'index' = '").append(table.getEsIndex()).append("')");
            table.setEsCreateSql(sb.toString());
        }
    }

    private String schemaTransform(String schemaDefine, String connectorType) {
        StringBuilder sb = new StringBuilder();
        JSONArray array = JSON.parseArray(schemaDefine);
        if ("02".equals(connectorType)) {
            for (int i = 0; i < array.size(); i++) {
                JSONObject o = (JSONObject) array.get(i);
                Object schemaDefine1 = o.get("jdbcKey");
                Object dataBaseType = o.get("jdbcType");
                sb.append("`" + schemaDefine1 + "` " + dataBaseType + ",");
            }
        } else if ("04".equals(connectorType) || "05".equals(connectorType)) {
            for (int i = 0; i < array.size(); i++) {
                JSONObject o = (JSONObject) array.get(i);
                Object schemaDefine1 = o.get("esKey");
                Object dataBaseType = o.get("esType");
                sb.append("`" + schemaDefine1 + "` " + dataBaseType + ",");
            }
        }
        return sb.substring(0, sb.length() - 1);
    }

    // 将数组转换为字段schema_defina的json格式
    private void tranSchemaJson(TDimensionTable tDimensionTable) {

        Object[] jdbcDynamicItem = tDimensionTable.getJdbcDynamicItem();
        Object[] esDynamicItem = tDimensionTable.getEsDynamicItem();
        if ("02".equals(tDimensionTable.getConnectorType()))
            tDimensionTable.setSchemaDefine(JSON.toJSONString(jdbcDynamicItem));
        else if ("04".equals(tDimensionTable.getConnectorType()))
            tDimensionTable.setSchemaDefine(JSON.toJSONString(esDynamicItem));
        else if ("05".equals(tDimensionTable.getConnectorType()))
            tDimensionTable.setSchemaDefine(JSON.toJSONString(esDynamicItem));
//        if (jdbcDynamicItem != null && jdbcDynamicItem.length > 0) {
//            List list = new ArrayList();
//            for (int i = 0; i < jdbcDynamicItem.length; i++) {
//                HashMap hashMap = new HashMap();
//                HashMap o = (HashMap) jdbcDynamicItem[i];
//                hashMap.put(o.get("jdbcKey"), o.get("jdbcType"));
//                list.add(hashMap);
//            }
//        }
    }

    /**
     * 修改数据维
     *
     * @param tDimensionTable 数据维
     * @return 结果
     */
    @Override
    public int updateTDimensionTable(TDimensionTable tDimensionTable) {
        tranSchemaJson(tDimensionTable);
        joinDimensionTableSql(tDimensionTable);
        tDimensionTable.setUpdateBy(SecurityUtils.getUsername());
        return tDimensionTableMapper.updateTDimensionTable(tDimensionTable);
    }

    /**
     * 批量删除数据维
     *
     * @param dimensionIds 需要删除的数据维ID
     * @return 结果
     */
    @Override
    public int deleteTDimensionTableByIds(Long[] dimensionIds) {
        return tDimensionTableMapper.deleteTDimensionTableByIds(dimensionIds);
    }

    /**
     * 删除数据维信息
     *
     * @param dimensionId 数据维ID
     * @return 结果
     */
    @Override
    public int deleteTDimensionTableById(Long dimensionId) {
        return tDimensionTableMapper.deleteTDimensionTableById(dimensionId);
    }

    @Override
    public List<TDimensionTable> selectTDimensionTableByConnType() {
        return tDimensionTableMapper.selectTDimensionTableByConntype();
    }

    // 根据维表id查询维表list
    @Override
    public List<TDimensionTable> getTDimensionTableListByIds(Long[] ids) {
        return tDimensionTableMapper.getTDimensionTableListByIds(ids);
    }
}
