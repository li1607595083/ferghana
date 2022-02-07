package com.skyon.project.system.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Strings;
import com.skyon.project.system.domain.TDataSource;
import com.skyon.project.system.domain.TDatasourceField;
import com.skyon.project.system.domain.TVariableCenter;
import com.skyon.project.system.mapper.TDataSourceMapper;
import com.skyon.project.system.mapper.TDatasourceFieldMapper;
import com.skyon.project.system.service.ITDataSourceService;
import com.skyon.project.system.service.ITDatasourceFieldService;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.poi.ss.formula.functions.T;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * 【请填写功能名称】Service业务层处理
 *
 * @date 2021-02-25
 */
@Service
public class TDatasourceFieldServiceImpl implements ITDatasourceFieldService {

    @Autowired
    private TDatasourceFieldMapper fieldMapper;

    @Override
    public List<TDatasourceField> selectTDatasourceFieldList(TDatasourceField tDatasourceField) {
        return fieldMapper.selectTDatasourceFieldList(tDatasourceField);
    }


    // 修改字段 为 已经使用
    @Override
    public void updateFiledIsUsed(TVariableCenter tVariableCenter) {

        // 修改数据源表对应的字段
        List listOne = getFields(tVariableCenter);
        Map map = new HashMap();
        map.put("id", tVariableCenter.getVariableClassification());
        map.put("variableName", "[" + tVariableCenter.getVariableNameEn() + "]");
        map.put("fields", listOne.toArray());
        if (listOne.size() > 0) {
            fieldMapper.updateFieldNameIsUsed(map);
        }

        // 修改数据维表 对应的字段
        List list = joinDeleteParamList(tVariableCenter);
        if (list.size() > 0) {
            fieldMapper.updateDimensionTableFieldNameIsUsed(list);
        }

    }

    // 组装删除参数
    private List joinDeleteParamList(TVariableCenter tVariableCenter) {
//   array 示例：
//        		[{"name":"数据维表:EP_BIND_ACCT","col":["ACCT_NO_TWO-主键","EP_BIND_ACCT.VERIFY_FLG"]}
//				,{"name":"数据维表:EP_CUST_INF","col":["CUST_NO_TWO-主键","EP_CUST_INF.ADDR"]}]
        JSONArray array = JSON.parseArray(tVariableCenter.getTestDimensionTableCol().toString());
        List list = new ArrayList();
        if (array != null && array.size() > 0) {
            for (int i = 0; i < array.size(); i++) {
                JSONObject jsonObject = (JSONObject) array.get(i);
                String name = jsonObject.get("name").toString();
                name = name.substring(5);
                JSONArray col = (JSONArray) jsonObject.get("col");
                for (int j = 0; j < col.size(); j++) {
                    String o = col.get(j).toString();
                    Map m = new HashMap();
                    m.put("variableName", '[' + tVariableCenter.getVariableNameEn() + ']');
                    m.put("tableName", name);
                    if (o.indexOf("主键") > 0 || o.indexOf("水印") > 0) {
                        String substring = o.substring(0, o.length() - 3);
                        m.put("fieldName", substring);
                    } else if (o.indexOf(".") > 0) {
                        String[] split = o.split("\\.");
                        m.put("fieldName", split[1]);
                    }
                    list.add(m);
                }
            }
        }
        return list;
    }

    private List getFields(TVariableCenter tVariableCenter) {
        // 需要改变的字段
        JSONArray array = JSON.parseArray(tVariableCenter.getTestSourceTableCol().toString());
        List listOne = new ArrayList();
        if (array != null && array.size() > 0) {
//            ["TRAN_NO-主键","TRAN_TIME-水印","EP_OPENACCT_FLOW_TABLE.BSN_CODE","EP_OPENACCT_FLOW_TABLE.STATUS",
//            "CUST_NO-EP_CUST_INF关联字段","ACCT_NO-EP_BIND_ACCT关联字段"]
            for (int i = 0; i < array.size(); i++) {
                String o = array.get(i).toString();
                if (o.indexOf("主键") > 0 || o.indexOf("水印") > 0) {
                    String substring = o.substring(0, o.length() - 3);
                    listOne.add(substring);
                } else if (o.indexOf("关联字段") > 0) {
                    String[] split = o.split("-");
                    listOne.add(split[0]);
                } else if (o.indexOf(".") > 0) {
                    String[] split = o.split("\\.");
                    listOne.add(split[1]);
                }
            }
        }
        return listOne;
    }

    /**
     * 新增字段
     *
     * @param tableName     表名
     * @param fields        表schema字段
     * @param tableType     01 数据源表 02 数据维表
     * @param connectorType 连接器类型
     * @return int
     */
    @Override
    public int insertTDatasourceField(String tableName, Object fields, String tableType, String connectorType) {
        List list = new ArrayList();
        if ("01".equals(tableType)) {
            Object[] dynamicItem = (Object[]) fields;
            for (int i = 0; i < dynamicItem.length; i++) {
                Map m = new HashMap();
                Map map = (Map) dynamicItem[i];
                m.put("tableName", tableName);
                m.put("tableType", "01");
                m.put("fieldName", map.get("schemaDefine"));
                m.put("fieldType", map.get("dataBaseType"));
                m.put("isUsed", "0");
                list.add(m);
            }
        } else if ("02".equals(tableType)) {
            JSONArray array = JSON.parseArray(fields.toString());
            //连接器类型为02，代表redis
            if ("02".equals(connectorType)) {
                for (int i = 0; i < array.size(); i++) {
                    Map map = new HashMap();
                    JSONObject o = (JSONObject) array.get(i);
                    map.put("tableName", tableName);
                    map.put("tableType", "02");
                    map.put("fieldName", o.get("redisKey"));
                    map.put("fieldType", o.get("redisType"));
                    map.put("isUsed", "0");
                    list.add(map);
                }
            } else {
                for (int i = 0; i < array.size(); i++) {
                    Map map = new HashMap();
                    JSONObject o = (JSONObject) array.get(i);
                    map.put("tableName", tableName);
                    map.put("tableType", "02");
                    map.put("fieldName", o.get("jdbcKey"));
                    map.put("fieldType", o.get("jdbcType"));
                    map.put("isUsed", "0");
                    list.add(map);
                }
            }
        }
        return fieldMapper.insertTDatasourceField(list);
    }

    @Override
    public void updateIsUsed(TVariableCenter tVariableCenter) {
        // 修改 t_datasource_field 里的数据源表字段
        List listOne = getFields(tVariableCenter);
        Map map = new HashMap();
        map.put("id", tVariableCenter.getVariableClassification());
        map.put("variableName", "[" + tVariableCenter.getVariableNameEn() + "]");
        map.put("fields", listOne.toArray());
        if (listOne.size() > 0) {
            fieldMapper.updateIsUsedTableType01ById(map);
        }

        // 修改数据维表 对应的字段
        List list = joinDeleteParamList(tVariableCenter);
        if(list.size() > 0) {
            fieldMapper.updateWhenDelete(list);
        }

    }

    @Override
    public void updateFieldAndIsUsed(TVariableCenter tVariableCenter) {
        // 修改t_datasource_field 中数据源表字段的变化
        updateSourceField(tVariableCenter);

        // 修改t_datasource_field 中数据源维表字段的变化
        updateDiemnsionField(tVariableCenter);
    }

    // 修改t_datasource_field 中数据源维表字段的变化
    private void updateDiemnsionField(TVariableCenter center) {

        // 数据维表修改后的字段
        List listAfter = joinDeleteParamList(center); // aaa 111  - aaa 222  - bbb 222 -bbb 333

        // 数据源表修改前的字段
        List datasourceFields = fieldMapper.selectDimensionField(center.getVariableNameEn());
        List listBefore = new ArrayList();  //  bbb 222 -bbb 333 - ccc 444
        if (datasourceFields != null && datasourceFields.size() > 0) {
            for (int i = 0; i < datasourceFields.size(); i++) {
                HashMap maps = new HashMap();
                HashMap m = (HashMap) datasourceFields.get(i);
                Object fieldName = m.get("field_name");
                maps.put("variableName", '[' + center.getVariableNameEn() + ']');
                maps.put("fieldName", m.get("field_name"));
                maps.put("tableName", m.get("datasource_table_name"));
                listBefore.add(maps);
            }
        }
        System.out.println("");


        List listdelete = new ArrayList();
        if (listAfter == null || listAfter.size() == 0) { // 没有新增的

        }

        HashSet set = new HashSet();
        set.addAll(listAfter);
        set.addAll(listBefore);


//        for (int i = 0; i < listAfter.size(); i++) {
//            for (int j = 0; j < listBefore.size(); j++) {
//                HashMap after = (HashMap) listAfter.get(i);
//                HashMap before = (HashMap) listBefore.get(j);
//                if (after.get("tableName").equals(before.get("tableName")) && after.get("fieldName").equals(before.get("fieldName")) ) {
//                    set.add();
//
//                }
//            }
//        }


        // 从 listAfter  和 listBefore 的数据中 。 把新增的和删除的分别拿出来

        // 新增的字段
//        m.put("variableName", '[' + tVariableCenter.getVariableNameEn() + ']');
//        m.put("tableName", name);
//        m.put("fieldName", substring);
//        list.add(m);

        List listInsert = new ArrayList();
        listInsert.addAll(set);
        listInsert.removeAll(listBefore);
        if (listInsert.size() > 0) {
            fieldMapper.updateDimensionTableFieldNameIsUsed(listInsert);
        }


        // 删除的字段

//        m.put("variableName", '[' + tVariableCenter.getVariableNameEn() + ']');
//        m.put("tableName", name);
//        m.put("fieldName", substring);
//        list.add(m);

        List listDelete = new ArrayList();
        listDelete.addAll(set);
        listDelete.removeAll(listAfter);
        if (listDelete.size() > 0) {
            fieldMapper.updateWhenDelete(listDelete);
        }



    }


    // 修改t_datasource_field 中数据源表字段的变化
    private void updateSourceField(TVariableCenter tVariableCenter) {
        // 数据源表修改后的字段
        List listOne = getFields(tVariableCenter);

        // 数据源表修改前的字段
        List listdemo = getDeleteField(tVariableCenter);
        // 查出 变化的 字段 ， 增加 ==  减少
        HashSet hashSet = new HashSet(listOne);
        hashSet.addAll(listdemo); // 所有的

        // 新增的
        HashSet hash1 = new HashSet();
        Map map1 = joinMap(hash1, hashSet, listdemo, tVariableCenter);
        if (map1 != null) fieldMapper.updateIsUsed1ById(map1);

        // 删除的
        HashSet hash2 = new HashSet();
        Map map2 = joinMap(hash2, hashSet, listOne, tVariableCenter);
        if (map2 != null) fieldMapper.updateIsUsedTableType01ById(map2);
    }


    // 数据源表修改前的字段
    private List getDeleteField(TVariableCenter tVariableCenter) {
        List listdemo = new ArrayList();
        Map map = new HashMap();
        map.put("id", tVariableCenter.getVariableClassification());
        map.put("name", tVariableCenter.getVariableNameEn());
        map.put("tableType", "01");
        List list = fieldMapper.selectFieldBYVariableEN(map);
        if (list != null) {
            for (int i = 0; i < list.size(); i++) {
                HashMap m = (HashMap) list.get(i);
                Object field_name = m.get("field_name");
                listdemo.add(field_name);
            }
        }
        return listdemo;
    }


    // 分别组装删除和新增的字段
    private Map joinMap(HashSet hash, HashSet hashSet, List list, TVariableCenter tVariableCenter) {

        hash.addAll(hashSet);
        hash.removeAll(list);
        // 修改对应的字段
        Map map = new HashMap();
        map.put("id", tVariableCenter.getVariableClassification());
        map.put("variableName", "[" + tVariableCenter.getVariableNameEn() + "]");
        map.put("fields", hash.toArray());
        if (hash.size() > 0) {
            return map;
        }
        return null;
    }

    /**
     * 根据 字段和表类型删除 t_datasource_field 数据
     *
     * @param tableNames 字段
     * @param tableType  表类型 01 数据源表 02 数据维表
     */
    @Override
    public void deleteTDatasourceField(String[] tableNames, String tableType) {
        Map map = new HashMap();
        map.put("tableNames", tableNames);
        map.put("tableType", tableType);
        fieldMapper.deleteTdatasourceField(map);
    }

    /**
     * 修改 t_datasource_field 里的字段
     *
     * @param fields    字段
     * @param tableType 表类型
     */
    @Override
    public void updatefieldName(Object fields, String tableType, String connectorType, String tableName) {
        List fieldsNew = new ArrayList();
        Object[] dynamicItem = new Object[0];
        if ("01".equals(tableType)) {
            Object[] array = (Object[]) fields;
            dynamicItem = new Object[array.length];
            for (int i = 0; i < array.length; i++) {
                Map m = (Map) array[i];
                Map map = new HashMap();
                map.put("fieldName", m.get("schemaDefine"));
                map.put("id", m.get("fieldId"));
                dynamicItem[i] = map;
            }
            fieldMapper.updateFieldName(dynamicItem);
        } else if ("02".equals(tableType)) {
            if("02".equals(connectorType)){
                JSONArray array = JSON.parseArray(fields.toString());
                dynamicItem = new Object[array.size()];
                for (int i = 0; i < array.size(); i++) {
                    Map map = new HashMap();
                    JSONObject o = (JSONObject) array.get(i);
                    map.put("fieldName", o.get("redisKey"));
                    map.put("id", o.get("fieldId"));
                    dynamicItem[i] = map;
                }
            }else{
                JSONArray array = JSON.parseArray(fields.toString());
                dynamicItem = new Object[array.size()];
                for (int i = 0; i < array.size(); i++) {
                    Map map = new HashMap();
                    JSONObject o = (JSONObject) array.get(i);
                    map.put("fieldName", o.get("jdbcKey"));
                    map.put("id", o.get("fieldId"));

                    // fieldId 为空的，代表新增字段  添加到 fieldsNew 中
                    if (o.get("fieldId") == null){
                        fieldsNew.add(o);
                    }

                    dynamicItem[i] = map;
                }
            }

        }

        fieldMapper.updateFieldName(dynamicItem);

        if(fieldsNew.size()>0){
            this.insertTDatasourceField(tableName, fieldsNew,  tableType,  connectorType);
        }

    }


}
