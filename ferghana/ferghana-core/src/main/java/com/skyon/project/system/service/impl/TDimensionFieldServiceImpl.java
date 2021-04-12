package com.skyon.project.system.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.skyon.project.system.domain.TDimensionTable;
import com.skyon.project.system.domain.TVariableCenter;
import com.skyon.project.system.mapper.TDimensionFieldMapper;
import com.skyon.project.system.service.ITdimensionFieldService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class TDimensionFieldServiceImpl implements ITdimensionFieldService {

    @Autowired
    private TDimensionFieldMapper fieldMapper;

    // 新增数据
    @Override
    public void insertDimensionField(TDimensionTable table) {
        List list = new ArrayList();
//        if (table.getConnectorType().equals("02")) { // jdbc
//            String schemaDefine = table.getSchemaDefine();
//            if (schemaDefine != null) {
//                JSONArray array = JSON.parseArray(schemaDefine);
//                for (int i = 0; i < array.size(); i++) {
//                    Map map = new HashMap();
//                    JSONObject o = (JSONObject) array.get(i);
//                    map.put("tableName", table.getDimensionName());
//                    map.put("connectorType", "jdbc");
//                    map.put("fieldName", o.get("jdbcKey"));
//                    map.put("fieldType", o.get("jdbcType"));
//                    map.put("isUsed", "0");
//                    list.add(map);
//                }
//            }
//        }
        if (list.size() > 0) fieldMapper.insertTDimensionField(list);
    }


    // 删除数据
    @Override
    public void deleteTDimensionField(String[] tableNames) {
        fieldMapper.deleteTDimensionField(tableNames);
    }


    // 修改字段 为 已经使用
    @Override
    public void updateFiledIsUsed(TVariableCenter tVariableCenter) {
        // 需要改变的字段
        JSONArray array = JSON.parseArray(tVariableCenter.getTestDimensionTableCol().toString());
        if (array != null && array.size() > 0) {
//            [{"name":"数据维表:EP_CUST_INF","col":["CUST_NO_TWO-主键","EP_CUST_INF.ADDR"]},
//            {"name":"数据维表:EP_BIND_ACCT","col":["ACCT_NO_TWO-主键","EP_BIND_ACCT.VERIFY_FLG"]}]
//            for (int i = 0; i < array.size(); i++) {
//                String o = array.get(i).toString();
//                if (o.indexOf("主键") > 0 || o.indexOf("水印") > 0) {
//                    String substring = o.substring(0, o.length() - 3);
//                    listOne.add(substring);
//                } else if (o.indexOf("关联字段") > 0) {
//                    String[] split = o.split("-");
//                    listOne.add(split[0]);
//                } else if (o.indexOf(".") > 0) {
//                    String[] split = o.split("\\.");
//                    listOne.add(split[1]);
//                }
//            }
        }
    }


}
