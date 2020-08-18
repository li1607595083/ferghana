package com.skyon.service.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.skyon.domain.TDimensionTable;
import com.skyon.mapper.TDimensionTableMapper;
import com.skyon.service.ITDimensionTableService;
import com.skyon.uitls.DateUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * 数据维Service业务层处理
 *
 * @author ruoyi
 * @date 2020-07-22
 */
@Service
public class TDimensionTableServiceImpl implements ITDimensionTableService {
    @Autowired
    private TDimensionTableMapper tDimensionTableMapper;

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
        String redisSchemaDefine = tDimensionTable.getRedisKey();
        if (StringUtils.isNotEmpty(redisSchemaDefine)) {
            List parse = (List) JSONObject.parse(redisSchemaDefine);
            Object[] oejectArr = new Object[parse.size()];
            final int[] j = {0};
            for (int i = 0; i < parse.size(); i++) {
                Map o = (Map) parse.get(i);
                o.forEach((key, Value) -> {
                    HashMap<String, Object> hashMap = new HashMap<>();
                    hashMap.put("redisKey", key);
                    hashMap.put("redisKeyField", Value);
                    oejectArr[j[0]] = hashMap;
                    j[0] = j[0] + 1;
                });

            }
            tDimensionTable.setRedisDynamicItem(oejectArr);
        }

        // 转换 jdbc 的json
        String jdbcSchemaDefine = tDimensionTable.getSchemaDefine();
        if (StringUtils.isNotEmpty(jdbcSchemaDefine)) {
            List parse = (List) JSONObject.parse(jdbcSchemaDefine);
            Object[] oejectArr = new Object[parse.size()];
            final int[] j = {0};
            for (int i = 0; i < parse.size(); i++) {
                Map o = (Map) parse.get(i);
                o.forEach((key, Value) -> {
                    HashMap<String, Object> hashMap = new HashMap<>();
                    hashMap.put("jdbcKey", key);
                    hashMap.put("jdbcType", Value);
                    oejectArr[j[0]] = hashMap;
                    j[0] = j[0] + 1;
                });

            }
            tDimensionTable.setJdbcDynamicItem(oejectArr);
        }
        // 转换 hbase 的json
        String hbaseSchemaDefine = tDimensionTable.getHbaseSchemaDefine();
        if (StringUtils.isNotEmpty(hbaseSchemaDefine)) {
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


        return tDimensionTable;
    }

    /**
     * 查询数据维列表
     *
     * @param tDimensionTable 数据维
     * @return 数据维
     */
    @Override
    public List<TDimensionTable> selectTDimensionTableList(TDimensionTable tDimensionTable) {
        return tDimensionTableMapper.selectTDimensionTableList(tDimensionTable);
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
        return tDimensionTableMapper.insertTDimensionTable(tDimensionTable);
    }

    // 将数组转换为字段schema_defina的json格式
    private void tranSchemaJson(TDimensionTable tDimensionTable) {

        Object[] dynamicItem = tDimensionTable.getRedisDynamicItem();
        if (dynamicItem != null && dynamicItem.length > 0) {
            List list = new ArrayList();
            for (int i = 0; i < dynamicItem.length; i++) {
                HashMap hashMap = new HashMap();
                HashMap o = (HashMap) dynamicItem[i];
                hashMap.put(o.get("redisKey"), o.get("redisKeyField"));
                list.add(hashMap);
            }
            tDimensionTable.setRedisKey(JSONObject.toJSONString(list));
        }

        Object[] jdbcDynamicItem = tDimensionTable.getJdbcDynamicItem();
        if (jdbcDynamicItem != null && jdbcDynamicItem.length > 0) {
            List list = new ArrayList();
            for (int i = 0; i < jdbcDynamicItem.length; i++) {
                HashMap hashMap = new HashMap();
                HashMap o = (HashMap) jdbcDynamicItem[i];
                hashMap.put(o.get("jdbcKey"), o.get("jdbcType"));
                list.add(hashMap);
            }
            tDimensionTable.setSchemaDefine(JSONObject.toJSONString(list));
        }
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
        tDimensionTable.setModifyTime(new Date());
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
}
