package com.skyon.project.system.service.impl;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Strings;
import com.skyon.common.utils.DateUtils;
import com.skyon.common.utils.SecurityUtils;
import com.skyon.framework.aspectj.lang.annotation.DataScope;
import com.skyon.project.system.domain.TDataResultSource;
import com.skyon.project.system.mapper.TDataResultSourceMapper;
import com.skyon.project.system.service.ITDataResultSourceService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * 【请填写功能名称】Service业务层处理
 *
 *
 * @date 2020-05-31
 */
@Service
public class TDataResultSourceServiceImpl implements ITDataResultSourceService {
    @Autowired
    private TDataResultSourceMapper tDataResultSourceMapper;

    /**
     * 查询【请填写功能名称】
     *
     * @param dataResultSourceId 【请填写功能名称】ID
     * @return 【请填写功能名称】
     */
    @Override
    public TDataResultSource selectTDataResultSourceById(Long dataResultSourceId) {
        TDataResultSource tDataSource = tDataResultSourceMapper.selectTDataResultSourceById(dataResultSourceId);
        String schemaDefine = tDataSource.getSchemaDefine();
        if (!Strings.isNullOrEmpty(schemaDefine)) {
            List parse = (List) JSONObject.parse(schemaDefine);
            Object[] oejectArr = new Object[parse.size()];
            final int[] j = {0};
            for (int i = 0; i < parse.size(); i++) {
                Map o = (Map) parse.get(i);
                o.forEach((key, Value) -> {
                    HashMap<String, Object> hashMap = new HashMap<>();
                    hashMap.put("schemaDefine", key);
                    hashMap.put("dataBaseType", Value);
                    oejectArr[j[0]] = hashMap;
                    j[0] = j[0] + 1;
                });

            }
            tDataSource.setDynamicItem(oejectArr);
        }

        return tDataSource;

    }

    /**
     * 查询【请填写功能名称】列表
     *
     * @param tDataResultSource 【请填写功能名称】
     * @return 【请填写功能名称】
     */
    @Override
    @DataScope(serviceTable = true)
    public List<TDataResultSource> selectTDataResultSourceList(TDataResultSource tDataResultSource) {
        return tDataResultSourceMapper.selectTDataResultSourceList(tDataResultSource);
    }

    @Override
    public TDataResultSource selectTDataResultSourceByTableName(String tableName) {
        TDataResultSource tDataResultSource = new TDataResultSource();
        tDataResultSource.setTableName(tableName);
        List<TDataResultSource> list = tDataResultSourceMapper.selectTDataResultSourceList(tDataResultSource);
        if (list != null){
            return list.get(0);
        }
        return null;
    }

    /**
     * 新增【请填写功能名称】
     *
     * @param tDataResultSource 【请填写功能名称】
     * @return 结果
     */
    @Override
    public int insertTDataResultSource(TDataResultSource tDataResultSource) {
        tDataResultSource.setCreateBy(SecurityUtils.getUsername());
        tDataResultSource.setCreateId(SecurityUtils.getUserId());
//        tranSchemaJson(tDataResultSource);
        return tDataResultSourceMapper.insertTDataResultSource(tDataResultSource);
    }

    /**
     * 修改【请填写功能名称】
     *
     * @param tDataResultSource 【请填写功能名称】
     * @return 结果
     */
    @Override
    public int updateTDataResultSource(TDataResultSource tDataResultSource) {
        tranSchemaJson(tDataResultSource);
        tDataResultSource.setUpdateBy(SecurityUtils.getUsername());
        return tDataResultSourceMapper.updateTDataResultSource(tDataResultSource);
    }

    /**
     * 批量删除【请填写功能名称】
     *
     * @param dataResultSourceIds 需要删除的【请填写功能名称】ID
     * @return 结果
     */
    @Override
    public int deleteTDataResultSourceByIds(Long[] dataResultSourceIds) {
        return tDataResultSourceMapper.deleteTDataResultSourceByIds(dataResultSourceIds);
    }

    /**
     * 删除【请填写功能名称】信息
     *
     * @param dataResultSourceId 【请填写功能名称】ID
     * @return 结果
     */
    @Override
    public int deleteTDataResultSourceById(Long dataResultSourceId) {
        return tDataResultSourceMapper.deleteTDataResultSourceById(dataResultSourceId);
    }

    @Override
    public List<TDataResultSource> selectDataResultSourceByIds(Long[] dataResultSourceIds) {
        return tDataResultSourceMapper.selectDataResultSourceByIds(dataResultSourceIds);
    }

    // 将数组转换为字段schema_defina的json格式
    private void tranSchemaJson(TDataResultSource tDataResultSource) {
        List list = new ArrayList();
        Object[] dynamicItem = tDataResultSource.getDynamicItem();
        if (dynamicItem != null && dynamicItem.length > 0) {
            for (int i = 0; i < dynamicItem.length; i++) {
                HashMap hashMap = new HashMap();
                HashMap o = (HashMap) dynamicItem[i];
                hashMap.put(o.get("schemaDefine"), o.get("dataBaseType"));
                list.add(hashMap);
            }
        }
        tDataResultSource.setSchemaDefine(JSONObject.toJSONString(list));
    }
}
