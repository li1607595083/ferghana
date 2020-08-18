package com.skyon.service.impl;


import com.skyon.domain.TDataSource;
import com.skyon.mapper.TDataSourceMapper;
import com.skyon.service.ITDataSourceService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * 【请填写功能名称】Service业务层处理
 *
 * @author ruoyi
 * @date 2020-05-21
 */
@Service
public class TDataSourceServiceImpl implements ITDataSourceService {

    @Autowired
    private TDataSourceMapper tDataSourceMapper;

    /**
     * 查询【请填写功能名称】
     *
     * @param dataSourceId 【请填写功能名称】ID
     * @return 【请填写功能名称】
     */
    @Override
    public TDataSource selectTDataSourceById(Long dataSourceId) {

        TDataSource tDataSource = tDataSourceMapper.selectTDataSourceById(dataSourceId);
//        String schemaDefine = tDataSource.getSchemaDefine();
//        if (!Strings.isNullOrEmpty(schemaDefine)) {
//            Map parse = (Map) JSONObject.parse(schemaDefine);
//            Object[] oejectArr = new Object[parse.size()];
//            final int[] i = {0};
//            parse.forEach((key, value) -> {
//                HashMap<String, Object> hashMap = new HashMap<>();
//                hashMap.put("schemaDefine", key);
//                hashMap.put("dataBaseType", value);
//                oejectArr[i[0]] = hashMap;
//                i[0] = i[0] + 1;
//            });
//            tDataSource.setDynamicItem(oejectArr);
//        }

        return tDataSource;
    }

    /**
     * 查询【请填写功能名称】列表
     *
     * @param tDataSource 【请填写功能名称】
     * @return 【请填写功能名称】
     */
    @Override
    public List<TDataSource> selectTDataSourceList(TDataSource tDataSource) {
        return tDataSourceMapper.selectTDataSourceList(tDataSource);
    }

    /**
     * 新增【请填写功能名称】
     *
     * @param tDataSource 【请填写功能名称】
     * @return 结果
     */
    @Override
    public int insertTDataSource(TDataSource tDataSource) {
//        tranSchemaJson(tDataSource);
        return tDataSourceMapper.insertTDataSource(tDataSource);
    }

    public static void main(String[] args) {


    }

    /**
     * 修改【请填写功能名称】
     *
     * @param tDataSource 【请填写功能名称】
     * @return 结果
     */
    @Override
    public int updateTDataSource(TDataSource tDataSource) {
//        tranSchemaJson(tDataSource);
        return tDataSourceMapper.updateTDataSource(tDataSource);
    }

    /**
     * 批量删除【请填写功能名称】
     *
     * @param dataSourceIds 需要删除的【请填写功能名称】ID
     * @return 结果
     */
    @Override
    public int deleteTDataSourceByIds(Long[] dataSourceIds) {
        return tDataSourceMapper.deleteTDataSourceByIds(dataSourceIds);
    }

    /**
     * 删除【请填写功能名称】信息
     *
     * @param dataSourceId 【请填写功能名称】ID
     * @return 结果
     */
    @Override
    public int deleteTDataSourceById(Long dataSourceId) {
        return tDataSourceMapper.deleteTDataSourceById(dataSourceId);
    }

    // 将数组转换为字段schema_defina的json格式
//    private void tranSchemaJson(TDataSource tDataSource){
//        HashMap hashMap = new HashMap();
//        Object[] dynamicItem = tDataSource.getDynamicItem();
//        if (dynamicItem != null && dynamicItem.length > 0) {
//            for (int i = 0; i < dynamicItem.length; i++) {
//                HashMap o = (HashMap) dynamicItem[i];
//                hashMap.put(o.get("schemaDefine"), o.get("dataBaseType"));
//            }
//        }
//        tDataSource.setSchemaDefine(JSONObject.toJSONString(hashMap));
//    }
}
