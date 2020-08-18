package com.skyon.service.impl;

import com.skyon.domain.TDataResultSource;
import com.skyon.mapper.TDataResultSourceMapper;
import com.skyon.service.ITDataResultSourceService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * 【请填写功能名称】Service业务层处理
 *
 * @author ruoyi
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
     * @param tDataResultSource 【请填写功能名称】
     * @return 【请填写功能名称】
     */
    @Override
    public List<TDataResultSource> selectTDataResultSourceList(TDataResultSource tDataResultSource) {
        return tDataResultSourceMapper.selectTDataResultSourceList(tDataResultSource);
    }

    /**
     * 新增【请填写功能名称】
     *
     * @param tDataResultSource 【请填写功能名称】
     * @return 结果
     */
    @Override
    public int insertTDataResultSource(TDataResultSource tDataResultSource) {
//        tDataResultSource.setCreateTime(DateUtils.getNowDate());
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
//        tranSchemaJson(tDataResultSource);
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

    // 将数组转换为字段schema_defina的json格式
//    private void tranSchemaJson(TDataResultSource tDataResultSource){
//        HashMap hashMap = new HashMap();
//        Object[] dynamicItem = tDataResultSource.getDynamicItem();
//        if (dynamicItem != null && dynamicItem.length > 0) {
//            for (int i = 0; i < dynamicItem.length; i++) {
//                HashMap o = (HashMap) dynamicItem[i];
//                hashMap.put(o.get("schemaDefine"), o.get("dataBaseType"));
//            }
//        }
//        tDataResultSource.setSchemaDefine(JSONObject.toJSONString(hashMap));
//    }
}
