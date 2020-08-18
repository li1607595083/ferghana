package com.skyon.service;



import com.skyon.domain.TDataResultSource;

import java.util.List;

/**
 * 【请填写功能名称】Service接口
 * 
 * @author ruoyi
 * @date 2020-05-31
 */
public interface ITDataResultSourceService 
{
    /**
     * 查询【请填写功能名称】
     * 
     * @param dataResultSourceId 【请填写功能名称】ID
     * @return 【请填写功能名称】
     */
    public TDataResultSource selectTDataResultSourceById(Long dataResultSourceId);

    /**
     * 查询【请填写功能名称】列表
     * 
     * @param tDataResultSource 【请填写功能名称】
     * @return 【请填写功能名称】集合
     */
    public List<TDataResultSource> selectTDataResultSourceList(TDataResultSource tDataResultSource);

    /**
     * 新增【请填写功能名称】
     * 
     * @param tDataResultSource 【请填写功能名称】
     * @return 结果
     */
    public int insertTDataResultSource(TDataResultSource tDataResultSource);

    /**
     * 修改【请填写功能名称】
     * 
     * @param tDataResultSource 【请填写功能名称】
     * @return 结果
     */
    public int updateTDataResultSource(TDataResultSource tDataResultSource);

    /**
     * 批量删除【请填写功能名称】
     * 
     * @param dataResultSourceIds 需要删除的【请填写功能名称】ID
     * @return 结果
     */
    public int deleteTDataResultSourceByIds(Long[] dataResultSourceIds);

    /**
     * 删除【请填写功能名称】信息
     * 
     * @param dataResultSourceId 【请填写功能名称】ID
     * @return 结果
     */
    public int deleteTDataResultSourceById(Long dataResultSourceId);
}
