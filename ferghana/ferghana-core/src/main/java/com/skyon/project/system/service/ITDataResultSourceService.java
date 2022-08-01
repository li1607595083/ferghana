package com.skyon.project.system.service;

import com.skyon.project.system.domain.TDataResultSource;

import java.util.List;

/**
 * 【请填写功能名称】Service接口
 * 
 *
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

    // 根据表名查数据结果表信息
    public TDataResultSource selectTDataResultSourceByTableName(String tableName);

    /**
     * 新增【请填写功能名称】
     * 
     * @param tDataResultSource 【请填写功能名称】
     * @return 结果
     */
    public int insertTDataResultSource(TDataResultSource tDataResultSource);

    /**
     * 修改
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

    public List<TDataResultSource> selectDataResultSourceByIds(Long[] dataResultSourceIds);
}
