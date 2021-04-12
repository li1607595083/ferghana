package com.skyon.project.system.mapper;

import com.skyon.project.system.domain.TDataResultSource;

import java.util.List;

/**
 * 【请填写功能名称】Mapper接口
 * 
 *
 * @date 2020-05-31
 */
public interface TDataResultSourceMapper 
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
     * 删除【请填写功能名称】
     * 
     * @param dataResultSourceId 【请填写功能名称】ID
     * @return 结果
     */
    public int deleteTDataResultSourceById(Long dataResultSourceId);

    /**
     * 批量删除【请填写功能名称】
     * 
     * @param dataResultSourceIds 需要删除的数据ID
     * @return 结果
     */
    public int deleteTDataResultSourceByIds(Long[] dataResultSourceIds);

    public List<TDataResultSource> selectDataResultSourceByIds(Long[] dataResultSourceIds);
}
