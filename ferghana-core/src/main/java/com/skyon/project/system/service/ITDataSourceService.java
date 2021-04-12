package com.skyon.project.system.service;

import java.util.List;
import com.skyon.project.system.domain.TDataSource;

/**
 * 【请填写功能名称】Service接口
 * 
 *
 * @date 2020-05-21
 */
public interface ITDataSourceService 
{
    /**
     * 查询【请填写功能名称】
     * 
     * @param dataSourceId 【请填写功能名称】ID
     * @return 【请填写功能名称】
     */
    public TDataSource selectTDataSourceById(Long dataSourceId);

    // 根据表名查询schema
    public TDataSource selectTDataSourceByTableName(String tableName);

    /**
     * 查询【请填写功能名称】列表
     * 
     * @param tDataSource 【请填写功能名称】
     * @return 【请填写功能名称】集合
     */
    public List<TDataSource> selectTDataSourceList(TDataSource tDataSource);

    /**
     * 新增【请填写功能名称】
     * 
     * @param tDataSource 【请填写功能名称】
     * @return 结果
     */
    public int insertTDataSource(TDataSource tDataSource);

    /**
     * 修改【请填写功能名称】
     * 
     * @param tDataSource 【请填写功能名称】
     * @return 结果
     */
    public int updateTDataSource(TDataSource tDataSource);

    /**
     * 批量删除【请填写功能名称】
     * 
     * @param dataSourceIds 需要删除的【请填写功能名称】ID
     * @return 结果
     */
    public int deleteTDataSourceByIds(Long[] dataSourceIds);

    /**
     * 删除【请填写功能名称】信息
     * 
     * @param dataSourceId 【请填写功能名称】ID
     * @return 结果
     */
    public int deleteTDataSourceById(Long dataSourceId);

    // 根据id查集合
    public List<TDataSource> selectTDataSourceListByIds(Long[] ids);
}
