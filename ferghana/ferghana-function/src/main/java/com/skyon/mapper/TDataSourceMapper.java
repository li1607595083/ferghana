package com.skyon.mapper;



import com.skyon.domain.TDataSource;

import java.util.List;

/**
 * 【请填写功能名称】Mapper接口
 * 
 * @author ruoyi
 * @date 2020-05-21
 */
public interface TDataSourceMapper 
{
    /**
     * 查询【请填写功能名称】
     * 
     * @param dataSourceId 【请填写功能名称】ID
     * @return 【请填写功能名称】
     */
    public TDataSource selectTDataSourceById(Long dataSourceId);

    /**
     * 查询数据源表的表名字段
     *
     * @param dataSourceId 【请填写功能名称】ID
     * @return 【请填写功能名称】
     */
    public TDataSource selectTDataSourceName();

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
     * 删除【请填写功能名称】
     * 
     * @param dataSourceId 【请填写功能名称】ID
     * @return 结果
     */
    public int deleteTDataSourceById(Long dataSourceId);

    /**
     * 批量删除【请填写功能名称】
     * 
     * @param dataSourceIds 需要删除的数据ID
     * @return 结果
     */
    public int deleteTDataSourceByIds(Long[] dataSourceIds);
}
