package com.skyon.mapper;

import com.skyon.domain.TDimensionTable;

import java.util.List;

/**
 * 数据维Mapper接口
 * 
 * @author ruoyi
 * @date 2020-07-22
 */
public interface TDimensionTableMapper 
{
    /**
     * 查询数据维
     * 
     * @param dimensionId 数据维ID
     * @return 数据维
     */
    public TDimensionTable selectTDimensionTableById(Long dimensionId);

    /**
     * 查询数据维列表
     * 
     * @param tDimensionTable 数据维
     * @return 数据维集合
     */
    public List<TDimensionTable> selectTDimensionTableList(TDimensionTable tDimensionTable);

    /**
     * 新增数据维
     * 
     * @param tDimensionTable 数据维
     * @return 结果
     */
    public int insertTDimensionTable(TDimensionTable tDimensionTable);

    /**
     * 修改数据维
     * 
     * @param tDimensionTable 数据维
     * @return 结果
     */
    public int updateTDimensionTable(TDimensionTable tDimensionTable);

    /**
     * 删除数据维
     * 
     * @param dimensionId 数据维ID
     * @return 结果
     */
    public int deleteTDimensionTableById(Long dimensionId);

    /**
     * 批量删除数据维
     * 
     * @param dimensionIds 需要删除的数据ID
     * @return 结果
     */
    public int deleteTDimensionTableByIds(Long[] dimensionIds);
}
