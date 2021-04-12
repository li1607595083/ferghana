package com.skyon.service;



import com.skyon.domain.TSelfFunction;

import java.util.List;

/**
 * 自定义函数Service接口
 * 
 * @author ruoyi
 * @date 2020-06-18
 */
public interface ITSelfFunctionService 
{
    /**
     * 查询自定义函数
     * 
     * @param selfFunctionId 自定义函数ID
     * @return 自定义函数
     */
    public TSelfFunction selectTSelfFunctionById(Long selfFunctionId);

    /**
     * 查询自定义函数列表
     * 
     * @param tSelfFunction 自定义函数
     * @return 自定义函数集合
     */
    public List<TSelfFunction> selectTSelfFunctionList(TSelfFunction tSelfFunction);

    /**
     * 新增自定义函数
     * 
     * @param tSelfFunction 自定义函数
     * @return 结果
     */
    public int insertTSelfFunction(TSelfFunction tSelfFunction);

    /**
     * 修改自定义函数
     * 
     * @param tSelfFunction 自定义函数
     * @return 结果
     */
    public int updateTSelfFunction(TSelfFunction tSelfFunction);

    /**
     * 批量删除自定义函数
     * 
     * @param selfFunctionIds 需要删除的自定义函数ID
     * @return 结果
     */
    public int deleteTSelfFunctionByIds(Long[] selfFunctionIds);

    /**
     * 删除自定义函数信息
     * 
     * @param selfFunctionId 自定义函数ID
     * @return 结果
     */
    public int deleteTSelfFunctionById(Long selfFunctionId);
}
