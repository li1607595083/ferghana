package com.skyon.project.system.mapper;

import com.skyon.project.system.domain.TSelfFunction;

import java.util.List;

/**
 * 自定义函数Mapper接口
 * 
 *
 * @date 2020-06-18
 */
public interface TSelfFunctionMapper 
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

    public List<TSelfFunction> selectTSelfAndFunctionList();

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
     * 删除自定义函数
     * 
     * @param selfFunctionId 自定义函数ID
     * @return 结果
     */
    public int deleteTSelfFunctionById(Long selfFunctionId);

    /**
     * 批量删除自定义函数
     * 
     * @param selfFunctionIds 需要删除的数据ID
     * @return 结果
     */
    public int deleteTSelfFunctionByIds(Long[] selfFunctionIds);

    public List<TSelfFunction> selectTSelfFunctionByIds(Long[] selfFunctionIds);


    public List<TSelfFunction> selectTSelfFunctionByNameCn(String selfFunctionNameCn);

    public List<TSelfFunction> selectTSelfFunctionByName(String name);

    public List<TSelfFunction> selectTSelfFunctionByPackage(String packagePath);

    public List<TSelfFunction> selectTSelfFunctionByNameCnNoId(TSelfFunction tSelfFunction);

    public List<TSelfFunction> selectTSelfFunctionByNameNoId(TSelfFunction tSelfFunction);

    public List<TSelfFunction> selectTSelfFunctionByPackageNoId(TSelfFunction tSelfFunction);
}
