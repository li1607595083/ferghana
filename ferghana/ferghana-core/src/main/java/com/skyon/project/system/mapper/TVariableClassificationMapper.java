package com.skyon.project.system.mapper;

import java.util.List;
import java.util.Map;

import com.skyon.project.system.domain.TVariableClassification;

/**
 * 变量分类Mapper接口
 * 
 *
 * @date 2020-08-21
 */
public interface TVariableClassificationMapper 
{
    /**
     * 查询变量分类
     * 
     * @param variableClassificationId 变量分类ID
     * @return 变量分类
     */
    public TVariableClassification selectTVariableClassificationById(Long variableClassificationId);

    /**
     * 查询变量分类列表
     * 
     * @param tVariableClassification 变量分类
     * @return 变量分类集合
     */
    public List<TVariableClassification> selectTVariableClassificationList(TVariableClassification tVariableClassification);

    /**
     * 新增变量分类
     * 
     * @param tVariableClassification 变量分类
     * @return 结果
     */
    public int insertTVariableClassification(TVariableClassification tVariableClassification);

    /**
     * 修改变量分类
     * 
     * @param tVariableClassification 变量分类
     * @return 结果
     */
    public int updateTVariableClassification(TVariableClassification tVariableClassification);

    /**
     * 删除变量分类
     * 
     * @param variableClassificationId 变量分类ID
     * @return 结果
     */
    public int deleteTVariableClassificationById(Long variableClassificationId);

    /**
     * 批量删除变量分类
     * 
     * @param variableClassificationIds 需要删除的数据ID
     * @return 结果
     */
    public int deleteTVariableClassificationByIds(Long[] variableClassificationIds);

    public Map selectSchemaPrimaryKeyByVariableId(Long variableClassificationId);
}
