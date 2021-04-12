package com.skyon.project.system.mapper;

import com.skyon.project.system.domain.TVariableCenter;

import java.util.List;

/**
 * 变量管理中心Mapper接口
 * 
 *
 * @date 2020-08-06
 */
public interface TVariableCenterMapper 
{
    /**
     * 查询变量管理中心
     * 
     * @param variableId 变量管理中心ID
     * @return 变量管理中心
     */
    public TVariableCenter selectTVariableCenterById(Long variableId);

    // 根据id批量查询
    public List<TVariableCenter> selectTVariableCenterByNames(String[] names);

    /**
     * 查询变量管理中心列表
     * 
     * @param tVariableCenter 变量管理中心
     * @return 变量管理中心集合
     */
    public List<TVariableCenter> selectTVariableCenterList(TVariableCenter tVariableCenter);

    public List<TVariableCenter> selectVariableVersionList(TVariableCenter tVariableCenter);

    /**
     * 新增变量管理中心
     * 
     * @param tVariableCenter 变量管理中心
     * @return 结果
     */
    public int insertTVariableCenter(TVariableCenter tVariableCenter);

    /**
     * 修改变量管理中心
     * 
     * @param tVariableCenter 变量管理中心
     * @return 结果
     */
    public int updateTVariableCenter(TVariableCenter tVariableCenter);

    public int updateVersionShow(TVariableCenter tVariableCenter);

    public TVariableCenter selectTVariableCenterVersion(TVariableCenter tVariableCenter);

    /**
     * 删除变量管理中心
     * 
     * @param variableId 变量管理中心ID
     * @return 结果
     */
    public int deleteTVariableCenterById(Long variableId);

    /**
     * 批量删除变量管理中心
     * 
     * @param variableIds 需要删除的数据ID
     * @return 结果
     */
    public int deleteTVariableCenterByIds(Long[] variableIds);

    public List<TVariableCenter> selectVariableCenterByIds(Long[] ids);
}
