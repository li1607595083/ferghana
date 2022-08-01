package com.skyon.project.system.mapper;

import java.util.ArrayList;
import java.util.List;
import com.skyon.project.system.domain.TVariablePackageManager;

/**
 * 变量包管理Mapper接口
 * 
 *
 * @date 2020-08-24
 */
public interface TVariablePackageManagerMapper 
{
    /**
     * 查询变量包管理
     * 
     * @param variablePackId 变量包管理ID
     * @return 变量包管理
     */
    public TVariablePackageManager selectTVariablePackageManagerById(Long variablePackId);

    public TVariablePackageManager selectTVariablePackageManagerByVersion(TVariablePackageManager tVariablePackageManager);

    /**
     * 查询变量包管理列表
     * 
     * @param tVariablePackageManager 变量包管理
     * @return 变量包管理集合
     */
    public List<TVariablePackageManager> selectTVariablePackageManagerList(TVariablePackageManager tVariablePackageManager);

    public List<TVariablePackageManager> selectTVariablePackageManagerVersionList(TVariablePackageManager tVariablePackageManager);

    /**
     * 新增变量包管理
     * 
     * @param tVariablePackageManager 变量包管理
     * @return 结果
     */
    public int insertTVariablePackageManager(TVariablePackageManager tVariablePackageManager);

    public int updatePackageVersion(TVariablePackageManager tVariablePackageManager);

    /**
     * 修改变量包管理
     * 
     * @param tVariablePackageManager 变量包管理
     * @return 结果
     */
    public int updateTVariablePackageManager(TVariablePackageManager tVariablePackageManager);

    public int updateTVariablePackageManagerOnApplication(TVariablePackageManager tVariablePackageManager);

    public int updateRuningState(ArrayList list);


    /**
     * 删除变量包管理
     * 
     * @param variablePackId 变量包管理ID
     * @return 结果
     */
    public int deleteTVariablePackageManagerById(Long variablePackId);

    /**
     * 批量删除变量包管理
     * 
     * @param variablePackIds 需要删除的数据ID
     * @return 结果
     */
    public int deleteTVariablePackageManagerByIds(Long[] variablePackIds);
}
