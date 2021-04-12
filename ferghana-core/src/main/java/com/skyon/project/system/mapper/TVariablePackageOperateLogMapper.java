package com.skyon.project.system.mapper;

import com.skyon.project.system.domain.TVariablePackageOperateLog;

import java.util.List;

/**
 * 变量管理中心Mapper接口
 * 
 *
 * @date 2020-08-06
 */
public interface TVariablePackageOperateLogMapper
{
    /**
     * 查询变量管理中心列表
     * 
     * @param TVariableOperateLog 变量管理中心
     * @return 变量管理中心集合
     */
    public List<TVariablePackageOperateLog> selectTVariablePackageOperateLogList(String variablePackNameEn);


    /**
     * 新增变量管理中心
     * 
     * @param TVariableOperateLog 变量管理中心
     * @return 结果
     */
    public int insertTVariablePackageOperateLog(TVariablePackageOperateLog tVariablePackageOperateLog);

}
