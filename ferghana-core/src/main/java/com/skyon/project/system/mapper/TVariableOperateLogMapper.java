package com.skyon.project.system.mapper;

import com.skyon.project.system.domain.TVariableOperateLog;

import java.util.List;

/**
 * 变量管理中心Mapper接口
 * 
 *
 * @date 2020-08-06
 */
public interface TVariableOperateLogMapper
{
    /**
     * 查询变量管理中心列表
     * 
     * @param TVariableOperateLog 变量管理中心
     * @return 变量管理中心集合
     */
    public List<TVariableOperateLog> selectTVariableOperateLogList(TVariableOperateLog tVariableOperateLog);


    /**
     * 新增变量管理中心
     * 
     * @param TVariableOperateLog 变量管理中心
     * @return 结果
     */
    public int insertTVariableOperateLog(TVariableOperateLog tVariableOperateLog);

}
