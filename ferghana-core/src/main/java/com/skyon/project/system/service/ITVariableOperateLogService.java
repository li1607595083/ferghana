package com.skyon.project.system.service;

import com.skyon.project.system.domain.TVariableOperateLog;

import java.util.List;

/**
 * 变量管理中心Service接口
 *
 *
 * @date 2020-08-06
 */
public interface ITVariableOperateLogService {
    public List<TVariableOperateLog> selectTVariableOperateLogList(TVariableOperateLog tVariableOperateLog);


    /**
     * 新增变量管理中心
     *
     * @param TVariableOperateLog 变量管理中心
     * @return 结果
     */
    public int insertTVariableOperateLog(TVariableOperateLog tVariableOperateLog);

}
