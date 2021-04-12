package com.skyon.project.system.service;

import com.skyon.project.system.domain.TVariableOperateLog;
import com.skyon.project.system.domain.TVariablePackageOperateLog;

import java.util.List;

/**
 * 变量管理中心Service接口
 *
 *
 * @date 2020-08-06
 */
public interface ITVariablePackageOperateLogService {

    public List<TVariablePackageOperateLog> selectTVariablePackageOperateLogList(String variablePackNameEn);


    /**
     * 新增变量管理中心
     *
     * @param TVariableOperateLog 变量管理中心
     * @return 结果
     */
    public int insertTVariablePackageOperateLog(TVariablePackageOperateLog tVariablePackageOperateLog);

}
