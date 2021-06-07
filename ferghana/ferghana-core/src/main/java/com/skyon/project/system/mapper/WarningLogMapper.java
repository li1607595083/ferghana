package com.skyon.project.system.mapper;

import com.skyon.project.system.domain.WarningLog;

import java.util.List;

/**
 * 预警日志Mapper接口
 *
 *
 * @date 2021-06-07
 */
public interface WarningLogMapper {

    /**
     * 查询预警日志列表
     *
     * @param warningLog 预警日志
     * @return 预警日志集合
     */
    public List<WarningLog> selectWarningLogList(WarningLog warningLog);
}
