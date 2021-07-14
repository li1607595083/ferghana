package com.skyon.project.system.service;

import com.skyon.project.system.domain.WarningLog;

import java.util.List;
import java.util.Map;

/**
 * 预警日志Service接口
 *
 * @date 2021-06-07
 */
public interface WarningLogService {

    /**
     * 查询预警日志列表
     *
     * @param warningLog 预警日志
     * @return 预警日志集合
     */
    public List<WarningLog> selectWarningLogList(WarningLog warningLog);

    /**
     * 新增预警日志
     *
     * @param warningLog 预警日志
     * @return 新增预警日志条数
     */
    public int insertWarningLogList(WarningLog warningLog);

    /**
     * 查询上一次预警距离现在的时间
     *
     * @param warningId 预警Id
     * @return 上一次预警距离现在的秒数
     */
    public Map checkWarningTime(Long warningId);
}
