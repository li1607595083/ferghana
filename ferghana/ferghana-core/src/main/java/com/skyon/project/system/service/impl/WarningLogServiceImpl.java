package com.skyon.project.system.service.impl;

import com.skyon.project.system.domain.WarningLog;
import com.skyon.project.system.mapper.WarningLogMapper;
import com.skyon.project.system.service.WarningLogService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class WarningLogServiceImpl implements WarningLogService {

    @Autowired
    private WarningLogMapper warningLogMapper;

    /**
     * 查询预警日志列表
     *
     * @param warningLog 预警日志
     * @return 预警日志集合
     */
    @Override
    public List<WarningLog> selectWarningLogList(WarningLog warningLog) {
        return warningLogMapper.selectWarningLogList(warningLog);
    }
}
