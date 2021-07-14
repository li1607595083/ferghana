package com.skyon.project.system.service.impl;

import com.skyon.project.system.domain.WarningLog;
import com.skyon.project.system.mapper.WarningLogMapper;
import com.skyon.project.system.service.WarningLogService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

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

    /**
     * 新增预警日志
     *
     * @param warningLog 预警日志
     * @return 新增预警日志条数
     */
    @Override
    public int insertWarningLogList(WarningLog warningLog) {
        return warningLogMapper.insertWarningLog(warningLog);
    }

    /**
     * 查询上一次预警距离现在的时间
     *
     * @param warningId 预警Id
     * @return 上一次预警距离现在的秒数
     */
    @Override
    public Map checkWarningTime(Long warningId){
        return warningLogMapper.checkWarningTime(warningId);
    }
}
