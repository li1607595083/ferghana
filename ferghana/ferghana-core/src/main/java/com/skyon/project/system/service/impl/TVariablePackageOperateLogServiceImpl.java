package com.skyon.project.system.service.impl;

import com.skyon.common.utils.SecurityUtils;
import com.skyon.common.utils.ip.IpUtils;
import com.skyon.project.system.domain.TVariableOperateLog;
import com.skyon.project.system.domain.TVariablePackageManager;
import com.skyon.project.system.domain.TVariablePackageOperateLog;
import com.skyon.project.system.mapper.TVariableOperateLogMapper;
import com.skyon.project.system.mapper.TVariablePackageOperateLogMapper;
import com.skyon.project.system.service.ITVariableOperateLogService;
import com.skyon.project.system.service.ITVariablePackageOperateLogService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.List;

/**
 * 变量管理中心Service接口
 *
 *
 * @date 2020-08-06
 */
@Service
public class TVariablePackageOperateLogServiceImpl implements ITVariablePackageOperateLogService {
    @Autowired
    private TVariablePackageOperateLogMapper logMapper;


    @Override
    public List<TVariablePackageOperateLog> selectTVariablePackageOperateLogList(String variablePackNameEn){
        return logMapper.selectTVariablePackageOperateLogList(variablePackNameEn);
    }


    /**
     * 新增变量管理中心
     *
     * @param TVariableOperateLog 变量管理中心
     * @return 结果
     */
    @Override
    public int insertTVariablePackageOperateLog(TVariablePackageOperateLog tVariablePackageOperateLog){
        tVariablePackageOperateLog.setCreateTime(new Date());
        tVariablePackageOperateLog.setOperateAuther(SecurityUtils.getUsername());
        tVariablePackageOperateLog.setOperateIp(IpUtils.getHostIp());
        return logMapper.insertTVariablePackageOperateLog(tVariablePackageOperateLog);
    }

}
