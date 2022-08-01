package com.skyon.project.system.service.impl;

import com.skyon.common.utils.SecurityUtils;
import com.skyon.common.utils.ip.IpUtils;
import com.skyon.project.system.domain.TVariableOperateLog;
import com.skyon.project.system.mapper.TDataResultSourceMapper;
import com.skyon.project.system.mapper.TVariableOperateLogMapper;
import com.skyon.project.system.service.ITVariableOperateLogService;
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
public class TVariableOperateLogServiceImpl implements ITVariableOperateLogService {
    @Autowired
    private TVariableOperateLogMapper tVariableOperateLogMapper;


    @Override
    public List<TVariableOperateLog> selectTVariableOperateLogList(TVariableOperateLog tVariableOperateLog){
        return tVariableOperateLogMapper.selectTVariableOperateLogList(tVariableOperateLog);
    }


    /**
     * 新增变量管理中心
     *
     * @param TVariableOperateLog 变量管理中心
     * @return 结果
     */
    @Override
    public int insertTVariableOperateLog(TVariableOperateLog tVariableOperateLog){
        tVariableOperateLog.setCreateTime(new Date());
        tVariableOperateLog.setOperateAuther(SecurityUtils.getUsername());
        tVariableOperateLog.setOperateIp(IpUtils.getHostIp());
        return tVariableOperateLogMapper.insertTVariableOperateLog(tVariableOperateLog);
    }

}
