package com.skyon.project.system.mapper;

import com.skyon.project.system.domain.OperationMonitor;
import org.apache.zookeeper.Op;

import java.util.List;

public interface OperationMonitorMapper {

    public int insertOperationMonitor(OperationMonitor operationMonitor);

    public List<OperationMonitor> selectOperationMonitor(OperationMonitor operationMonitor);

    public int deleteOperationMonitor();
}
