package com.skyon.project.system.domain;

import com.skyon.framework.web.domain.BaseEntity;

import java.util.Date;

/**
 * 变量管理中心对象 t_variable_center
 *
 *
 * @date 2020-08-06
 */
public class TVariableOperateLog extends BaseEntity {
    private static final long serialVersionUID = 1L;

    /**
     * $column.columnComment
     */
    private Long tVariableOperateId;

    private String variableNameEn;

    private String operateAuther;

    private String operateIp;

    private String operateType;

    private Date createTime;

    public Long gettVariableOperateId() {
        return tVariableOperateId;
    }

    public void settVariableOperateId(Long tVariableOperateId) {
        this.tVariableOperateId = tVariableOperateId;
    }

    public String getVariableNameEn() {
        return variableNameEn;
    }

    public void setVariableNameEn(String variableNameEn) {
        this.variableNameEn = variableNameEn;
    }

    public String getOperateAuther() {
        return operateAuther;
    }

    public void setOperateAuther(String operateAuther) {
        this.operateAuther = operateAuther;
    }

    public String getOperateIp() {
        return operateIp;
    }

    public void setOperateIp(String operateIp) {
        this.operateIp = operateIp;
    }

    public String getOperateType() {
        return operateType;
    }

    public void setOperateType(String operateType) {
        this.operateType = operateType;
    }

    @Override
    public Date getCreateTime() {
        return createTime;
    }

    @Override
    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }
}
