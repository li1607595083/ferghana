package com.skyon.project.system.domain;

import com.skyon.framework.web.domain.BaseEntity;

import java.util.Date;

/**
 * 变量管理中心对象 t_variable_center
 *
 *
 * @date 2020-08-06
 */
public class TVariablePackageOperateLog extends BaseEntity {
    private static final long serialVersionUID = 1L;

    /**
     * $column.columnComment
     */
    private Long tVariablePackOperateId;

    private String variablePackNameEn;

    private String operateAuther;

    private String operateIp;

    private String operateType;

    private Date createTime;

    public Long gettVariablePackOperateId() {
        return tVariablePackOperateId;
    }

    public void settVariablePackOperateId(Long tVariablePackOperateId) {
        this.tVariablePackOperateId = tVariablePackOperateId;
    }

    public String getVariablePackNameEn() {
        return variablePackNameEn;
    }

    public void setVariablePackNameEn(String variablePackNameEn) {
        this.variablePackNameEn = variablePackNameEn;
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
