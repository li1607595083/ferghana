package com.skyon.project.system.domain;

import com.skyon.framework.web.domain.BaseEntity;

import java.util.Date;

/**
 * 预警配置对象 warning_config
 *
 *
 * @date 2021-06-03
 */
public class WarningConfig extends BaseEntity {

    private static final long serialVersionUID = 1L;

    /** 预警配置id */
    private Long warningId;

    /** 变量包ID */
    private Long variablePackageId;

    /** 变量包名称 */
    private String variablePackageName;

    /** 预警名称 */
    private String warningName;

    /** 预警通知方式 */
    private Object warningNoticeType;

    /** 预警状态 */
    private String warningState;

    /** 描述 */
    private String description;

//    /** 新增人 */
//    private String createBy;
//
//    /** 修改人 */
//    private String updateBy;
//
//    /** 新增时间 */
//    private Date createTime;
//
//    /** 修改时间 */
//    private Date updateTime;

    /** 预警内容 */
    private Object warningContent;

    /** 预警频率 */
    private String warningFrequency;

    /** 预警生效时间 */
    private Object warningEffectTime;

    /** 预警联系人 */
    private Object warningNoticeUser;

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public Long getWarningId() {
        return warningId;
    }

    public void setWarningId(Long warningId) {
        this.warningId = warningId;
    }

    public Long getVariablePackageId() {
        return variablePackageId;
    }

    public void setVariablePackageId(Long variablePackageId) {
        this.variablePackageId = variablePackageId;
    }

    public String getVariablePackageName() {
        return variablePackageName;
    }

    public void setVariablePackageName(String variablePackageName) {
        this.variablePackageName = variablePackageName;
    }

    public String getWarningName() {
        return warningName;
    }

    public void setWarningName(String warningName) {
        this.warningName = warningName;
    }

    public Object getWarningNoticeType() {
        return warningNoticeType;
    }

    public void setWarningNoticeType(Object warningNoticeType) {
        this.warningNoticeType = warningNoticeType;
    }

    public String getWarningState() {
        return warningState;
    }

    public void setWarningState(String warningState) {
        this.warningState = warningState;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Object getWarningContent() {
        return warningContent;
    }

    public void setWarningContent(Object warningContent) {
        this.warningContent = warningContent;
    }

    public String getWarningFrequency() {
        return warningFrequency;
    }

    public void setWarningFrequency(String warningFrequency) {
        this.warningFrequency = warningFrequency;
    }

    public Object getWarningEffectTime() {
        return warningEffectTime;
    }

    public void setWarningEffectTime(Object warningEffectTime) {
        this.warningEffectTime = warningEffectTime;
    }

    public Object getWarningNoticeUser() {
        return warningNoticeUser;
    }

    public void setWarningNoticeUser(Object warningNoticeUser) {
        this.warningNoticeUser = warningNoticeUser;
    }

    @Override
    public String toString() {
        return "WarningConfig{" +
                "warningId=" + warningId +
                ", variablePackageId=" + variablePackageId +
                ", variablePackageName='" + variablePackageName + '\'' +
                ", warningName='" + warningName + '\'' +
                ", warningNoticeType=" + warningNoticeType +
                ", warningState='" + warningState + '\'' +
                ", description='" + description + '\'' +
                ", warningContent=" + warningContent +
                ", warningFrequency='" + warningFrequency + '\'' +
                ", warningEffectTime=" + warningEffectTime +
                ", warningNoticeUser=" + warningNoticeUser +
                '}';
    }
}
