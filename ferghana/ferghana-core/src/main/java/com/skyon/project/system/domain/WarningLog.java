package com.skyon.project.system.domain;

/**
 * 预警日志对象 warning_config
 *
 *
 * @date 2021-06-06
 */
public class WarningLog {

    private static final long serialVersionUID = 1L;

    /** 预警日志Id */
    private Long warningLogId;

    /** 预警名称 */
    private String warningName;

    /** 变量包名称 */
    private String variablePackageName;

    /** 预警方式 */
    private Object warningNoticeType;

    /** 预警联系人 */
    private Object warningNoticeUser;

    /** 预警内容 */
    private Object warningContent;

    /** 预警时间 */
    private String warningLogTime;

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public Long getWarningLogId() {
        return warningLogId;
    }

    public void setWarningLogId(Long warningLogId) {
        this.warningLogId = warningLogId;
    }

    public String getWarningName() {
        return warningName;
    }

    public void setWarningName(String warningName) {
        this.warningName = warningName;
    }

    public String getVariablePackageName() {
        return variablePackageName;
    }

    public void setVariablePackageName(String variablePackageName) {
        this.variablePackageName = variablePackageName;
    }

    public Object getWarningNoticeType() {
        return warningNoticeType;
    }

    public void setWarningNoticeType(Object warningNoticeType) {
        this.warningNoticeType = warningNoticeType;
    }

    public Object getWarningNoticeUser() {
        return warningNoticeUser;
    }

    public void setWarningNoticeUser(Object warningNoticeUser) {
        this.warningNoticeUser = warningNoticeUser;
    }

    public Object getWarningContent() {
        return warningContent;
    }

    public void setWarningContent(Object warningContent) {
        this.warningContent = warningContent;
    }

    public String getWarningLogTime() {
        return warningLogTime;
    }

    public void setWarningLogTime(String warningLogTime) {
        this.warningLogTime = warningLogTime;
    }

    @Override
    public String toString() {
        return "WarningLog{" +
                "warningLogId=" + warningLogId +
                ", warningName='" + warningName + '\'' +
                ", variablePackageName='" + variablePackageName + '\'' +
                ", warningNoticeType=" + warningNoticeType +
                ", warningNoticeUser=" + warningNoticeUser +
                ", warningContent=" + warningContent +
                ", warningLogTime='" + warningLogTime + '\'' +
                '}';
    }
}
