package com.skyon.common.enums;

/**
 * 单点登录状态
 * @author zhouminfeng
 */
public enum SsoStatus
{
    OK("操作成功"),
    ERROR("系统错误"),
    invalidIp("调用者ip地址非法"),
    invalidApp("调用者标识或访问密码错误"),
    invalidUserAccount("无效的用户账号"),
    invalidToken("TOKEN无效"),
    differToken("TOKEN校验不通过"),
    loginSucess("单点登录成功")
    ;

    private final String message;

    SsoStatus(String message)
    {
        this.message = message;
    }

    public String getMessage()
    {
        return message;
    }
}
