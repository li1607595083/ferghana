package com.skyon.framework.web.domain;

import com.skyon.common.enums.SsoStatus;
import com.skyon.common.utils.StringUtils;

import java.util.HashMap;

/**
 *
 * 单点登录操作提醒
 * @author zhouminfeng
 */
public class SsoResult extends HashMap<String, Object>
{
    private static final long serialVersionUID = 1L;

    /** 状态码 */
    public static final String CODE_TAG = "code";

    /** 返回内容 */
    public static final String MSG_TAG = "msg";

    /** 数据对象 */
    public static final String DATA_TAG = "data";

    /**
     * 初始化一个新创建的 AjaxResult 对象，使其表示一个空消息。
     */
    public SsoResult()
    {
    }

    /**
     * 初始化一个新创建的 AjaxResult 对象
     *
     * @param code 状态码
     * @param msg 返回内容
     */
    public SsoResult(String code, String msg)
    {
        super.put(CODE_TAG, code);
        super.put(MSG_TAG, msg);
    }

    /**
     * 初始化一个新创建的 AjaxResult 对象
     *
     * @param code 状态码
     * @param msg 返回内容
     * @param data 数据对象
     */
    public SsoResult(String code, String msg, Object data)
    {
        super.put(CODE_TAG, code);
        super.put(MSG_TAG, msg);
        if (StringUtils.isNotNull(data))
        {
            super.put(DATA_TAG, data);
        }
    }

    /**
     * 返回成功消息
     * 
     * @return 成功消息
     */
    public static SsoResult success()
    {
        return SsoResult.success("操作成功");
    }

    /**
     * 返回成功数据
     * 
     * @return 成功消息
     */
    public static SsoResult success(Object data)
    {
        return SsoResult.success("操作成功", data);
    }

    /**
     * 返回成功消息
     * 
     * @param msg 返回内容
     * @return 成功消息
     */
    public static SsoResult success(String msg)
    {
        return SsoResult.success(msg, null);
    }

    /**
     * 返回成功消息
     * 
     * @param msg 返回内容
     * @param data 数据对象
     * @return 成功消息
     */
    public static SsoResult success(String msg, Object data)
    {
        return new SsoResult(SsoStatus.OK.name(), msg, data);
    }

    /**
     * 返回错误消息
     * 
     * @return
     */
    public static SsoResult error()
    {
        return SsoResult.error("操作失败");
    }

    /**
     * 返回错误消息
     * 
     * @param msg 返回内容
     * @return 警告消息
     */
    public static SsoResult error(String msg)
    {
        return SsoResult.error(msg, null);
    }

    /**
     * 返回错误消息
     * 
     * @param msg 返回内容
     * @param data 数据对象
     * @return 警告消息
     */
    public static SsoResult error(String msg, Object data)
    {
        return new SsoResult(SsoStatus.ERROR.name(), msg, data);
    }

    /**
     * 返回错误消息
     * 
     * @param code 状态码
     * @param msg 返回内容
     * @return 警告消息
     */
    public static SsoResult error(String code, String msg)
    {
        return new SsoResult(code, msg, null);
    }
}
