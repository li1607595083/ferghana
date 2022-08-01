package com.skyon.project.system.mapper;

import com.skyon.project.system.domain.SysSsoApp;
import org.apache.ibatis.annotations.Param;

/**
 * 单点登录系统配置
 * @author zhouminfeng
 */
public interface SysSsoAppMapper
{

    /**
     * 通过应用系统appId查找应用配置信息
     * @param appId
     * @return
     */
    public SysSsoApp findAppByAppId(@Param("appId") String appId);
}
