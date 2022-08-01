package com.skyon.project.system.service;

import com.skyon.project.system.domain.WarningConfig;

import java.util.List;
import java.util.Map;

/**
 * 预警配置Service接口
 *
 *
 * @date 2021-06-02
 */
public interface WarningConfigService
{
    /**
     * 查询预警配置列表
     *
     * @param warningConfig 预警配置
     * @return 预警配置集合
     */
    public List<WarningConfig> selectWarningConfigList(WarningConfig warningConfig);

    public int runWarningConfig(Long warningId);

    public int stopWarningConfig(Long warningId);

    /**
     * 添加预警规则
     *
     * @param warningConfig 预警配置
     * @return 新增数据条数
     */
    public int insertWarningConfig(WarningConfig warningConfig);

    /**
     * 修改预警规则
     *
     * @param warningConfig 预警配置
     * @return 修改数据条数
     */
    public int updateWarningConfig(WarningConfig warningConfig);

    /**
     * 查询预警详情
     *
     * @param warningId 预警Id
     * @return 预警详情
     */
    public WarningConfig selectWarningConfigById(Long warningId);

    /**
     * 删除预警规则
     *
     * @param warningIds 预警Id列表
     * @return 删除预警条数
     */
    public int deleteWarningConfigByIds(Long[] warningIds);

    public List<Map> selectWarningConfigMapList();
}
