package com.skyon.project.system.service.impl;

import com.alibaba.fastjson.JSON;
import com.skyon.common.utils.SecurityUtils;
import com.skyon.project.system.domain.WarningConfig;
import com.skyon.project.system.mapper.WarningConfigMapper;
import com.skyon.project.system.service.WarningConfigService;
import com.skyon.project.system.util.WarningTaskUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class WarningConfigServiceImpl implements WarningConfigService {

    @Autowired
    private WarningConfigMapper warningConfigMapper;

    /**
     * 查询预警配置列表
     *
     * @param warningConfig 预警配置
     * @return 预警配置集合
     */
    @Override
    public List<WarningConfig> selectWarningConfigList(WarningConfig warningConfig) {
        List<WarningConfig> list = warningConfigMapper.selectWarningConfigList(warningConfig);
        return list;
    }

    /**
     * 启动预警
     *
     * @param warningId 预警Id
     * @return 启动预警结果
     */
    @Override
    public int runWarningConfig(Long warningId) {
        // 获取预警参数
        Map map = warningConfigMapper.selectWarningConfigMapById(warningId);
        boolean bool = WarningTaskUtil.startWarningTask(map);
        // 预警线程启动失败，直接返回0
        return bool ? warningConfigMapper.runWarningConfig(warningId) : 0;
    }

    @Override
    public int stopWarningConfig(Long warningId) {
        boolean bool = WarningTaskUtil.stopWarningTask(String.valueOf(warningId));
        return bool ? warningConfigMapper.stopWarningConfig(warningId) : 0;
    }

    /**
     * 新增预警规则
     *
     * @param warningConfig 预警配置
     * @return 新增数据条数
     */
    @Override
    public int insertWarningConfig(WarningConfig warningConfig) {
        warningConfig.setCreateBy(SecurityUtils.getUsername());
        warningConfig.setWarningContent(JSON.toJSONString(warningConfig.getWarningContent()));
        warningConfig.setWarningEffectTime(JSON.toJSONString(warningConfig.getWarningEffectTime()));
        warningConfig.setWarningNoticeType(JSON.toJSONString(warningConfig.getWarningNoticeType()));
        warningConfig.setWarningNoticeUser(JSON.toJSONString(warningConfig.getWarningNoticeUser()));
        return warningConfigMapper.insertWarningConfig(warningConfig);
    }

    /**
     * 新增预警规则
     *
     * @param warningConfig 预警配置
     * @return 新增数据条数
     */
    @Override
    public int updateWarningConfig(WarningConfig warningConfig) {
        warningConfig.setUpdateBy(SecurityUtils.getUsername());
        warningConfig.setWarningContent(JSON.toJSONString(warningConfig.getWarningContent()));
        warningConfig.setWarningEffectTime(JSON.toJSONString(warningConfig.getWarningEffectTime()));
        warningConfig.setWarningNoticeType(JSON.toJSONString(warningConfig.getWarningNoticeType()));
        warningConfig.setWarningNoticeUser(JSON.toJSONString(warningConfig.getWarningNoticeUser()));
        return warningConfigMapper.updateWarningConfig(warningConfig);
    }

    /**
     * 查询预警详情
     *
     * @param warningId 预警Id
     * @return 预警详情
     */
    @Override
    public WarningConfig selectWarningConfigById(Long warningId) {
        return warningConfigMapper.selectWarningConfigById(warningId);
    }

    /**
     * 删除预警规则
     *
     * @param warningIds 预警Id列表
     * @return 删除预警条数
     */
    @Override
    public int deleteWarningConfigByIds(Long[] warningIds) {
        for(int i=0;i<warningIds.length;i++){
            // 删除预警之前先停止预警
            stopWarningConfig(warningIds[i]);
        }
        return warningConfigMapper.deleteWarningConfigByIds(warningIds);
    }

    @Override
    public List<Map> selectWarningConfigMapList(){
        return warningConfigMapper.selectWarningConfigMapList();
    }
}
