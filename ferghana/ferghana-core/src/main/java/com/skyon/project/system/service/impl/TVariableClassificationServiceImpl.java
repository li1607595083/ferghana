package com.skyon.project.system.service.impl;

import java.util.*;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.skyon.common.utils.DateUtils;
import com.skyon.common.utils.SecurityUtils;
import com.skyon.framework.aspectj.lang.annotation.DataScope;
import com.skyon.project.system.domain.TDimensionTable;
import com.skyon.project.system.mapper.TDimensionTableMapper;
import joptsimple.internal.Strings;
import org.hibernate.validator.constraints.Currency;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.skyon.project.system.mapper.TVariableClassificationMapper;
import com.skyon.project.system.domain.TVariableClassification;
import com.skyon.project.system.service.ITVariableClassificationService;

/**
 * 变量分类Service业务层处理
 *
 * @date 2020-08-21
 */
@Service
public class TVariableClassificationServiceImpl implements ITVariableClassificationService {
    @Autowired
    private TVariableClassificationMapper tVariableClassificationMapper;
    @Autowired
    private TDimensionTableMapper tDimensionTableMapper;

    /**
     * 查询变量分类
     *
     * @param variableClassificationId 变量分类ID
     * @return 变量分类
     */
    @Override
    public TVariableClassification selectTVariableClassificationById(Long variableClassificationId) {
        return tVariableClassificationMapper.selectTVariableClassificationById(variableClassificationId);
    }

    /**
     * 查询变量分类列表
     *
     * @param tVariableClassification 变量分类
     * @return 变量分类
     */
    @Override
    @DataScope(serviceTable = "1")
    public List<TVariableClassification> selectTVariableClassificationList(TVariableClassification tVariableClassification) {
        List<TVariableClassification> classifications = tVariableClassificationMapper
                .selectTVariableClassificationList(tVariableClassification);
        return classifications;
    }

    /**
     * 新增变量分类
     *
     * @param tVariableClassification 变量分类
     * @return 结果
     */
    @Override
    public int insertTVariableClassification(TVariableClassification tVariableClassification) {
        tVariableClassification.setCreateTime(DateUtils.getNowDate());
        setDimensionRelation(tVariableClassification);
        setSourceRelation(tVariableClassification);
        tVariableClassification.setCreateBy(SecurityUtils.getUsername());
        tVariableClassification.setCreateId(SecurityUtils.getUserId());
        return tVariableClassificationMapper.insertTVariableClassification(tVariableClassification);
    }

    private void setDimensionRelation(TVariableClassification tVariableClassification) {
        tVariableClassification.setDimensionRelation(JSON.toJSONString(tVariableClassification.getDimensionRelation()));
    }

    private void setSourceRelation(TVariableClassification tVariableClassification) {
        tVariableClassification.setSourceRelation(JSON.toJSONString(tVariableClassification.getSourceRelation()));
    }

    /**
     * 修改变量分类
     *
     * @param tVariableClassification 变量分类
     * @return 结果
     */
    @Override
    public int updateTVariableClassification(TVariableClassification tVariableClassification) {
        setDimensionRelation(tVariableClassification);
        setSourceRelation(tVariableClassification);
        tVariableClassification.setUpdateBy(SecurityUtils.getUsername());
        return tVariableClassificationMapper.updateTVariableClassification(tVariableClassification);
    }

    /**
     * 批量删除变量分类
     *
     * @param variableClassificationIds 需要删除的变量分类ID
     * @return 结果
     */
    @Override
    public int deleteTVariableClassificationByIds(Long[] variableClassificationIds) {
        return tVariableClassificationMapper.deleteTVariableClassificationByIds(variableClassificationIds);
    }

    /**
     * 删除变量分类信息
     *
     * @param variableClassificationId 变量分类ID
     * @return 结果
     */
    @Override
    public int deleteTVariableClassificationById(Long variableClassificationId) {
        return tVariableClassificationMapper.deleteTVariableClassificationById(variableClassificationId);
    }
}
