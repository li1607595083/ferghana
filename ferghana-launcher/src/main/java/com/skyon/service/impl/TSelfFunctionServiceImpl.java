package com.skyon.service.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.skyon.domain.TSelfFunction;
import com.skyon.mapper.TSelfFunctionMapper;
import com.skyon.service.ITSelfFunctionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 自定义函数Service业务层处理
 *
 * @author ruoyi
 * @date 2020-06-18
 */
@Service
public class TSelfFunctionServiceImpl implements ITSelfFunctionService {
    @Autowired
    private TSelfFunctionMapper tSelfFunctionMapper;

    /**
     * 查询自定义函数
     *
     * @param selfFunctionId 自定义函数ID
     * @return 自定义函数
     */
    @Override
    public TSelfFunction selectTSelfFunctionById(Long selfFunctionId) {
        TSelfFunction tSelfFunction = tSelfFunctionMapper.selectTSelfFunctionById(selfFunctionId);
        return tSelfFunction;
    }


    /**
     * 查询自定义函数列表
     *
     * @param tSelfFunction 自定义函数
     * @return 自定义函数
     */
    @Override
    public List<TSelfFunction> selectTSelfFunctionList(TSelfFunction tSelfFunction) {
        return tSelfFunctionMapper.selectTSelfFunctionList(tSelfFunction);
    }

    /**
     * 新增自定义函数
     *
     * @param tSelfFunction 自定义函数
     * @return 结果
     */
    @Override
    public int insertTSelfFunction(TSelfFunction tSelfFunction) {
        return tSelfFunctionMapper.insertTSelfFunction(tSelfFunction);
    }

    /**
     * 修改自定义函数
     *
     * @param tSelfFunction 自定义函数
     * @return 结果
     */
    @Override
    public int updateTSelfFunction(TSelfFunction tSelfFunction) {
        tSelfFunction.setUpdateTime(new Date());
        return tSelfFunctionMapper.updateTSelfFunction(tSelfFunction);
    }

    /**
     * 批量删除自定义函数
     *
     * @param selfFunctionIds 需要删除的自定义函数ID
     * @return 结果
     */
    @Override
    public int deleteTSelfFunctionByIds(Long[] selfFunctionIds) {
        return tSelfFunctionMapper.deleteTSelfFunctionByIds(selfFunctionIds);
    }

    /**
     * 删除自定义函数信息
     *
     * @param selfFunctionId 自定义函数ID
     * @return 结果
     */
    @Override
    public int deleteTSelfFunctionById(Long selfFunctionId) {
        return tSelfFunctionMapper.deleteTSelfFunctionById(selfFunctionId);
    }
}
