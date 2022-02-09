package com.skyon.project.system.service;

import com.alibaba.fastjson.JSONArray;
import com.skyon.project.system.domain.TVariableCenter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 变量管理中心Service接口
 *
 *
 * @date 2020-08-06
 */
public interface ITVariableCenterService {
    /**
     * 查询变量管理中心
     *
     * @param variableId 变量管理中心ID
     * @return 变量管理中心
     */
    public TVariableCenter selectTVariableCenterById(Long variableId);

    /**
     * 查询变量管理中心列表
     *
     * @param tVariableCenter 变量管理中心
     * @return 变量管理中心集合
     */
    public List<TVariableCenter> selectTVariableCenterList(TVariableCenter tVariableCenter);

    public List<TVariableCenter> selectVariableVersionList(TVariableCenter tVariableCenter);

    /**
     * 新增变量管理中心
     *
     * @param tVariableCenter 变量管理中心
     * @return 结果
     */
    public int insertTVariableCenter(TVariableCenter tVariableCenter);

    // 拼接sql
    public String jointSqlContext(JSONArray dimensionName, TVariableCenter tVariableCenter);

    /**
     * 修改变量管理中心
     *
     * @param tVariableCenter 变量管理中心
     * @return 结果
     */
    public int updateTVariableCenter(TVariableCenter tVariableCenter);

    public int updateVersionShow(TVariableCenter tVariableCenter);

    public TVariableCenter selectTVariableCenterVersion(TVariableCenter tVariableCenter);



    /**
     * 批量删除变量管理中心
     *
     * @param variableIds 需要删除的变量管理中心ID
     * @return 结果
     */
    public int deleteTVariableCenterByIds(Long[] variableIds);

    /**
     * 删除变量管理中心信息
     *
     * @param variableId 变量管理中心ID
     * @return 结果
     */
    public int deleteTVariableCenterById(Long variableId);

    // 当有维表的时候拼接joinSQL 和 维表测试数据
    public void parseJoinSQl(Map mapParam, JSONArray dimensionRelation, Map map, ArrayList array);

    // 变量测试
    public String variableTest(TVariableCenter variable, Map map, String millis);

    //调试
    public List testRun(String paramJsonString, String millis, int size, int waterMarkTime);

    // 根据变量名获取对应的变量所需参数
    public List getCol(String names);

    public List<TVariableCenter> selectVariableCenterByIds(Long[] ids);
}
