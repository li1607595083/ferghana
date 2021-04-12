package com.skyon.project.system.service;

import java.lang.reflect.MalformedParameterizedTypeException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.skyon.project.system.domain.TVariableCenter;
import com.skyon.project.system.domain.TVariablePackageManager;

/**
 * 变量包管理Service接口
 *
 * @date 2020-08-24
 */
public interface ITVariablePackageManagerService {
    /**
     * 查询变量包管理
     *
     * @param variablePackId 变量包管理ID
     * @return 变量包管理
     */
    public TVariablePackageManager selectTVariablePackageManagerById(Long variablePackId);

    /**
     * 查询变量包管理列表
     *
     * @param tVariablePackageManager 变量包管理
     * @return 变量包管理集合
     */
    public List<TVariablePackageManager> selectTVariablePackageManagerList(TVariablePackageManager tVariablePackageManager);

    public List<TVariablePackageManager> selectTVariablePackageManagerVersionList(TVariablePackageManager tVariablePackageManager);

    /**
     * 新增变量包管理
     *
     * @param tVariablePackageManager 变量包管理
     * @return 结果
     */
    public int insertTVariablePackageManager(Map map, TVariablePackageManager tVariablePackageManager, String runFlag);

    public int updatePackageVersion(TVariablePackageManager tVariablePackageManager);

    /**
     * 修改变量包管理
     *
     * @param tVariablePackageManager 变量包管理
     * @return 结果
     */
    public int updateTVariablePackageManager(Map map,TVariablePackageManager tVariablePackageManager);

    public int updateTVariablePackageManagerOnApplication(TVariablePackageManager tVariablePackageManager);

    public int updateRuningState(ArrayList list);

    public int updateStatus(TVariablePackageManager tVariablePackageManager);

    public TVariablePackageManager selectTVariablePackageManagerByVersion(TVariablePackageManager tVariablePackageManager);

    /**
     * 批量删除变量包管理
     *
     * @param variablePackIds 需要删除的变量包管理ID
     * @return 结果
     */
    public int deleteTVariablePackageManagerByIds(Long[] variablePackIds);

    /**
     * 删除变量包管理信息
     *
     * @param variablePackId 变量包管理ID
     * @return 结果
     */
    public int deleteTVariablePackageManagerById(Long variablePackId);

    // 根据变量分类查数据源表主键
    public Map getKeyByVariableId(Long variableId);

    public List<TVariableCenter> getVariableListByIds(Object variableIds);

    public String[] joinPath(Map map, TVariablePackageManager pkManager, List<TVariableCenter> variableListByIds);

    // 获取结果
    public List getTestResult(Object jobId);

    // test
    public List testRun(String paramJsonString);

    // 组装测试参数
    public String variableTest(TVariablePackageManager pk, Map map, List<TVariableCenter> variableListByIds, String runFlag, String millis);

    // 启动
    public Map exe(String[] path);

    // 停止作业
    public Map<String,String> jobKill(TVariablePackageManager pk);


}
