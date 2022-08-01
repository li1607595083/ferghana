package com.skyon.project.system.service;

import com.skyon.project.system.domain.TDataResultSource;
import com.skyon.project.system.domain.TDataSource;
import com.skyon.project.system.domain.TSqlDevelop;

import java.util.List;
import java.util.Map;

/**
 * SQL任务开发Service接口
 *
 *
 * @date 2020-06-04
 */
public interface ITSqlDevelopService {
    /**
     * 查询SQL任务开发
     *
     * @param sqlDevelopId SQL任务开发ID
     * @return SQL任务开发
     */
    public TSqlDevelop selectTSqlDevelopById(Long sqlDevelopId);

    /**
     * 查询SQL任务开发列表
     *
     * @param tSqlDevelop SQL任务开发
     * @return SQL任务开发集合
     */
    public List<TSqlDevelop> selectTSqlDevelopList(TSqlDevelop tSqlDevelop);

    /**
     * 新增SQL任务开发
     *
     * @param tSqlDevelop SQL任务开发
     * @return 结果
     */
    public int insertTSqlDevelop(TSqlDevelop tSqlDevelop);

    /**
     * 修改SQL任务开发
     *
     * @param tSqlDevelop SQL任务开发
     * @return 结果
     */
    public int updateTSqlDevelop(TSqlDevelop tSqlDevelop, Map<String, String> map);

    /**
     * 修改SQL任务开发
     *
     * @param tSqlDevelop SQL任务开发
     * @return 结果
     */
    public String insertTSqlDevelopHigh(TSqlDevelop tSqlDevelop);

    public void selectBooleanStart(TSqlDevelop tSqlDevelop);

    /**
     * 批量删除SQL任务开发
     *
     * @param sqlDevelopIds 需要删除的SQL任务开发ID
     * @return 结果
     */
    public int deleteTSqlDevelopByIds(Long[] sqlDevelopIds);

    /**
     * 删除SQL任务开发信息
     *
     * @param sqlDevelopId SQL任务开发ID
     * @return 结果
     */
    public int deleteTSqlDevelopById(Long sqlDevelopId);

    /**
     * 根据sql作业名查询最大的版本号
     *
     * @param tSqlDevelop
     * @return
     */
    public String selectMaxVersionBySqlTaskName(TSqlDevelop tSqlDevelop);

    /**
     * 判断该作业名称是否已经存在
     *
     * @param tSqlDevelop
     * @return
     */
    public void decideTaskNameBySqlTaskName(TSqlDevelop tSqlDevelop);

    /**
     * 启动作业
     *
     * @param sourceTableId 通常情况下就一个数据源表
     * @param sinkTableIds  可以有多个数据结果表，多个数据结果表用逗号拼接
     * @param sqls          可以有多个sql,多个sql用分号拼接
     * @param jobName
     */
    public Map<String, String> jobSubmit(String sourceTableId, String sinkTableIds, String sqls, String jobName, String testRun);

    // 启动入口
    public Map exe(String[] path);

    /**
     * 停止作业
     *
     * @param applicationId
     */
    public void jobKill(String applicationId);

    /**
     * @param sourceTableId 通常情况下就一个数据源表
     * @param sinkTableIds  可以有多个数据结果表，多个数据结果表用逗号拼接
     * @param sqls          可以有多个sql,多个sql用分号拼接
     * @param jobName       job名称
     * @param testRun       1代表调试运行，0代表正式运行
     * @param param         调试输入参数，多条参数用逗号拼接
     */
    public Map testRun(String sourceTableId, String sinkTableIds, String sqls,
                       String jobName, String testRun, String param[],
                       TDataSource tDataSource, List<TDataResultSource> tDataResultSources);

    // 查询所有已启动的作业 application id
    public Map selectTSqlDevelopByRunStatus();
}
