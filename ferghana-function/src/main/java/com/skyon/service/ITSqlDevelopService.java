package com.skyon.service;


import com.skyon.domain.TSqlDevelop;

import java.util.List;

/**
 * SQL任务开发Service接口
 * 
 * @author ruoyi
 * @date 2020-06-04
 */
public interface ITSqlDevelopService 
{
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
    public int updateTSqlDevelop(TSqlDevelop tSqlDevelop);

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
     * @param tSqlDevelop
     * @return
     */
    public String selectMaxVersionBySqlTaskName(TSqlDevelop tSqlDevelop);

    /**
     * 判断该作业名称是否已经存在
     * @param tSqlDevelop
     * @return
     */
    public void decideTaskNameBySqlTaskName(TSqlDevelop tSqlDevelop);
}
