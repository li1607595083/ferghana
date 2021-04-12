package com.skyon.mapper;



import com.skyon.domain.TSqlDevelop;

import java.util.List;
import java.util.Map;

/**
 * SQL任务开发Mapper接口
 * 
 * @author ruoyi
 * @date 2020-06-04
 */
public interface TSqlDevelopMapper 
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
     * 查询该作业已启动的id
     *
     * @param tSqlDevelop SQL任务开发
     * @return 结果
     */
    public Map selectBooleanStart(TSqlDevelop tSqlDevelop);

    /**
     * 修改SQL任务开发
     * 
     * @param tSqlDevelop SQL任务开发
     * @return 结果
     */
    public int updateTSqlDevelop(TSqlDevelop tSqlDevelop);

    /**
     * 删除SQL任务开发
     * 
     * @param sqlDevelopId SQL任务开发ID
     * @return 结果
     */
    public int deleteTSqlDevelopById(Long sqlDevelopId);

    /**
     * 批量删除SQL任务开发
     * 
     * @param sqlDevelopIds 需要删除的数据ID
     * @return 结果
     */
    public int deleteTSqlDevelopByIds(Long[] sqlDevelopIds);

    /**
     * 根据sql作业名查询最大的版本号
     * @param tSqlDevelop
     * @return
     */
    public String selectMaxVersionBySqlTaskName(TSqlDevelop tSqlDevelop);
}
