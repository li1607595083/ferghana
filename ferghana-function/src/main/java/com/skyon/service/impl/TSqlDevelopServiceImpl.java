package com.skyon.service.impl;

import com.skyon.domain.TSqlDevelop;
import com.skyon.mapper.TSqlDevelopMapper;
import com.skyon.service.ITSqlDevelopService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;

/**
 * SQL任务开发Service业务层处理
 *
 * @author ruoyi
 * @date 2020-06-04
 */
@Service
public class TSqlDevelopServiceImpl implements ITSqlDevelopService {
    @Autowired
    private TSqlDevelopMapper tSqlDevelopMapper;

    /**
     * 查询SQL任务开发
     *
     * @param sqlDevelopId SQL任务开发ID
     * @return SQL任务开发
     */
    @Override
    public TSqlDevelop selectTSqlDevelopById(Long sqlDevelopId) {
        return tSqlDevelopMapper.selectTSqlDevelopById(sqlDevelopId);
    }

    /**
     * 查询SQL任务开发列表
     *
     * @param tSqlDevelop SQL任务开发
     * @return SQL任务开发
     */
    @Override
    public List<TSqlDevelop> selectTSqlDevelopList(TSqlDevelop tSqlDevelop) {
        return tSqlDevelopMapper.selectTSqlDevelopList(tSqlDevelop);
    }

    /**
     * 新增SQL任务开发
     *
     * @param tSqlDevelop SQL任务开发
     * @return 结果
     */
    @Override
    public int insertTSqlDevelop(TSqlDevelop tSqlDevelop) {
//        tSqlDevelop.setCreateTime(DateUtils.getNowDate());
        return tSqlDevelopMapper.insertTSqlDevelop(tSqlDevelop);
    }

    /**
     * 升级SQL任务开发
     * 先去查询最大的版本号，本版本号再 +1
     *
     * @param tSqlDevelop SQL任务开发
     * @return 结果
     */
    @Override
    public int updateTSqlDevelop(TSqlDevelop tSqlDevelop) {
        if ("0".equals(tSqlDevelop.getRunStatus())) tSqlDevelop.setStopTime(new Date());
        else tSqlDevelop.setStartTime(new Date());
        return tSqlDevelopMapper.updateTSqlDevelop(tSqlDevelop);
    }

    /**
     * 升级SQL任务开发
     * 先去查询最大的版本号，本版本号再 +1
     *
     * @param tSqlDevelop SQL任务开发
     * @return 结果
     */
    public String insertTSqlDevelopHigh(TSqlDevelop tSqlDevelop) {
        // 先去查询最大的版本号
        String s = tSqlDevelopMapper.selectMaxVersionBySqlTaskName(tSqlDevelop);
        BigDecimal bigDecimal = new BigDecimal(s).add(new BigDecimal("1.0"));
        tSqlDevelop.setSqlTaskVersion(bigDecimal.toString());
//        tSqlDevelop.setCreateTime(DateUtils.getNowDate());
        tSqlDevelopMapper.insertTSqlDevelop(tSqlDevelop);
        return "作业名称:" + tSqlDevelop.getSqlTaskName() + ",版本:" + tSqlDevelop.getSqlTaskVersion() + " 升级成功";
    }

    public void selectBooleanStart(TSqlDevelop tSqlDevelop) {
        // 只在启动的时候检查
//        if ("1".equals(tSqlDevelop.getRunStatus())) {
//            Map s = tSqlDevelopMapper.selectBooleanStart(tSqlDevelop);
//            if (s != null && s.size() > 0) {
//                throw new CustomException("该作业中版本号为:"+s.get("sqlTaskVersion")+" 还有在启动中的，不能启动多个！");
//            }
//        }
    }

    /**
     * 批量删除SQL任务开发
     *
     * @param sqlDevelopIds 需要删除的SQL任务开发ID
     * @return 结果
     */
    @Override
    public int deleteTSqlDevelopByIds(Long[] sqlDevelopIds) {
        return tSqlDevelopMapper.deleteTSqlDevelopByIds(sqlDevelopIds);
    }

    /**
     * 删除SQL任务开发信息
     *
     * @param sqlDevelopId SQL任务开发ID
     * @return 结果
     */
    @Override
    public int deleteTSqlDevelopById(Long sqlDevelopId) {
        return tSqlDevelopMapper.deleteTSqlDevelopById(sqlDevelopId);
    }

    /**
     * @param tSqlDevelop SQL任务 实体
     * @return 对应sql作业名最大的版本号
     */
    @Override
    public String selectMaxVersionBySqlTaskName(TSqlDevelop tSqlDevelop) {
        return tSqlDevelopMapper.selectMaxVersionBySqlTaskName(tSqlDevelop);
    }

    /**
     * @param tSqlDevelop 参数实体
     */
    @Override
    public void decideTaskNameBySqlTaskName(TSqlDevelop tSqlDevelop) {
//        String s = tSqlDevelopMapper.selectMaxVersionBySqlTaskName(tSqlDevelop);
//        if (!Strings.isNullOrEmpty(s)) {
//            throw new CustomException("该作业名称【"+tSqlDevelop.getSqlTaskName()+"】已存在！");
//        }
    }
}
