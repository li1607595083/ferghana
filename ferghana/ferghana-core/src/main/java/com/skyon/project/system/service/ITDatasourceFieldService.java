package com.skyon.project.system.service;

import com.skyon.project.system.domain.TDataSource;
import com.skyon.project.system.domain.TDatasourceField;
import com.skyon.project.system.domain.TVariableCenter;
import org.apache.poi.ss.formula.functions.T;

import java.util.List;
import java.util.Map;

/**
 * t_datasource_field   Service接口
 * 
 *
 * @date 2020-05-21
 */
public interface ITDatasourceFieldService
{

    // 查询集合
    public List<TDatasourceField> selectTDatasourceFieldList(TDatasourceField tDatasourceField);


    // 修改字段 为 已经使用
    public  void updateFiledIsUsed(TVariableCenter tVariableCenter);

    // 新增字段 tableType ：01 数据源表 02 数据维表
    public int insertTDatasourceField(String tableName, Object fields, String tableType,String connectorType);

    //
    public void updateIsUsed(TVariableCenter tVariableCenter);

    // 根据变量英文名查询已经使用的字段
    public void updateFieldAndIsUsed(TVariableCenter tVariableCenter);

    // 删除
    public void deleteTDatasourceField(String[] tableNames, String tableType);

    // 修改字段名
    public void updatefieldName(Object fields, String tableType, String connectorType,String tableName);

}
