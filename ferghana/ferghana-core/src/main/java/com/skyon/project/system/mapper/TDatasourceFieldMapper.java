package com.skyon.project.system.mapper;

import com.skyon.project.system.domain.TDatasourceField;
import org.apache.ibatis.annotations.Param;

import java.util.List;
import java.util.Map;

/**
 * t_datasource_field Mapper接口
 * 
 *
 * @date 2020-05-21
 */
public interface TDatasourceFieldMapper
{

    public List<TDatasourceField> selectTDatasourceFieldList(TDatasourceField tDatasourceField);

    // 根据表名查询字段
    public List<TDatasourceField> selectTDatasourceFieldByName(@Param("tableName") String tableName, @Param("tableType") String tableType);

    // 根据id 和 字段名修改 字段为已使用
    public void updateFieldNameIsUsed(Map map);

    // 插入多条数据
    public int insertTDatasourceField(List list);

    // 根据 t_datasource_field 的 id , 修改 is_used 为 0， used_variable_id 应该为空了.
    public void updateIsUsedTableType01ById(Map map);

    // 根据 t_datasource_field 的 id  将 used_variable_id 中的 name  替换为''
    public void updateIsUsed1ById(Map map);

    // 根据变量英文名查询已经使用的字段
    public List selectFieldBYVariableEN(Map map);

    // 根据变量名 查询已经使用的数据维表字段
    public List selectDimensionField(String variableName);

    // 删除
    public void deleteTdatasourceField(Map map);

    public void updateFieldName(Object[] array);

    // 修改数据维表的字段
    public void updateDimensionTableFieldNameIsUsed(List list);

    // 删除变量时修改t_datasource_field里的维表字段
    public void updateWhenDelete(List list);

}
