package com.skyon.project.system.mapper;


import java.util.List;

/**
 * t_datasource_field Mapper接口
 * 
 *
 * @date 2020-05-21
 */
public interface TDimensionFieldMapper
{

//    public List<TDatasourceField> selectTDatasourceFieldList(TDatasourceField tDatasourceField);
//
//    // 根据表名查询字段
//    public List<TDatasourceField> selectTDatasourceFieldByName(String tableName);
//
//    // 根据id 和 字段名修改 字段为已使用
//    public void updateFieldNameIsUsed(Map map);

    // 插入多条数据
    public int insertTDimensionField(List list);

//    // 根据 t_datasource_field 的 id , 修改 is_used 为 0， used_variable_id 应该为空了.
//    public void updateIsUsed0ById(Map map);
//
//    // 根据 t_datasource_field 的 id  将 used_variable_id 中的 name  替换为''
//    public void updateIsUsed1ById(Map map);
//
//    // 根据变量英文名查询已经使用的字段
//    public List selectFieldBYVariableEN(Map map);
//
    // 删除
    public void deleteTDimensionField(String[] tableNames);
//
//    public void updateFieldName(Object[] array);

}
