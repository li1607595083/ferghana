package com.skyon.project.system.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Strings;
import com.skyon.common.utils.StringUtils;
import com.skyon.project.system.domain.TDataSource;
import com.skyon.project.system.domain.TDatasourceField;
import com.skyon.project.system.mapper.TDataSourceMapper;
import com.skyon.project.system.mapper.TDatasourceFieldMapper;
import com.skyon.project.system.service.ITDataSourceService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.swing.text.TabableView;
import java.lang.reflect.Array;
import java.util.*;

/**
 * 【请填写功能名称】Service业务层处理
 *
 * @date 2020-05-21
 */
@Service
public class TDataSourceServiceImpl implements ITDataSourceService {

    @Autowired
    private TDataSourceMapper tDataSourceMapper;

    @Autowired
    private TDatasourceFieldMapper fieldMapper;

    /**
     * 查询【请填写功能名称】
     *
     * @param dataSourceId 【请填写功能名称】ID
     * @return 【请填写功能名称】
     */
    @Override
    public TDataSource selectTDataSourceById(Long dataSourceId) {

        TDataSource tDataSource = tDataSourceMapper.selectTDataSourceById(dataSourceId);
        JSONArray array = JSON.parseArray(tDataSource.getSchemaDefine());

        // 查询数据源表字段的使用情况 01:数据源表
        List<TDatasourceField> tDatasourceFields = fieldMapper.selectTDatasourceFieldByName(tDataSource.getTableName(), "01");
        if (tDatasourceFields != null) {
            for (int j = 0; j < array.size(); j++) {
                JSONObject o = (JSONObject) array.get(j);
                Object schemaDefine = o.get("schemaDefine");
                for (int i = 0; i < tDatasourceFields.size(); i++) {
                    TDatasourceField tDatasourceField = tDatasourceFields.get(i);
                    String fieldName = tDatasourceField.getFieldName();
                    if (schemaDefine.equals(fieldName)) {
                        o.put("isUsed", tDatasourceField.getIsUsed());
                        o.put("fieldId", tDatasourceField.getId());
                        break;
                    }
                }
            }
        }
        tDataSource.setDynamicItem(array.toArray());
        if (tDataSource.getHandleData() != null) {
            tDataSource.setHandleData(JSON.parseArray(tDataSource.getHandleData().toString()));
        }


        return tDataSource;
    }

    // 根据表名查询schema
    @Override
    public TDataSource selectTDataSourceByTableName(String tableName) {
        return tDataSourceMapper.selectTDataSourceByTableName(tableName);
    }

    /**
     * 查询【请填写功能名称】列表
     *
     * @param tDataSource 【请填写功能名称】
     * @return 【请填写功能名称】
     */
    @Override
    public List<TDataSource> selectTDataSourceList(TDataSource tDataSource) {
        return tDataSourceMapper.selectTDataSourceList(tDataSource);
    }

    /**
     * 新增【请填写功能名称】
     *
     * @param tDataSource 【请填写功能名称】
     * @return 结果
     */
    @Override
    public int insertTDataSource(TDataSource tDataSource) {
        tranSchemaJson(tDataSource);
        tDataSource.setCreateTableSql(joinCreateTableSql(tDataSource));
//        if (tDataSource.getConnectorType().equals("02")){
        tDataSource.setHandleData(JSON.toJSONString(tDataSource.getHandleData()));
//        }
        return tDataSourceMapper.insertTDataSource(tDataSource);
    }

    // 拼接建表sql
    private String joinCreateTableSql(TDataSource dataSource) {
        String sqlString = "";
        String sourceTableSchema = schemaTransform(dataSource.getConnectorType(), dataSource.getSchemaDefine());
        //01代表kafka连接器
        if ("01".equals(dataSource.getConnectorType())) { // kafka
            String consumerMode;
            String topicName;
            if ("01".equals(dataSource.getConsumerMode())) {
                consumerMode = "latest-offset";
            } else {
                consumerMode = "earliest-offset";
            }
            topicName = dataSource.getTopicName();
            String waterMark = dataSource.getWaterMarkName() != null ?
                    "`" + dataSource.getWaterMarkName() + "` as " + dataSource.getWaterMarkName() + " - INTERVAL '" + dataSource.getWaterMarkTime() + "' SECOND"
                    : "proctime as proctime - INTERVAL '" + dataSource.getWaterMarkTime() + "' SECOND";
            String waterSchema = dataSource.getWaterMarkName() != null ? ",`" + dataSource.getWaterMarkName() + "` TIMESTAMP" : " ";
            sqlString = "CREATE TABLE `" + dataSource.getTableName() + "`(" + sourceTableSchema + waterSchema +
                    ",proctime AS PROCTIME(),WATERMARK FOR " + waterMark +
                    ") WITH ('connector' = 'kafka-0.11' ,'topic' = '"
                    + topicName + "','properties.bootstrap.servers' = '" + dataSource.getKafkaAddress()
                    + "','properties.group.id' = '" + dataSource.getConsumerGroup()
                    + "','scan.startup.mode' = '" + consumerMode + "','format' = 'json')";
        } else if ("02".equals(dataSource.getConnectorType())) { // mysql-cdc
            String scan = dataSource.getScanAll().equals("1") ? "initial" : "schema_only";
            ArrayList handleData = (ArrayList) dataSource.getHandleData();
            String join1 = StringUtils.join(handleData, ",");
            sqlString = "CREATE TABLE orders (" + sourceTableSchema + ") WITH ('connector' = 'mysql-cdc',"
                    + "  'hostname' = '" + dataSource.getMyAddress() + "',"
                    + "  'port' = '" + dataSource.getPort() + "',"
                    + "  'username' = '" + dataSource.getUserName() + "',"
                    + "  'password' = '" + dataSource.getPassword() + "',"
                    + "  'database-name' = '" + dataSource.getMyDatabase() + "',"
                    + "  'table-name' = '" + dataSource.getMyTableName() + "',"
                    + " 'debezium.snapshot.mode' = '" + scan + "'" // 不扫描全表
                    + "  'server-time-zone'= 'Asia/Shanghai'"
                    + ")"
                    + "|" + join1;


        } else if ("03".equals(dataSource.getConnectorType())) { // oracle-cdc
            String readPosition = dataSource.getScanAll().equals("1") ? "all" : "current";
            Map map = new HashMap();
            map.put("jdbcUrl", dataSource.getMyAddress());
            map.put("username", dataSource.getUserName());
            map.put("password", dataSource.getPassword());
            String[] tableArray = dataSource.getMyTableName().split(";");
            for (int i = 0; i < tableArray.length; i++) {
                tableArray[i] = dataSource.getUserName() + "." + tableArray[i];
            }
            map.put("table", tableArray);
            ArrayList handleData = (ArrayList) dataSource.getHandleData();
            String join1 = StringUtils.join(handleData, ",");
            map.put("cat", join1);
            map.put("readPosition", readPosition);

            Map parameter = new HashMap();
            parameter.put("parameter", map);
            parameter.put("name", "oraclelogminerreader");

            Map reader = new HashMap();
            reader.put("reader", parameter);

            List li = new ArrayList();
            Map content = new HashMap();
            li.add(reader);
            content.put("content", li);

            Map job = new HashMap();
            job.put("job", content);

            sqlString = JSON.toJSONString(job);
        }
        return sqlString;
    }

    // 拼接字段
    private String schemaTransform(String type, String schemaDefine) {
        StringBuilder sb = new StringBuilder();
        JSONArray array = JSON.parseArray(schemaDefine);
        if ("01".equals(type)) {
            for (int i = 0; i < array.size(); i++) {
                JSONObject o = (JSONObject) array.get(i);
                Object schemaDefine1 = o.get("schemaDefine");
                Object dataBaseType = o.get("dataBaseType");
                sb.append("`" + schemaDefine1 + "` " + dataBaseType + ",");
            }
        } else if ("02".equals(type) || "03".equals(type)) { // mysql-cdc 或者 oracle-cdc
            for (int i = 0; i < array.size(); i++) {
                JSONObject o = (JSONObject) array.get(i);
                Object schemaDefine1 = o.get("schemaDefine");
                Object dataBaseType = o.get("dataBaseType");
                sb.append(schemaDefine1 + " " + dataBaseType + ",");
            }
        }
        return sb.substring(0, sb.length() - 1);


    }

    /**
     * 修改【请填写功能名称】
     *
     * @param tDataSource 【请填写功能名称】
     * @return 结果
     */
    @Override
    public int updateTDataSource(TDataSource tDataSource) {
        tranSchemaJson(tDataSource);
        tDataSource.setCreateTableSql(joinCreateTableSql(tDataSource));
//        if (tDataSource.getConnectorType().equals("02")){
        tDataSource.setHandleData(JSON.toJSONString(tDataSource.getHandleData()));
//        }
        tDataSource.setModifyTime(new Date());
        return tDataSourceMapper.updateTDataSource(tDataSource);
    }

    /**
     * 批量删除【请填写功能名称】
     *
     * @param dataSourceIds 需要删除的【请填写功能名称】ID
     * @return 结果
     */
    @Override
    public int deleteTDataSourceByIds(Long[] dataSourceIds) {
        return tDataSourceMapper.deleteTDataSourceByIds(dataSourceIds);
    }

    /**
     * 删除【请填写功能名称】信息
     *
     * @param dataSourceId 【请填写功能名称】ID
     * @return 结果
     */
    @Override
    public int deleteTDataSourceById(Long dataSourceId) {
        return tDataSourceMapper.deleteTDataSourceById(dataSourceId);
    }

    // 将数组转换为字段schema_defina的json格式
    private void tranSchemaJson(TDataSource tDataSource) {

        Object[] dynamicItem = tDataSource.getDynamicItem();
//        List list = new ArrayList();
//        // 把schema转换成json串
//        if (dynamicItem != null && dynamicItem.length > 0) {
//            for (int i = 0; i < dynamicItem.length; i++) {
//                HashMap hashMap = new HashMap();
//                HashMap o = (HashMap) dynamicItem[i];
//                hashMap.put(o.get("schemaDefine"), o.get("dataBaseType"));
//                list.add(hashMap);
//            }
//        }
        String s = JSON.toJSONString(dynamicItem);
        tDataSource.setSchemaDefine(s);
    }

    // 根据id查集合
    @Override
    public List<TDataSource> selectTDataSourceListByIds(Long[] ids) {
        return tDataSourceMapper.selectTDataSourceListByIds(ids);
    }
}
