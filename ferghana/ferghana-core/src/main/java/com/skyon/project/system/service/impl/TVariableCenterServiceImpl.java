package com.skyon.project.system.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.skyon.common.exception.CustomException;
import com.skyon.common.utils.SecurityUtils;
import com.skyon.common.utils.StringUtils;
import com.skyon.framework.aspectj.lang.annotation.DataScope;
import com.skyon.project.system.domain.*;
import com.skyon.project.system.mapper.TDimensionTableMapper;
import com.skyon.project.system.mapper.TSelfFunctionMapper;
import com.skyon.project.system.mapper.TVariableCenterMapper;
import com.skyon.project.system.mapper.TVariableClassificationMapper;
import com.skyon.project.system.service.ITVariableCenterService;
import com.skyon.project.system.util.PropertiesUtil;
import joptsimple.internal.Strings;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import static org.apache.flink.optimizer.Optimizer.LOG;

/**
 * 变量管理中心Service业务层处理
 *
 * @date 2020-08-06
 */
@Service
public class TVariableCenterServiceImpl implements ITVariableCenterService {

    private static final String VARSUBMITPATH = "var_submit_path"; // 启动脚本路径
    private static final String VARSUBMITNAME = "var_submit_name"; // 启动脚本文件名
    private static final String VARKILLPATH = "var_kill_path"; // 停用脚本路径
    private static final String VARKILLNAME = "var_kill_name"; // 停用脚本文件名
    private static final String TESTZOOKEEPERADDREE = "test_zookeeper_addree"; // 变量测试zookeeper地址
    private static final String TESTKAFKAADDRESS = "test_kafka_address"; // 变量测试kafka地址


    @Autowired
    private TVariableCenterMapper tVariableCenterMapper;
    @Autowired
    private TDimensionTableMapper tDimensionTableMapper;
    @Autowired
    private TVariableClassificationMapper tVariableClassificationMapper;

    @Autowired
    private TSelfFunctionMapper tSelfFunctionMapper;
    private TVariableClassification variableClassification;

    /**
     * 查询变量管理中心
     *
     * @param variableId 变量管理中心ID
     * @return 变量管理中心
     */
    @Override
    public TVariableCenter selectTVariableCenterById(Long variableId) {
        return tVariableCenterMapper.selectTVariableCenterById(variableId);
    }

    /**
     * 查询变量管理中心列表
     *
     * @param tVariableCenter 变量管理中心
     * @return 变量管理中心
     */
    @Override
    @DataScope(serviceTable = true)
    public List<TVariableCenter> selectTVariableCenterList(TVariableCenter tVariableCenter) {
        return tVariableCenterMapper.selectTVariableCenterList(tVariableCenter);
    }

    @Override
    public List<TVariableCenter> selectVariableVersionList(TVariableCenter tVariableCenter) {
        return tVariableCenterMapper.selectVariableVersionList(tVariableCenter);
    }

    /**
     * 新增变量管理中心
     *
     * @param tVariableCenter 变量管理中心
     * @return 结果
     */
    @Override
    public int insertTVariableCenter(TVariableCenter tVariableCenter) {
        tVariableCenter.setCreateBy(SecurityUtils.getUsername());
        tVariableCenter.setCreateId(SecurityUtils.getUserId());
        insertORupdate(tVariableCenter);
        return tVariableCenterMapper.insertTVariableCenter(tVariableCenter);
    }

    private void insertORupdate(TVariableCenter tVariableCenter) {
        JSONArray dimensionName = null;
        if (tVariableCenter.getVariableClassification() != null) {
            TVariableClassification variableClassification = tVariableClassificationMapper
                    .selectTVariableClassificationById(new Long(tVariableCenter.getVariableClassification()));
            dimensionName = JSON.parseArray(variableClassification.getDimensionRelation().toString());
        }
        // redisSelfFunctionItem 赋值
        tVariableCenter.setSqlContext(jointSqlContext(dimensionName, tVariableCenter));
        tVariableCenter.setRedisSelfFunctionItem(JSON.toJSONString(tVariableCenter.getRedisSelfFunctionItem()));
        tVariableCenter.setStatisticsGroupItem(JSON.toJSONString(tVariableCenter.getStatisticsGroupItem()));
        tVariableCenter.setStatisticsSelfFunctionItem(JSON.toJSONString(tVariableCenter.getStatisticsSelfFunctionItem()));
    }

    // 拼接条件sql
    @Override
    public String jointSqlContext(JSONArray dimensionName, TVariableCenter variable) {
        StringBuilder sb = new StringBuilder();

        // 查询表  如果为join拼接表 新表的格式为: xxxx_join_xxxx   该新表会在启动的时候创建！！！！
        String resultTableName = "";

        if (dimensionName != null && dimensionName.size() > 0) {
            String name = "";
            for (int i = 0; i < dimensionName.size(); i++) {
                JSONObject o = (JSONObject) dimensionName.get(i);
                String dimensionName1 = o.get("dimensionName").toString();
                if (!Strings.isNullOrEmpty(dimensionName1)) {
                    name = name + o.get("dimensionName") + "_join_";
                    resultTableName = (variable.getSourceTableName() + "_join_" + name.substring(0, name.length() - 6)) + "";
                } else {
                    resultTableName = (variable.getSourceTableName()) + "";
                }
            }
        } else {
            resultTableName = (variable.getSourceTableName()) + "";
        }

        if ("01".equals(variable.getVariableType())) { // 基础变量

            if ("01".equals(variable.getVariableModelType())) {   // 模板类型为普通查询时
                sb.append("SELECT ");
                // 查询主键
                sb.append("*").append(", ");
                //select redis_query(methodName, key, field / null) from 数据源表 join 数据维表  ；  or 新join 表
                sb.append("redis_query(");
                String key = variable.getRedisKey();
                // 如果作用函数有值，需加上 格式： function(字段)
                Map redisSelfFunctionItem = (Map) variable.getRedisSelfFunctionItem();
                if (redisSelfFunctionItem.size() > 0) {
                    Object effectFunction = redisSelfFunctionItem.get("effectFunction");
                    Object beginIndex = redisSelfFunctionItem.get("beginIndex");
                    Object lengthNum = redisSelfFunctionItem.get("lengthNum");
                    if (effectFunction != null && !effectFunction.equals("")) {
                        if ("SUBSTRING".equals(effectFunction)) { // SUBSTRING(VARCHAR a, INT start, INT len)
                            key = effectFunction + "(" + variable.getRedisKey() + "," + beginIndex + "," + lengthNum + ")";
                        } else {
                            key = effectFunction + "(" + variable.getRedisKey() + ")";
                        }
                    }
                }
                if ("01".equals(variable.getRedisFunction())) {
                    sb.append("'get',").append(key).append(",null) ");
                } else if ("02".equals(variable.getRedisFunction())) {
                    sb.append("'hget',").append("'").append(variable.getRedisValue()).append("'").append(",").append(key).append(") ");
                } else if ("03".equals(variable.getRedisFunction())) {
                    sb.append("'sismember',").append("'").append(variable.getRedisValue()).append("'").append(",").append(key).append(") ");
                } else if ("04".equals(variable.getRedisFunction())) {
                    sb.append("'scard',").append("'").append(variable.getRedisValue()).append("'").append(",null) ");
                } else if ("05".equals(variable.getRedisFunction())) {
                    sb.append("'hcard',").append("'").append(variable.getRedisValue()).append("'")
                            .append(",").append(key).append(",'").append(variable.getRedisElementDistinct()).append("') ");
                }

                sb.append(" AS " + variable.getVariableNameEn() + " FROM " + resultTableName);
                // where 条件
                if (!Strings.isNullOrEmpty(variable.getStatisticsConditions())) {
                    sb.append(" where " + variable.getStatisticsConditions()); // 条件
                }

            } else if ("02".equals(variable.getVariableModelType())) {   // 模板类型为统计查询时
                sb.append("SELECT ");
                // 查询主键
                sb.append("*").append(", ");
                // 统计计算模板
                sb.append(joinStatisticsCountModel(variable.getStatisticsCountModel(), variable.getVariableFactor(), variable.getStatisticsConditions(), resultTableName, variable.getStatisticsSelfFunctionItem()));
                // 统计周期的判断
                if ("03".equals(variable.getStatisticsModel()) || "04".equals(variable.getStatisticsModel())
                        || "05".equals(variable.getStatisticsModel()) || "06".equals(variable.getStatisticsModel()) || "07".equals(variable.getStatisticsModel())) { // over 窗口  单日窗口 单周窗口  单月窗口
                    sb.append(" over(");

                    LOG.info("-------------Watermark:" + variable.getWatermark());
                    // 是否分组判断
                    ArrayList statisticsGroupItem = (ArrayList) variable.getStatisticsGroupItem();
                    String group = "";
                    if (statisticsGroupItem != null && statisticsGroupItem.size() > 0) {
                        for (int i = 0; i < statisticsGroupItem.size(); i++) {
                            Map o = (Map) statisticsGroupItem.get(i);
                            String groupField = o.get("groupField").toString();
                            String groupFunction = o.get("groupFunction").toString();
                            if (!Strings.isNullOrEmpty(groupField)) {
                                String[] split = groupField.split("\\.");
                                if (!Strings.isNullOrEmpty(groupFunction)) {
                                    if ("SUBSTRING".equals(groupFunction)) {
                                        split[1] = groupFunction + "(" + split[1] + "," + o.get("beginIndex") + "," + o.get("lengthNum") + ")";
                                    } else {
                                        split[1] = groupFunction + "(" + split[1] + ")";
                                    }
                                }
                                group = group + split[1] + ",";
                                sb.append(" PARTITION BY " + group.substring(0, group.length() - 1));
                            }
                        }
                    }
                    // 水印字段的判断
                    if (!Strings.isNullOrEmpty(variable.getWatermark())) {
                        sb.append(" ORDER BY " + variable.getWatermark());
                    } else {
                        sb.append(" ORDER BY proctime ");
                    }
                    // 统计周期的数值
                    if ("03".equals(variable.getStatisticsModel())) {
                        sb.append(" RANGE BETWEEN INTERVAL '"
                                + tranStatisticsCycle(variable.getStatisticsNum(), variable.getStatisticsCycle())
                                + " preceding AND CURRENT ROW) ");
                    } else if ("04".equals(variable.getStatisticsModel())) { //  单日窗口 77
                        sb.append(" RANGE BETWEEN INTERVAL '777' MINUTE preceding AND CURRENT ROW) ");
                    } else if ("05".equals(variable.getStatisticsModel())) { //  单周窗口
                        sb.append(" RANGE BETWEEN INTERVAL '888' MINUTE preceding AND CURRENT ROW) ");
                    } else if ("06".equals(variable.getStatisticsModel())) { //  单月窗口
                        sb.append(" RANGE BETWEEN INTERVAL '999' MINUTE preceding AND CURRENT ROW) ");
                    } else if ("07".equals(variable.getStatisticsModel())) { //  全局窗口
                        // 如果有开始时间结束时间 就拼接上 用@_@拼接
                        if (!Strings.isNullOrEmpty(variable.getGlobalVariableStartTime())
                                && !Strings.isNullOrEmpty(variable.getGlobalVariableStopTime())) {
                            sb.append(" RANGE BETWEEN UNBOUNDED ");
                            sb.append("@_@").append(variable.getGlobalVariableStartTime()).append("@_@").append(variable.getGlobalVariableStopTime()).append("@_@");
                            sb.append("  preceding AND CURRENT ROW) ");
                        } else {
                            sb.append(" RANGE BETWEEN UNBOUNDED preceding AND CURRENT ROW) ");
                        }
                    }

                }

                // 别名
                sb.append(" AS " + variable.getVariableNameEn());

                // where 条件
                if (!Strings.isNullOrEmpty(variable.getStatisticsConditions())) {
                    String variableFactor = variable.getVariableFactor();
                    // 如果变量因子有作用函数
//                    Map redisSelfFunctionItem = (Map) variable.getStatisticsSelfFunctionItem();
//                    if (redisSelfFunctionItem.size() > 0) {
//                        Object effectFunction = redisSelfFunctionItem.get("effectFunction");
//                        Object beginIndex = redisSelfFunctionItem.get("beginIndex");
//                        Object lengthNum = redisSelfFunctionItem.get("lengthNum");
//                        if (effectFunction != null && !effectFunction.equals("")) {
//                            if ("SUBSTRING".equals(effectFunction)) { // SUBSTRING(VARCHAR a, INT start, INT len)
//                                variableFactor = effectFunction + "(" + variableFactor + "," + beginIndex + "," + lengthNum + ")";
//                            } else {
//                                variableFactor = effectFunction + "(" + variableFactor + ")";
//                            }
//                        }
//                    }

                    sb.append(" FROM(SELECT ").append("*").append(",").append("IF(");// 主键


                    sb.append(variable.getStatisticsConditions()).append(","); // 条件
                    sb.append(variableFactor).append(",CAST(ifFalseSetNull() AS ")
                            .append(variable.getVariableFactorType()).append(")) AS ")
                            .append(variable.getVariableFactor() + "_RE");  // 变量因子及类型 _RE为当求值字段为主键时，加的区分辨识,改成只要有where添加，都加
//                    if (variable.getStatisticsGroupItem() != null) {
//                        ArrayList statisticsGroup = (ArrayList) variable.getStatisticsGroupItem();
//                        if (statisticsGroup != null && statisticsGroup.size() > 0) {
//                            String group = "";
//                            for (int i = 0; i < statisticsGroup.size(); i++) {
//                                Map map = (Map) statisticsGroup.get(i);
//                                String groupField = map.get("groupField").toString();
//                                String groupFunction = map.get("groupFunction").toString();
//                                if (!Strings.isNullOrEmpty(groupField)) {
//                                    group = group + groupField + ",";
//                                    sb.append(",").append(group.substring(0, group.length() - 1)); // 分组的字段
//                                }
//                            }
//                        }
//                    }
                    // 水印字段的判断
//                    if (!Strings.isNullOrEmpty(variable.getWatermark())) {
//                        sb.append(",").append(variable.getWatermark());
//                    } else {
//                        sb.append(",proctime");
//                    }
                    sb.append(" FROM " + resultTableName);
                    sb.append(") AS tmp");
                } else {
                    sb.append(" FROM " + resultTableName);

                }

            } else if ("03".equals(variable.getVariableModelType())) {  // 数据加工
                sb.append("SELECT ");
                //  主键
                sb.append("*").append(",");
                // select 函数英文名（） from 数据源表;
                sb.append(variable.getSelfFunctionNameCn()).append("(");
                // 组装参数
                String inputParams = variable.getProcessInputParams();
                JSONArray jsonArray = JSON.parseArray(inputParams);
                String s = "";
                for (int i = 0; i < jsonArray.size(); i++) {
                    JSONObject o = (JSONObject) jsonArray.get(i);
                    String outParam = o.get("outParam").toString();
                    // 如果有作用函数
                    if (o.get("functionType") != null) {
                        if ("SUBSTRING".equals(o.get("functionType").toString())) {
                            String beginIndex = o.get("beginIndex").toString();
                            String lengthNum = o.get("lengthNum").toString();
                            outParam = o.get("functionType").toString() + "(" + outParam + "," + beginIndex + "," + lengthNum + ")";
                        } else {
                            outParam = o.get("functionType").toString() + "(" + outParam + ")";
                        }
                    }
                    s = s + outParam + ",";
                }
                sb.append(s, 0, s.length() - 1).append(") AS ").append(variable.getVariableNameEn());
                sb.append(" FROM " + resultTableName);
            } else if ("04".equals(variable.getVariableModelType())) {
                sb.append(variable.getUserDefinedSql());
            }
        } else if ("02".equals(variable.getVariableType())) { // 派生变量 sqlContext

            if ("01".equals(variable.getDeriveVariableModelType())) { // 四则运算  // 单个派生变量  和  对应的基础变量
                String deriveBaseVariable = variable.getDeriveBaseVariable().toString();
                List<String> strings = JSON.parseArray(deriveBaseVariable, String.class);
                String[] strings1 = strings.toArray(new String[strings.size()]);
                List<TVariableCenter> tVariableCenters = tVariableCenterMapper.selectTVariableCenterByNames(strings1);
                String s = "";
                if (tVariableCenters != null && tVariableCenters.size() > 0) {
                    for (int i = 0; i < tVariableCenters.size(); i++) {
                        TVariableCenter tVariableCenter = tVariableCenters.get(i);
                        String sqlContext = tVariableCenter.getSqlContext();
                        s = s + sqlContext + ";";
                    }
                }
                sb.append(s, 0, s.length() - 1).append("@");
                // select 编辑区 as 英文名 from
                sb.append("SELECT ").append("*").append(",").append(variable.getDeriveVariableSql().replaceAll("@", ""))
                        .append(" as ").append(variable.getVariableNameEn()).append(" FROM ");
                // 合成的表名 uuid 随机
                String uuid = "A" + UUID.randomUUID().toString().replaceAll("-", "");
                sb.append(uuid).append("@").append(uuid);
                // 字段个数： 主键+基础变量的个数
                sb.append("@").append(strings.size() + 1);
            } else if ("02".equals(variable.getDeriveVariableModelType())) { // 计算引擎   // 多个派生变量  和  对应的基础变量   及单个基础变量
                // 这里只存函数sql，方便以后变量包调用
                TSelfFunction tSelfFunction = tSelfFunctionMapper.selectTSelfFunctionById(new Long(variable.getDeriveProcessModel()));
                sb.append(tSelfFunction.getFunctionName()).append("(");

                JSONArray deriveInputParams = (JSONArray) JSONObject.parse(variable.getDeriveInputParams());
                JSONArray parse = (JSONArray) JSONObject.parse(tSelfFunction.getInputParam());
                StringBuilder tmp = new StringBuilder();
                for (int i = 0; i < parse.size(); i++) {
                    Map o = (Map) parse.get(i);
                    Object schemaDefine = o.get("schemaDefine");
                    Object dataBaseType = o.get("dataBaseType");
                    for (int j = 0; j < deriveInputParams.size(); j++) {
                        Map o2 = (Map) deriveInputParams.get(j);
                        Object selfFuncParam = o2.get("selfFuncParam");
                        Object type = o2.get("type");
                        Object outParam = o2.get("outParam");
                        if (schemaDefine.equals(selfFuncParam)) {
                            if (dataBaseType.equals(type) && !"map".equals(type)) {
                                tmp.append("'").append(outParam).append("',");
                            } else {
                                JSONArray out = (JSONArray)outParam;
                                for (int k = 0; k < out.size(); k++) {
                                    tmp.append(out.get(k)).append(",");
                                }
                            }

                        }
                    }
                }
                sb.append(tmp.substring(0,tmp.length()-1)).append(")");
            } else if ("03".equals(variable.getDeriveVariableModelType())) {
                String deriveBaseVariable = variable.getDeriveBaseVariable().toString();
                List<String> strings = JSON.parseArray(deriveBaseVariable, String.class);
                String[] strings1 = strings.toArray(new String[strings.size()]);
                List<TVariableCenter> tVariableCenters = tVariableCenterMapper.selectTVariableCenterByNames(strings1);
                String s = "";
                if (tVariableCenters != null && tVariableCenters.size() > 0) {
                    for (int i = 0; i < tVariableCenters.size(); i++) {
                        TVariableCenter tVariableCenter = tVariableCenters.get(i);
                        String sqlContext = tVariableCenter.getSqlContext();
                        s = s + sqlContext + ";";
                    }
                }
                sb.append(s, 0, s.length() - 1).append("@");
                // select 编辑区 as 英文名 from
                sb.append("SELECT ").append("*").append(",").append("if(").append(variable.getDeriveVariableSql().replaceAll("@", ""))
                        .append(",1,0)").append(" as ").append(variable.getVariableNameEn()).append(" FROM ");
                // 合成的表名 uuid 随机
                String uuid = "A" + UUID.randomUUID().toString().replaceAll("-", "");
                sb.append(uuid).append("@").append(uuid);
                // 字段个数： 主键+基础变量的个数
                sb.append("@").append(strings.size() + 1);
            }
        }
        // 把所有的表名. 去掉
        if (dimensionName != null && dimensionName.size() > 0) {
            for (int i = 0; i < dimensionName.size(); i++) {
                JSONObject o = (JSONObject) dimensionName.get(i);
                String dimensionName1 = o.get("dimensionName").toString();
                if (!Strings.isNullOrEmpty(dimensionName1)) {
                    sb = new StringBuilder(sb.toString().replaceAll(o.get("dimensionName") + "\\.", ""));
                }
            }
        }
        return sb.toString().replaceAll(resultTableName + "\\.", "").replaceAll(variable.getSourceTableName() + "\\.", "");
    }
    // 统计计算模板的拼接

    /**
     * @param statisticsCountModel 统计计算模板
     * @param VariableFactor       变量因子
     * @return
     */
    private String joinStatisticsCountModel(String statisticsCountModel, String variableFactor, String conditions,
                                            String resultTableName, Object selfFunction) {
        // variableFactor的格式为表名.字段  如果有维表就是合成表.字段
        String[] split = variableFactor.split("\\.");
        // 重复情况
//        variableFactor = resultTableName + "." + split[1];
        //
        variableFactor = split[1];

//        _RE为当求值字段为主键时，加的区分辨识,改成只要有where添加，都加
        // where 条件
        if (!Strings.isNullOrEmpty(conditions)) {
            variableFactor = variableFactor + "_RE";
        }
        // 如果变量因子有作用函数
        Map redisSelfFunctionItem = (Map) selfFunction;
        if (redisSelfFunctionItem.size() > 0) {
            Object effectFunction = redisSelfFunctionItem.get("effectFunction");
            Object beginIndex = redisSelfFunctionItem.get("beginIndex");
            Object lengthNum = redisSelfFunctionItem.get("lengthNum");
            if (effectFunction != null && !effectFunction.equals("")) {
                if ("SUBSTRING".equals(effectFunction)) { // SUBSTRING(VARCHAR a, INT start, INT len)
                    variableFactor = effectFunction + "(" + variableFactor + "," + beginIndex + "," + lengthNum + ")";
                } else {
                    variableFactor = effectFunction + "(" + variableFactor + ")";
                }
            }
        }
        String s = "";
        if ("sum(distinct())".equals(statisticsCountModel)) {
            s = " sum(distinct(" + variableFactor + ")) ";
        }else if ("count(distinct())".equals(statisticsCountModel)) {
            s = " count(distinct(" + variableFactor + ")) ";
        } else {
            s = statisticsCountModel + "(" + variableFactor + ") ";
        }
        return s;
    }

    // SECOND、MINUTE、HOUR、DAY   转换  '4' DAY
    private String tranStatisticsCycle(String numStr, String val) {
        switch (val) {
            case "01":
                return numStr + "' SECOND";
            case "02":
                return numStr + "' MINUTE";
            case "03":
                return numStr + "' HOUR";
            case "04":
                return numStr + "' DAY";
        }
        return null;
    }

    /**
     * 修改变量管理中心
     *
     * @param tVariableCenter 变量管理中心
     * @return 结果
     */
    @Override
    public int updateTVariableCenter(TVariableCenter tVariableCenter) {
        insertORupdate(tVariableCenter);
        tVariableCenter.setUpdateBy(SecurityUtils.getUsername());
        return tVariableCenterMapper.updateTVariableCenter(tVariableCenter);
    }

    @Override
    public int updateVersionShow(TVariableCenter tVariableCenter) {
        return tVariableCenterMapper.updateVersionShow(tVariableCenter);
    }

    // 查询版本号最大的变量
    @Override
    public TVariableCenter selectTVariableCenterVersion(TVariableCenter tVariableCenter) {
        return tVariableCenterMapper.selectTVariableCenterVersion(tVariableCenter);
    }

    /**
     * 批量删除变量管理中心
     *
     * @param variableIds 需要删除的变量管理中心ID
     * @return 结果
     */
    @Override
    public int deleteTVariableCenterByIds(Long[] variableIds) {
        return tVariableCenterMapper.deleteTVariableCenterByIds(variableIds);
    }

    /**
     * 删除变量管理中心信息
     *
     * @param variableId 变量管理中心ID
     * @return 结果
     */
    @Override
    public int deleteTVariableCenterById(Long variableId) {
        return tVariableCenterMapper.deleteTVariableCenterById(variableId);
    }

    // 当有维表的时候拼接joinSQL 和 维表测试数据
    public void parseJoinSQl(Map mapParam, JSONArray dimensionRelation, Map map, ArrayList array) {
        if (dimensionRelation != null && dimensionRelation.size() > 0) {
            String s = "";
            String t = "";
            List listDimension = new ArrayList();
            for (int i = 0; i < dimensionRelation.size(); i++) {
                JSONObject o = (JSONObject) dimensionRelation.get(i);
                String dimensionName = o.get("dimensionName").toString();
                if (!Strings.isNullOrEmpty(dimensionName)) {
                    s = s + dimensionName + "_join_";
                    t = t + dimensionName + ".*,";
                    listDimension.add(dimensionName);
                }
            }
            if (listDimension.size() > 0) {
                List<TDimensionTable> dimensionTables = tDimensionTableMapper.getTDimensionTableListByNames(listDimension.toArray());
//          样式 "dimensionTable":[{"dimensionTableSql":"CREATE, TABLE ...","testDimType":"02","testDimdata":"JSON数组形式"}
                List list = new ArrayList();
                String m = "";
                String es = "";
                if (dimensionTables != null && dimensionTables.size() > 0) { // 有维表
                    for (int i = 0; i < dimensionTables.size(); i++) {
                        TDimensionTable table = dimensionTables.get(i);
                        if (array != null && array.size() > 0) {
                            for (int j = 0; j < array.size(); j++) {
                                Map o = (Map) array.get(j);
                                String name = o.get("name").toString();
                                Map mapTmp = new HashMap();
                                if ("02".equals(table.getConnectorType()) && table.getDimensionName().equals(name.substring(5))) { // jdbc
                                    mapTmp.put("dimensionTableSql", table.getJdbcCreateSql());
                                    mapTmp.put("testDimType", "02");
                                    ArrayList arrlist = (ArrayList) o.get("dimensionTableValue");
                                    if (arrlist != null && arrlist.size() > 0) {
                                        mapTmp.put("testDimdata", parseSplit(arrlist));
                                    }

                                } else if ("03".equals(table.getConnectorType()) && table.getDimensionName().equals(name.substring(5))) { // hbase
                                    mapTmp.put("dimensionTableSql", table.getHbaseCreateSql());
                                    mapTmp.put("testDimType", "03");
                                    // 为了去掉前面的表名
                                    ArrayList arrlist = (ArrayList) o.get("dimensionTableValue");
                                    if (arrlist != null && arrlist.size() > 0) {
                                        List dd = new ArrayList();
                                        for (int k = 0; k < arrlist.size(); k++) {
                                            Map tmo = (Map) arrlist.get(k);
                                            Iterator<Map.Entry<String, Object>> entries = tmo.entrySet().iterator();
                                            Map mapTmo = new HashMap();
                                            while (entries.hasNext()) {
                                                Map.Entry<String, Object> entry = entries.next();
                                                if (entry.getKey().startsWith(table.getDimensionName())) {
                                                    String key = entry.getKey();
                                                    mapTmo.put(key.substring(table.getDimensionName().length() + 1), entry.getValue());
                                                } else {
                                                    mapTmo.put(entry.getKey(), entry.getValue());
                                                }
                                            }
                                            dd.add(mapTmo);
                                        }
                                        mapTmp.put("testDimdata", dd);
                                    }
                                }
                                else if ("04".equals(table.getConnectorType()) && table.getDimensionName().equals(name.substring(5))) { // es
                                    mapTmp.put("dimensionTableSql", table.getEsCreateSql());
                                    mapTmp.put("testDimType", "04");
                                    ArrayList arrlist = (ArrayList) o.get("dimensionTableValue");
                                    if (arrlist != null && arrlist.size() > 0) {
                                        mapTmp.put("testDimdata", parseSplit(arrlist));
                                    }
                                }
                                else if ("05".equals(table.getConnectorType()) && table.getDimensionName().equals(name.substring(5))) { // es
                                    mapTmp.put("dimensionTableSql", table.getEsCreateSql());
                                    mapTmp.put("testDimType", "05");
                                    ArrayList arrlist = (ArrayList) o.get("dimensionTableValue");
                                    if (arrlist != null && arrlist.size() > 0) {
                                        mapTmp.put("testDimdata", parseSplit(arrlist));
                                    }
                                }
                                if (mapTmp.size() > 0) list.add(mapTmp);
                            }
                        } else {
                            Map mapTmp = new HashMap();
                            if ("02".equals(table.getConnectorType())) { // jdbc
                                mapTmp.put("dimensionTableSql", table.getJdbcCreateSql());
                                mapTmp.put("testDimType", "02");
                            } else if ("03".equals(table.getConnectorType())) { // hbase
                                mapTmp.put("dimensionTableSql", table.getHbaseCreateSql());
                                mapTmp.put("testDimType", "03");
                            } else if ("04".equals(table.getConnectorType())) { // hbase
                                mapTmp.put("dimensionTableSql", table.getEsCreateSql());
                                mapTmp.put("testDimType", "04");
                            } else if ("05".equals(table.getConnectorType())) { // hbase
                                mapTmp.put("dimensionTableSql", table.getEsCreateSql());
                                mapTmp.put("testDimType", "05");
                            }
                            if (mapTmp.size() > 0) list.add(mapTmp);
                        }

                        for (int j = 0; j < dimensionRelation.size(); j++) {
                            JSONObject o = (JSONObject) dimensionRelation.get(j);
                            String dimensionName = o.get("dimensionName").toString();
                            if (!Strings.isNullOrEmpty(dimensionName)) {
                                if (dimensionName.equals(table.getDimensionName())) {
                                    if ("02".equals(table.getConnectorType())) { // jdbc
                                        String sourceDabField = o.get("sourceDabField").toString();
                                        m = m + " left join " + dimensionName + " FOR SYSTEM_TIME AS OF s.proctime AS "
                                                + dimensionName + " ON s." + sourceDabField + " = " + dimensionName + "." + table.getJdbcPrimaryKey();
                                    } else if ("03".equals(table.getConnectorType())) { // hbase
                                        String sourceDabField = o.get("sourceDabField").toString();
                                        m = m + " left join " + dimensionName + " FOR SYSTEM_TIME AS OF s.proctime AS "
                                                + dimensionName + " ON s." + sourceDabField + " = " + dimensionName + "." + table.getRowkey();
                                    }
                                }
                            }
                        }
                        for (int j = 0; j < dimensionRelation.size(); j++) {
                            JSONObject o = (JSONObject) dimensionRelation.get(j);
                            String dimensionName = o.get("dimensionName").toString();
                            if (!Strings.isNullOrEmpty(dimensionName)) {
                                if (dimensionName.equals(table.getDimensionName())) {
                                    if ("04".equals(table.getConnectorType()) || "05".equals(table.getConnectorType())) { // es
                                        String relation = o.get("relation").toString();
                                        JSONArray r = JSONArray.parseArray(relation);
                                        if(!"".equals(es)){
                                            es = es + ";";
                                        }
                                        for(int k=0;k<r.size();k++){
                                            JSONObject a = (JSONObject) r.get(k);
                                            String dimensionDabField = a.get("dimensionDabField").toString();
                                            String sourceDabField = a.get("sourceDabField").toString();
                                            es = es  + sourceDabField + " = " + dimensionName + "." + dimensionDabField;
                                            if(k < r.size() - 1){
                                                es = es + " AND ";
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    mapParam.put("dimensionTable", list);
                    StringBuilder sb = new StringBuilder();
                    // 新表 create TABLE xxxxx as select s.*,t.* from blackList s left join jdbc2 t on s.id = t.id；
                    String dimensionNameJOIN = s.substring(0, s.length() - 6);
                    if("".equals(m)){
                        sb.append(es);
                    }
                    else if("".equals(es)){
                        sb.append("create table `" + map.get("tableName") + "_join_" + dimensionNameJOIN
                                + "` (select s.*," + t.substring(0, t.length() - 1) + " from " + map.get("tableName") + " s " + m + ")");
                    }
                    else if(!"".equals(m) && !"".equals(es)){
                        sb.append("create table `" + map.get("tableName") + "_join_" + dimensionNameJOIN
                                + "` (select s.*," + t.substring(0, t.length() - 1) + " from " + map.get("tableName") + " s " + m + ");" + es);
                    }
                    mapParam.put("joinSql", sb.toString());

                }
            }
        }
    }

    // 双数据源表时拼接 twoStreamJoinSqls 参数
    public Map parseTwoStreamJoinSQl(Map mapParam,Map map,JSONObject sourceRelation){
        Object sourceDabRelation = map.get("sourceDabRelation");
        Object sourceTwoDabRelation = map.get("sourceTwoDabRelation");
        String sql = "SELECT * FROM " + sourceDabRelation + " LEFT JOIN " + sourceTwoDabRelation + " ON " + sourceDabRelation + "." +
                sourceRelation.getString("sourceDabField") + " = " + sourceTwoDabRelation + "." + sourceRelation.getString("sourceTwoDabField")
                + " AND " + sourceDabRelation + "." + map.get("waterMarkName");
        int lowScope = Integer.parseInt(sourceRelation.getString("lowScope"));
        int highScope = Integer.parseInt(sourceRelation.getString("highScope"));
        sql = sql + " BETWEEN " + sourceTwoDabRelation + "." + map.get("waterMarkTwoName") + (lowScope >= 0 ? " + " : " - ") + "INTERVAL '" + Math.abs(lowScope) + "' SECOND";
        sql = sql + " AND " + sourceTwoDabRelation + "." + map.get("waterMarkTwoName") + (highScope >= 0 ? " + " : " - ") + "INTERVAL '" + Math.abs(highScope) + "' SECOND";
        mapParam.put("twoStreamJoinSqls",sql+"|"+map.get("tableName")+"_"+map.get("tableTwoName")+"|["+lowScope+","+highScope+"]");
        return mapParam;
    }

    @Override
    public String variableTest(TVariableCenter variable, Map map, String millis) {
        Map mapParam = new HashMap();
        mapParam.put("runMode", "01");
        mapParam.put("testTopicName", millis);
        //
        mapParam.put("waterMark", map.get("waterMarkName") + "|" + map.get("waterMarkTime"));
        // 有维表时
        JSONArray dimensionRelation = JSON.parseArray(map.get("dimensionRelation").toString());
        // 如果有两个数据源表，sourceTableSql用分号隔开，新增sourceTableSql参数
        if(map.containsKey("sourceTwoDabRelation") && StringUtils.isNotEmpty(map.get("sourceTwoDabRelation").toString()) && StringUtils.isNotNull(map.get("sourceTwoDabRelation").toString())){
            mapParam.put("sourceTableSql", map.get("createTableSql")+";"+map.get("createTableTwoSql"));
            JSONObject sourceRelation = JSON.parseObject(map.get("sourceRelation").toString());
            parseTwoStreamJoinSQl(mapParam,map, sourceRelation);
        }
        else{
            mapParam.put("sourceTableSql", map.get("createTableSql"));
        }
        ArrayList array = (ArrayList) variable.getTestDimdata();
        // 当有维表的时候拼接joinSQL 和 维表测试数据
        parseJoinSQl(mapParam, dimensionRelation, map, array);
        TVariableClassification variableClassification = tVariableClassificationMapper
                .selectTVariableClassificationById(new Long(variable.getVariableClassification()));
        JSONArray dimensionName = JSON.parseArray(variableClassification.getDimensionRelation().toString());

        if ("01".equals(variable.getVariableType())) { // 基础变量
            mapParam.put("variableSqls", jointSqlContext(dimensionName, variable));
            // fieldOutNum 参数输出个数+主键  若相同的
            mapParam.put("fieldOutNum", "2");
        } else if ("02".equals(variable.getVariableType())) { // 派生变量
            if ("01".equals(variable.getDeriveVariableModelType())) { // 四则运算  // 单个派生变量  和  对应的基础变量
                mapParam.put("deVariableSqls", jointSqlContext(dimensionName, variable));
                // fieldOutNum 参数输出个数+主键  若相同的
                mapParam.put("fieldOutNum", "2");
            } else if ("02".equals(variable.getDeriveVariableModelType())) { // 计算引擎
                // 计算引擎拼接
                joinDecisionSql(mapParam, variable, millis);
            }
        }
        mapParam.put("sourcePrimaryKey", map.get("schemaPrimaryKey").toString());
        ArrayList sourceTableValue = (ArrayList) variable.getSourceTableValue();
        if (sourceTableValue != null && sourceTableValue.size() > 0) {
            mapParam.put("testSourcedata", parseSplit(sourceTableValue));
        }


        return JSON.toJSONString(mapParam);
    }

    private void joinDecisionSql(Map mapParam, TVariableCenter variable, String millis) {
        String deriveBaseVariable = variable.getDeriveBaseVariable().toString();
        List<String> strings = JSON.parseArray(deriveBaseVariable, String.class);
        String[] strings1 = strings.toArray(new String[strings.size()]);
        List<TVariableCenter> tVariableCenters = tVariableCenterMapper.selectTVariableCenterByNames(strings1);
        StringBuilder sb = new StringBuilder();
        StringBuilder st = new StringBuilder();
        for (int i = 0; i < tVariableCenters.size(); i++) {
            TVariableCenter tVariableCenter = tVariableCenters.get(i);
            if (tVariableCenter.getVariableType().equals("01")) {
                sb.append(tVariableCenter.getSqlContext()).append(";");
            } else if (tVariableCenter.getVariableType().equals("02")
                    && "01".equals(tVariableCenter.getDeriveVariableModelType())) {
                st.append(tVariableCenter.getSqlContext()).append("|");
            }
        }
        if (sb.length() > 0) mapParam.put("variableSqls", sb.substring(0, sb.toString().length() - 1));
        if (st.length() > 0) mapParam.put("deVariableSqls", st.substring(0, st.toString().length() - 1));
        // decisionSql 拼接
//        decisionSql:SELECT bod_decision('SQFQZ',CUST_NO,TRADE_ID,TRADE_AMOUNT,TRADE_AMOUNT123) FROM 表名 表名：变量测试：TestTopicName
        StringBuilder sd = new StringBuilder();
        TSelfFunction tSelfFunction = tSelfFunctionMapper.selectTSelfFunctionById(new Long(variable.getDeriveProcessModel()));
        sd.append("SELECT ").append("*").append(",").append(tSelfFunction.getFunctionName()).append("(");

//        [{"selfFuncParam":"source1","outParam":"1112"},{"selfFuncParam":"source2","outParam":"222"},{"selfFuncParam":"sourceMap","outParam":["ad","e"]}]
        JSONArray deriveInputParams = (JSONArray) JSONObject.parse(variable.getDeriveInputParams());
        JSONArray parse = (JSONArray) JSONObject.parse(tSelfFunction.getInputParam());
        StringBuilder tmp = new StringBuilder();
        for (int i = 0; i < parse.size(); i++) {
            Map o = (Map) parse.get(i);
            Object schemaDefine = o.get("schemaDefine");
            Object dataBaseType = o.get("dataBaseType");
            for (int j = 0; j < deriveInputParams.size(); j++) {
                Map o2 = (Map) deriveInputParams.get(j);
                Object selfFuncParam = o2.get("selfFuncParam");
                Object type = o2.get("type");
                Object outParam = o2.get("outParam");
                if (schemaDefine.equals(selfFuncParam)) {
                    if (dataBaseType.equals(type) && !"map".equals(type)) {
                        tmp.append("'").append(outParam).append("',");
                    } else {
                        JSONArray out = (JSONArray)outParam;
                        for (int k = 0; k < out.size(); k++) {
                            tmp.append(out.get(k)).append(",");
                        }
                    }

                }
            }
        }
        sd.append(tmp.substring(0,tmp.length()-1)).append(")");
        sd.append(" AS ").append(variable.getVariableNameEn());
        sd.append(" FROM ").append(millis);
        mapParam.put("decisionSql", sd.toString());
        mapParam.put("fieldOutNum", tVariableCenters.size() + 1);
    }

    private List parseSplit(ArrayList listSource) {
        ArrayList list = new ArrayList();
        for (int i = 0; i < listSource.size(); i++) {
            Map o = (Map) listSource.get(i);
            Iterator<Map.Entry<String, Object>> entries = o.entrySet().iterator();
            Map mapTmp = new HashMap();
            while (entries.hasNext()) {
                Map.Entry<String, Object> entry = entries.next();
                String tmp = entry.getKey();
                if (tmp.indexOf("-主键") > 0) {
                    tmp = tmp.substring(0, tmp.length() - 3);
                }
                if (tmp.indexOf(".") > 0) {
                    String[] split = tmp.split("\\.");
                    mapTmp.put(split[1], entry.getValue());
                } else {
                    mapTmp.put(tmp, entry.getValue());
                }
            }
            list.add(mapTmp);
        }
        return list;
    }

    public static void main(String[] args) {
        String s = "EP_OPENACCT_FLOW_TABLE)EP_OPENACCT_FLOW_TABLE.";
        System.out.println(s.replaceAll("EP_OPENACCT_FLOW_TABLE" + "\\.", ""));
    }


    /**
     * 调试
     */
    public List testRun(String paramJsonString, String millis) {
        List messageList = new ArrayList();
        //第一个变量是sh命令，第二个变量是需要执行的脚本路径，从第三个变量开始是我们要传到脚本里的参数。
        try {
            Runtime runtime = Runtime.getRuntime();
            //  参数加密
            String param = base64(paramJsonString);
            // 启动脚本
            String[] path = new String[]{"sh", PropertiesUtil.getPro(VARSUBMITPATH) + PropertiesUtil.getPro(VARSUBMITNAME), param};

            LOG.info("---------path1:" + path[1]);
            LOG.info("---------path2:" + path[2]);
            Process pro = runtime.exec(path);
            BufferedReader br = new BufferedReader(new InputStreamReader(pro.getInputStream()));
            StringBuffer strbr = new StringBuffer();
            String line;
            String jobId = null;
            if (true) { // 变量测试
                while ((line = br.readLine()) != null) {
                    strbr.append(line).append("\n");
                    if (line.contains("Job has been submitted with JobID")) { // 成功

                        String[] words = line.split(" ");
                        jobId = words[words.length - 1];
                        LOG.info("作业启动成功！JobID为：" + jobId);
                        break;
                    }
                }
            }

            // 获取计算出来的kafka结果
            if (jobId != null) {
                messageList = kafkaMessageGet(PropertiesUtil.getPro(TESTKAFKAADDRESS), millis);
            }

            LOG.info("---------messageList----------");
            // 收到结果杀死程序  超过页面响应时间杀死程序  报错杀死
            if (jobId != null) {
                String[] path2 = new String[]{"sh", PropertiesUtil.getPro(VARKILLPATH) + PropertiesUtil.getPro(VARKILLNAME), jobId};
                runtime.exec(path2);
                LOG.info("---------kill:" + jobId);
            }

            if (jobId == null) {
                //读取标准错误流
                BufferedReader brError = new BufferedReader(new InputStreamReader(pro.getErrorStream(), "gb2312"));
                StringBuffer errline = new StringBuffer();
                while (brError.readLine() != null) {
                    errline.append(brError.readLine() + "\n");
                    if ("null".equals(errline)) {
                        LOG.info("------------err1:" + errline.toString());
                    }
                }
                LOG.info("----------------err2:" + errline.toString());
            }
        } catch (IOException ec) {
            ec.printStackTrace();
            throw new CustomException("该调试出现系统异常！");
        }
        return messageList;
    }

    @Override
    public List getCol(String names) {
        String[] split = names.split(",");
        return tVariableCenterMapper.selectTVariableCenterByNames(split);
    }

    @Override
    public List<TVariableCenter> selectVariableCenterByIds(Long[] ids) {
        return tVariableCenterMapper.selectVariableCenterByIds(ids);
    }


    // base64加密
    private static String base64(String message) {
        byte[] bytes = message.getBytes();

        //Base64 加密
        String encoded = Base64.getEncoder().encodeToString(bytes);
//        System.out.println("Base 64 加密后：" + encoded);
//
//        //Base64 解密
//        byte[] decoded = Base64.getDecoder().decode(encoded);
//
//        String decodeStr = new String(decoded);
//        System.out.println("Base 64 解密后：" + decodeStr);
        return encoded;

    }

    // 生产者的连接集群信息
    public void kafkaMessageSend(String kafkaAddress, String topicName, String[] param) {
        Properties props = new Properties();
        // 配置集群信息，至少一个 最好多个
        props.put("bootstrap.servers", kafkaAddress);
        // key的序列方式
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // value的序列方式
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
//        String[] split = param.split(",");
        for (String message : param) {
            producer.send(new ProducerRecord<>(topicName, null, message));
        }
        producer.close();
    }


    // 从kafka取信息
    public List kafkaMessageGet(String kafkaAddress, String topicName) {
        Properties props = new Properties();
        // 配置集群信息，至少一个 最好多个
        props.put("bootstrap.servers", kafkaAddress);
        // 消费组
        props.put("group.id", String.valueOf(System.currentTimeMillis()));
        // 消费模式 earliest 消费历史  latest消费最新
        // 同一个消费组维护一个offset ,重新启动会继续消费
        // 不同消费组对应不同的offset, 都要设置消费模式
        props.put("auto.offset.reset", "earliest");
        // key的反序列方式  与生产者对应相反
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // value的反序列方式  与生产者对应相反
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        // 订阅的topic 可以多个
        consumer.subscribe(Collections.singletonList(topicName));

        List list = new ArrayList();
        while (true) {
            // 拉数据
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            ConsumerRecords<String, String> records = consumer.poll(100);
            if (records.count() > 0) {
                for (ConsumerRecord<String, String> record : records) {
                    list.add(record.value());
                    LOG.info("------result----:" + record.value());
                }
                return list;
            }
        }
    }
}
