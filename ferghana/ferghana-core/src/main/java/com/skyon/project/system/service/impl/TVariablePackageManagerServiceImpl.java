package com.skyon.project.system.service.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.skyon.common.exception.CustomException;
import com.skyon.common.utils.SecurityUtils;
import com.skyon.common.utils.StringUtils;
import com.skyon.framework.aspectj.lang.annotation.DataScope;
import com.skyon.project.system.domain.*;
import com.skyon.project.system.mapper.*;
import com.skyon.project.system.service.ITDataResultSourceService;
import com.skyon.project.system.util.PropertiesUtil;
import joptsimple.internal.Strings;
import lombok.extern.log4j.Log4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.skyon.project.system.service.ITVariablePackageManagerService;

import static org.apache.flink.optimizer.Optimizer.LOG;

/**
 * 变量包管理Service业务层处理
 *
 * @date 2020-08-24
 */
@Service
@Log4j
public class TVariablePackageManagerServiceImpl implements ITVariablePackageManagerService {

    private static final String PACKAGETESTKILLPATH = "package_test_kill_path"; // 停用脚本路径
    private static final String PACKAGETESTKILLNAME = "package_test_kill_name"; // 停用脚本文件名
    private static final String JOBREADOFFSET = "job_read_offset"; // 停用脚本文件名
    private static final String PACKAGETESTPATH = "package_test_path"; // 启动脚本路径
    private static final String PACKAGETESTNAME = "package_test_name"; // 启动脚本文件名
    private static final String PACKAGESTARTVARDIR = "package_start_var_dir"; //
    private static final String PACKAGESTARTJOBID = "package_start_jobid"; //


    private static final String TESTKAFKAADDRESS = "test_kafka_address"; // 变量测试kafka地址
    @Autowired
    private TVariablePackageManagerMapper tVariablePackageManagerMapper;

    @Autowired
    private TDataSourceMapper tDataSourceMapper;

    @Autowired
    private TVariableClassificationMapper tVariableClassificationMapper;

    @Autowired
    private TVariableCenterMapper tVariableCenterMapper;

    @Autowired
    private TDimensionTableMapper tDimensionTableMapper;

    @Autowired
    private ITDataResultSourceService resultSourceService;
    @Autowired
    private TDataResultSourceMapper resultSourceMapper;

    @Autowired
    private ITVariablePackageManagerService tVariablePackageManagerService;
    @Autowired
    private TVariableCenterServiceImpl tVariableCenterService;

    /**
     * 查询变量包管理
     *
     * @param variablePackId 变量包管理ID
     * @return 变量包管理
     */
    @Override
    public TVariablePackageManager selectTVariablePackageManagerById(Long variablePackId) {
        return tVariablePackageManagerMapper.selectTVariablePackageManagerById(variablePackId);
    }

    /**
     * 查询变量包管理列表
     *
     * @param tVariablePackageManager 变量包管理
     * @return 变量包管理
     */
    @Override
    @DataScope(serviceTable = true)
    public List<TVariablePackageManager> selectTVariablePackageManagerList(TVariablePackageManager tVariablePackageManager) {
        return tVariablePackageManagerMapper.selectTVariablePackageManagerList(tVariablePackageManager);
    }

    @Override
    public List<TVariablePackageManager> selectTVariablePackageManagerVersionList(TVariablePackageManager tVariablePackageManager) {
        return tVariablePackageManagerMapper.selectTVariablePackageManagerVersionList(tVariablePackageManager);
    }

    /**
     * 新增变量包管理
     *
     * @param pkManager 变量包管理
     * @return 结果
     */
    @Override
    public int insertTVariablePackageManager(Map map, TVariablePackageManager pkManager, String runFlag) {
        pkManager.setRuningState("0"); // 初始化都是0 停止
        pkManager.setCreateBy(SecurityUtils.getUsername());
        pkManager.setCreateId(SecurityUtils.getUserId());

        if (pkManager.getVariablePackType().equals("03")) { // oraclecdc
            pkManager.setResultTableSql(editOraclecdcSql(map, pkManager));
            pkManager.setVariableId(null);
        } else if (pkManager.getVariablePackType().equals("02")) { // mysqlcdc
            pkManager.setResultTableSql(setMysqlResultSql(pkManager));
            pkManager.setVariableId(null);
        } else if (pkManager.getVariablePackType().equals("01")) { // 一般变量包
            Map map1 = setResultTableSql(pkManager, runFlag, "");
            pkManager.setResultTableSql(map1.get("resultSql").toString());
            pkManager.setVariableId(JSON.toJSONString(pkManager.getVariableId()));
            pkManager.setOriginalVariableSql(joinOriginalVariableSql(map, pkManager));
        }

        pkManager.setVarDir(System.currentTimeMillis() + "");
        ArrayList array = (ArrayList) pkManager.getOriginalVariable();
        if (array != null && array.size() > 0) {
            pkManager.setOriginalVariable(JSON.toJSONString(pkManager.getOriginalVariable()));
        } else {
            pkManager.setOriginalVariable("");
        }
        return tVariablePackageManagerMapper.insertTVariablePackageManager(pkManager);
    }

    private String editOraclecdcSql(Map map, TVariablePackageManager pkManager) {
        // 原始变量 只有字段名 没有类型
        ArrayList originalVariableArr = (ArrayList) pkManager.getOriginalVariable();
        // 添加类型
        JSONArray schemaDefine = JSON.parseArray((String) map.get("schemaDefine"));
        for (int i = 0; i < originalVariableArr.size(); i++) {
            Object o = originalVariableArr.get(i);
            for (int j = 0; j < schemaDefine.size(); j++) {
                Map o1 = (Map) schemaDefine.get(j);
                if (o.equals(pkManager.getSourceTableName()+"."+o1.get("schemaDefine"))){
                    originalVariableArr.set(i,o1.get("schemaDefine") + ":" + o1.get("dataBaseType"));
                    break;
                }

            }
        }
        String joinSchema = StringUtils.join(originalVariableArr, ",");
        //


        // 拼接writer
        TDataResultSource resultSource =resultSourceMapper.selectTDataResultSourceByName(pkManager.getResultTable());


        Map producerSettings = new HashMap();
        producerSettings.put("bootstrap.servers",resultSource.getKafkaAddress());

        Map parameter = new HashMap();
        parameter.put("timezone", "UTC");
        parameter.put("topic", resultSource.getTableName());
        parameter.put("producerSettings", producerSettings);

        Map writer = new HashMap();
        writer.put("parameter",parameter);
        writer.put("name","kafkawriter");


        Object createTableSql = map.get("createTableSql");
        JSONObject jsonObject = JSON.parseObject((String) createTableSql);
        JSONObject job = (JSONObject) jsonObject.get("job");
//        job.put("schema", joinSchema);
        JSONArray content = (JSONArray) job.get("content");
        for (int i = 0; i < content.size(); i++) {
            JSONObject o = (JSONObject) content.get(i);
            o.put("writer", writer);
            JSONObject reader = (JSONObject) o.get("reader");
            reader.put("schema", joinSchema);
        }

        return JSON.toJSONString(jsonObject);
    }

    public int updatePackageVersion(TVariablePackageManager tVariablePackageManager) {
        return tVariablePackageManagerMapper.updatePackageVersion(tVariablePackageManager);
    }

    // 编辑原始变量sql
    private String joinOriginalVariableSql(Map map, TVariablePackageManager pkManager) {
        StringBuilder sb = new StringBuilder();
        ArrayList array = (ArrayList) pkManager.getOriginalVariable();
        if (array != null && array.size() > 0) {
            sb.append("select ");
            String tableName = "";
            // 有维表时
            JSONArray dimensionRelation = JSON.parseArray(map.get("dimensionRelation").toString());
            if (dimensionRelation != null && dimensionRelation.size() > 0) {
                String s = "";
                for (int i = 0; i < dimensionRelation.size(); i++) {
                    JSONObject o = (JSONObject) dimensionRelation.get(i);
                    String dimensionName = o.get("dimensionName").toString();
                    if (!Strings.isNullOrEmpty(dimensionName)) {
                        s = s + dimensionName + "_join_";
                    }
                }
                String dimensionNameJOIN = "";
                if (s.length() > 0) {
                    dimensionNameJOIN = s.substring(0, s.length() - 6);
                }
                tableName = map.get("tableName").toString();
                if (dimensionNameJOIN.length() > 0) {
                    tableName = tableName + "_join_" + dimensionNameJOIN;
                }
            } else {
                tableName = map.get("tableName") + "";
            }

            // 拼接字段
            String field = "";
            boolean flag = true;
            for (int i = 0; i < array.size(); i++) {
                String o = array.get(i).toString();
                String[] split = o.split("\\.");
                if (split[1].equals(map.get("schemaPrimaryKey"))) {
                    flag = false;
                }
                field = field + split[1] + ",";
            }
            // 拼接主键
            if (flag == true) {
                field = map.get("schemaPrimaryKey") + "," + field;
            }
            sb.append(field, 0, field.length() - 1);
            sb.append(" FROM " + tableName);
        }
        return sb.toString();
    }


    private String setMysqlResultSql(TVariablePackageManager pkManager){
        // INSERT INTO topic_13(SELECT * FROM tmp_variablePackEn)
        StringBuffer sb = new StringBuffer();
        sb.append("INSERT INTO ").append(pkManager.getResultTable()).append("(SELECT ");

        // 若有原始变量，把原始变量加进去
        ArrayList array = (ArrayList) pkManager.getOriginalVariable();
        if (array != null && array.size() > 0) {
            // 拼接字段
            String[] fieldArr = new String[array.size()];
            for (int i = 0; i < array.size(); i++) {
                String o = array.get(i).toString();
                String[] split = o.split("\\.");
                fieldArr[i] = split[1];
            }
            sb.append(StringUtils.join(fieldArr,","));
        }

        sb.append(" FROM tmp_").append(pkManager.getVariablePackEn()).append(")");

        return sb.toString();
    }

    // 结果表sql赋值
    private Map setResultTableSql(TVariablePackageManager pkManager, String runFlag, String millis) {
        Map mapResult = new HashMap();
        // insert into 结果表表名 select 输出字段,输出字段,输出字段 from tmp_变量包英文名;
        TDataResultSource resultSource = resultSourceService.selectTDataResultSourceByTableName(pkManager.getResultTable());

        StringBuilder sb = new StringBuilder();
        sb.append(" insert into ").append(pkManager.getResultTable()).append(" (select ");

        // 输出字段
        ArrayList variableId = (ArrayList) pkManager.getVariableId();
        Object[] objects = variableId.toArray();
        String[] stringArray = new String[objects.length];
        for (int i = 0; i < objects.length; i++) {
            stringArray[i] = objects[i].toString();
        }
        List<TVariableCenter> list = tVariableCenterMapper.selectTVariableCenterByNames(stringArray);
        // 顺序跳转，让该顺序与页面的选择顺序一致
        List<TVariableCenter> l = new ArrayList();
        for (String s : stringArray) {
            for (TVariableCenter tVariableCenter : list) {
                if (s.equals(tVariableCenter.getVariableNameEn())) {
                    l.add(tVariableCenter);
                    break;
                }
            }
        }

        List<TVariableCenter> listTmp = new ArrayList<>();
        listTmp.addAll(l);

        // 目的是移除派生变量的基础变量
        ArrayList<TVariableCenter> listDmo = new ArrayList();
        for (int i = 0; i < l.size(); i++) {
            TVariableCenter tVariableCenter = l.get(i);
            if ("02".equals(tVariableCenter.getVariableType())) { // 派生变量
                String deriveBaseVariable = tVariableCenter.getDeriveBaseVariable().toString();
                JSONArray array = JSON.parseArray(deriveBaseVariable);
                for (int j = 0; j < array.size(); j++) { // 1,2
                    for (int k = 0; k < listTmp.size(); k++) { // 2 .3 .4.1
                        TVariableCenter tVariableCenter1 = listTmp.get(k);
                        if (tVariableCenter1.getVariableNameEn().equals(array.get(j))) {
                            listDmo.add(tVariableCenter1);
                        }
                    }
                }
            }
        }
        listTmp.removeAll(listDmo);

        mapResult.put("outVariableNum", listTmp.size());


        String ssss = "";
        String shbase = "";
        for (int i = 0; i < listTmp.size(); i++) {
            TVariableCenter center = listTmp.get(i);
            if (!"02".equals(center.getDeriveVariableModelType())) {
                String en = center.getVariableNameEn();
                ssss = ssss + en + ",";
                shbase = shbase + " cast(o." + en + " as STRING) as " + en + ",";
            }
        }

        if (resultSource != null) {
            Map map = tVariablePackageManagerService.getKeyByVariableId(pkManager.getVariableClassification());

            // 若有决策引擎，把sql添加进去
            for (int i = 0; i < l.size(); i++) {
                TVariableCenter tVariableCenter = l.get(i);
                if ("02".equals(tVariableCenter.getVariableType()) && "02".equals(tVariableCenter.getDeriveVariableModelType())) {
                    sb.append(tVariableCenter.getSqlContext()).append(" AS ").append(tVariableCenter.getVariableNameEn()).append(",");
                }
            }
            // 移除到originalVariableSql去了
            // 若有原始变量，把原始变量加进去
            ArrayList array = (ArrayList) pkManager.getOriginalVariable();
            if (array != null && array.size() > 0) {
                // 拼接字段
                String field = "";
                for (int i = 0; i < array.size(); i++) {
                    String o = array.get(i).toString();
                    String[] split = o.split("\\.");
                    field = field + split[1] + ",";
                }
                sb.append(field);
            }

            if ("03".equals(resultSource.getConnectorType())) {  // hbase
                sb.append(map.get("schemaPrimaryKey")).append(", row(");
                // 普通查询和统计查询的输出字段为变量的英文名  一对一
                // 数据加工可能有多个输出字段，不能为变量的英文名，为函数的输出字段

                if (ssss.length() > 0) sb.append(ssss, 0, ssss.length() - 1).append(") as family from ");
                sb.append("(select cast(o.").append(map.get("schemaPrimaryKey")).append(" as STRING) as ").append(map.get("schemaPrimaryKey")).append(",");
                if (shbase.length() > 0) sb.append(shbase, 0, shbase.length() - 1);
                if ("test".equals(runFlag)) {
                    sb.append(" FROM " + millis + "o))"); // 测试的时候去 test_var_topic
                } else {
                    sb.append(" FROM tmp_").append(pkManager.getVariablePackEn()).append(" o))");
                }
            } else {
                // 普通查询和统计查询的输出字段为变量的英文名  一对一
                // 数据加工可能有多个输出字段，不能为变量的英文名，为函数的输出字段
                sb.append(ssss);
                sb.append(map.get("schemaPrimaryKey"));
//                if (ssss.length() > 0) sb.append(ssss, 0, ssss.length() - 1);
//                sb.append(" from tmp_").append(pkManager.getVariablePackEn()).append(")");
                if ("test".equals(runFlag)) {
                    sb.append(" FROM " + millis + ")");
                } else {
                    sb.append(" FROM tmp_").append(pkManager.getVariablePackEn()).append(")");
                }
            }
        }
        mapResult.put("resultSql", sb.toString());
        return mapResult;
    }

    /**
     * 修改变量包管理
     *
     * @param tVariablePackageManager 变量包管理
     * @return 结果
     */
    @Override
    public int updateTVariablePackageManager(Map map, TVariablePackageManager tVariablePackageManager) {
        tVariablePackageManager.setUpdateBy(SecurityUtils.getUsername());
        if (tVariablePackageManager.getVariablePackType().equals("03")) { // oraclecdc
            tVariablePackageManager.setResultTableSql(editOraclecdcSql(map, tVariablePackageManager));
            tVariablePackageManager.setVariableId(null);
        } else if (tVariablePackageManager.getVariablePackType().equals("02")) { // mysqlcdc
            tVariablePackageManager.setResultTableSql(setMysqlResultSql(tVariablePackageManager));
            tVariablePackageManager.setVariableId(null);
        } else if (tVariablePackageManager.getVariablePackType().equals("01")) { // 一般变量包
            Map map1 = setResultTableSql(tVariablePackageManager, "start", "");
            tVariablePackageManager.setResultTableSql(map1.get("resultSql").toString());
            tVariablePackageManager.setVariableId(JSON.toJSONString(tVariablePackageManager.getVariableId()));
            tVariablePackageManager.setOriginalVariableSql(joinOriginalVariableSql(map, tVariablePackageManager));
        }
        ArrayList array = (ArrayList) tVariablePackageManager.getOriginalVariable();
        if (array != null && array.size() > 0) {
            tVariablePackageManager.setOriginalVariable(JSON.toJSONString(tVariablePackageManager.getOriginalVariable()));
        }
        return tVariablePackageManagerMapper.updateTVariablePackageManager(tVariablePackageManager);
    }

    @Override
    public int updateTVariablePackageManagerOnApplication(TVariablePackageManager tVariablePackageManager) {
        return tVariablePackageManagerMapper.updateTVariablePackageManagerOnApplication(tVariablePackageManager);
    }

    @Override
    public int updateRuningState(ArrayList list) {
        return tVariablePackageManagerMapper.updateRuningState(list);
    }

    @Override
    public int updateStatus(TVariablePackageManager tVariablePackageManager) {
        tVariablePackageManager.setModifyTime(new Date());
        return tVariablePackageManagerMapper.updateTVariablePackageManager(tVariablePackageManager);
    }

    // 根据版本号和变量包英文名查变量包
    @Override
    public TVariablePackageManager selectTVariablePackageManagerByVersion(TVariablePackageManager tVariablePackageManager) {
        return tVariablePackageManagerMapper.selectTVariablePackageManagerByVersion(tVariablePackageManager);
    }

    /**
     * 批量删除变量包管理
     *
     * @param variablePackIds 需要删除的变量包管理ID
     * @return 结果
     */
    @Override
    public int deleteTVariablePackageManagerByIds(Long[] variablePackIds) {
        return tVariablePackageManagerMapper.deleteTVariablePackageManagerByIds(variablePackIds);
    }

    /**
     * 删除变量包管理信息
     *
     * @param variablePackId 变量包管理ID
     * @return 结果
     */
    @Override
    public int deleteTVariablePackageManagerById(Long variablePackId) {
        return tVariablePackageManagerMapper.deleteTVariablePackageManagerById(variablePackId);
    }

    // 根据变量分类查数据源表主键
    @Override
    public Map getKeyByVariableId(Long variableId) {
        return tVariableClassificationMapper.selectSchemaPrimaryKeyByVariableId(variableId);
    }

    @Override
    public List<TVariableCenter> getVariableListByIds(Object variableIds) {
//        JSONArray parse = (JSONArray)JSON.parse(variableIds.toString());
//        Object[] objects =parse.toArray();
        ArrayList variableId = (ArrayList) variableIds;
        Object[] objects = variableId.toArray();
        String[] names = new String[objects.length];
        for (int i = 0; i < objects.length; i++) {
            names[i] = objects[i].toString();
        }
        List<TVariableCenter> list = new ArrayList<>();
        List<TVariableCenter> tVariableCenters = tVariableCenterMapper.selectTVariableCenterByNames(names);
        list.addAll(tVariableCenters);
        // 目的是移除派生变量的基础变量
        ArrayList<TVariableCenter> listDmo = new ArrayList<>();
        if (tVariableCenters != null && tVariableCenters.size() > 0) {
            for (int i = 0; i < tVariableCenters.size(); i++) {
                TVariableCenter tVariableCenter = tVariableCenters.get(i);
                if ("02".equals(tVariableCenter.getVariableType())
                        && "01".equals(tVariableCenter.getDeriveVariableModelType())) { // 派生变量
                    String deriveBaseVariable = tVariableCenter.getDeriveBaseVariable().toString();
                    JSONArray array = JSON.parseArray(deriveBaseVariable);
                    for (int j = 0; j < array.size(); j++) {
                        for (int k = 0; k < list.size(); k++) {
                            TVariableCenter tVariableCenter1 = list.get(k);
                            if (tVariableCenter1.getVariableNameEn().equals(array.get(j))) {
                                listDmo.add(tVariableCenter1);
                            }
                        }
                    }
                }
            }
        }
        list.removeAll(listDmo);
        return list;
    }

    @Override
    public List getTestResult(Object jobId) {
        List list = new ArrayList();
        if (jobId != null) {
            List messageList = tVariableCenterService.
                    kafkaMessageGet(PropertiesUtil.getPro(TESTKAFKAADDRESS), "test_var_topic_package");
        }
        return list;
    }

    @Override
    public List testRun(String paramJsonString) {

        return null;
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
    public String variableTest(TVariablePackageManager pk, Map map, List<TVariableCenter> variableListByIds, String runFlag, String millis) {
        Map mapParam = new HashMap();
        mapParam.put("runMode", "01");
        mapParam.put("kafkaZK", "master:2181"); // 数据源表的zk地址
        mapParam.put("testTopicName", millis); //
        mapParam.put("sourceTableSql", map.get("createTableSql"));
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
        ArrayList testDimdata = (ArrayList) pk.getTestDimdata();
        tVariableCenterService.parseJoinSQl(mapParam, dimensionRelation, map, testDimdata);
        // 拼接条件运行sql
        StringBuilder sb2 = new StringBuilder();
        StringBuilder sb3 = new StringBuilder();
        for (int i = 0; i < variableListByIds.size(); i++) {
            if ("02".equals(variableListByIds.get(i).getVariableType()) &&
                    "01".equals(variableListByIds.get(i).getDeriveVariableModelType())) {
                sb2.append(variableListByIds.get(i).getSqlContext()).append("|");
            } else if ("01".equals(variableListByIds.get(i).getVariableType())) {
                sb3.append(variableListByIds.get(i).getSqlContext()).append(";");
            }
        }
        if (sb2.length() > 0) {
            mapParam.put("deVariableSqls", sb2.toString().substring(0, sb2.length() - 1));
        }
        if (sb3.length() > 0) { // 决策引擎依赖的基础变量也放这里
            mapParam.put("variableSqls", sb3.toString().substring(0, sb3.length() - 1));
        }
        // 字段输出个数   数据加工可能会有多个
        ArrayList variableId = (ArrayList) pk.getVariableId();
        Object[] objects = variableId.toArray();

        int num = 0;
        // 原始变量
        ArrayList originalVariable = (ArrayList) pk.getOriginalVariable();
        if (originalVariable != null) {
            for (int i = 0; i < originalVariable.size(); i++) {
                String s = originalVariable.get(i).toString();
                String[] split = s.split("\\.");
                if (!map.get("schemaPrimaryKey").equals(split[1])) {
                    num = num + 1;
                }
            }
        }

        Map map1 = setResultTableSql(pk, runFlag, millis);

        // 输出参数sql
        mapParam.put("sinkSql", map1.get("resutltSql"));


        mapParam.put("fieldOutNum", (int) map1.get("outVariableNum") + 1 + num);
        mapParam.put("sourcePrimaryKey", map.get("schemaPrimaryKey").toString());
        ArrayList sourceTableValue = (ArrayList) pk.getSourceTableValue();
        if (sourceTableValue != null && sourceTableValue.size() > 0) {
            ArrayList value = new ArrayList();
            for (int i = 0; i < sourceTableValue.size(); i++) {
                Map mapNew = new HashMap();
                Map o = (Map) sourceTableValue.get(i);
                Set<String> strings = o.keySet();
                Set<Map.Entry<String, String>> entryseSet = o.entrySet();
                for (Map.Entry<String, String> entry : entryseSet) {
                    if (entry.getKey().indexOf("-") > 0) {
                        String[] split = entry.getKey().split("-");
                        mapNew.put(split[0], entry.getValue());
                    } else {
                        mapNew.put(entry.getKey(), entry.getValue());
                    }

                }
                value.add(mapNew);
            }
            mapParam.put("testSourcedata", value);
        }


        // 原始变量
        if (originalVariable != null) {
            mapParam.put("originalVariableSql", pk.getOriginalVariableSql());
        }

        // 并行
//        mapParam.put("concurrency", "1");
        // 数据结果表的参数
        TDataResultSource resultSource = resultSourceService.selectTDataResultSourceByTableName(pk.getResultTable());
        if (resultSource != null) {
            mapParam.put("connectorType", resultSource.getConnectorType());
            if ("01".equals(resultSource.getConnectorType())) {
                mapParam.put("kafkaTopic", resultSource.getTopicName());
                mapParam.put("kafkaZK", resultSource.getZookeeperAddress());
                mapParam.put("kafkaAddress", resultSource.getKafkaAddress());
            } else if ("02".equals(resultSource.getConnectorType())) {
                mapParam.put("jdbcURL", resultSource.getJdbcUrlAddress());
                mapParam.put("jdbcDrive", resultSource.getJdbcDrive());
                mapParam.put("jdbcUserName", resultSource.getJdbcUserName());
                mapParam.put("jdbcUserPwd", resultSource.getJdbcUserPwd());
            } else if ("03".equals(resultSource.getConnectorType())) {
                mapParam.put("hbaseZK", resultSource.getHbaseZKAddress());
            } else if ("04".equals(resultSource.getConnectorType())) {
                mapParam.put("esAddress", resultSource.getEsAddress());
            }
        }
        return JSON.toJSONString(mapParam).replaceAll(map.get("tableName") + "\\.", "");
    }

    @Override
    public String[] joinMysqlPath(Map map, TVariablePackageManager pkManager) {
        Map mapParam = new HashMap();
        //变量包名字、SQL（以分号拼接）、字段个数、主键名称、运行or测试、资源配置情况（以分号拼接）并发数、taskmanager内存、jobmanager内存
        mapParam.put("variablePackEn", pkManager.getVariablePackEn());

        mapParam.put("sourceTableSql", map.get("createTableSql"));

        //waterMark
        mapParam.put("waterMark", map.get("waterMarkName") + "|" + map.get("waterMarkTime"));

        // 输出类型
        mapParam.put("connectorType", "01");

        // 运行参数 测试
        mapParam.put("runMode", "02");

        // 数据源表主键
        mapParam.put("sourcePrimaryKey", map.get("schemaPrimaryKey").toString());

        // 输出参数sql
        mapParam.put("sinkSql", pkManager.getResultTableSql());

        // 数据结果表的参数
        TDataResultSource resultSource = resultSourceService.selectTDataResultSourceByTableName(pkManager.getResultTable());
        if (resultSource != null) {
            mapParam.put("connectorType", resultSource.getConnectorType());
            if ("01".equals(resultSource.getConnectorType())) {
                mapParam.put("kafkaTopic", resultSource.getTopicName());
                mapParam.put("kafkaZK", resultSource.getZookeeperAddress());
                mapParam.put("kafkaAddress", resultSource.getKafkaAddress());
            } else if ("02".equals(resultSource.getConnectorType())) {
                mapParam.put("jdbcURL", resultSource.getJdbcUrlAddress());
                mapParam.put("jdbcDrive", resultSource.getJdbcDrive());
                mapParam.put("jdbcUserName", resultSource.getJdbcUserName());
                mapParam.put("jdbcUserPwd", resultSource.getJdbcUserPwd());
            } else if ("03".equals(resultSource.getConnectorType())) {
                mapParam.put("hbaseZK", resultSource.getHbaseZKAddress());
            } else if ("04".equals(resultSource.getConnectorType())) {
                mapParam.put("esAddress", resultSource.getEsAddress());
            }
        }

        // 启动资源配置
        String source = sourceConfiguration(pkManager);
        // point 的配置
        String points = joinPoints(pkManager);

        LOG.info("----参数:" + JSONObject.toJSONString(mapParam));
        return new String[]{"sh", PropertiesUtil.getPro(PACKAGETESTPATH) + PropertiesUtil.getPro(PACKAGETESTNAME),
                base64(JSONObject.toJSONString(mapParam)) + " " + source + " " + points};
    }

    @Override
    public String[] joinPath(Map map, TVariablePackageManager pkManager,
                             List<TVariableCenter> variableListByIds) {
        Map mapParam = new HashMap();
        //变量包名字、SQL（以分号拼接）、字段个数、主键名称、运行or测试、资源配置情况（以分号拼接）并发数、taskmanager内存、jobmanager内存
        mapParam.put("variablePackEn", pkManager.getVariablePackEn());
        // SQL
//        mapParam.put("sourceTableSql", map.get("createTableSql"));

        // 如果有两个数据源表，sourceTableSql用分号隔开，新增sourceTableSql参数
        if(map.containsKey("sourceTwoDabRelation") && StringUtils.isNotEmpty(map.get("sourceTwoDabRelation").toString()) && StringUtils.isNotNull(map.get("sourceTwoDabRelation").toString())){
            mapParam.put("sourceTableSql", map.get("createTableSql")+";"+map.get("createTableTwoSql"));
            JSONObject sourceRelation = JSON.parseObject(map.get("sourceRelation").toString());
            parseTwoStreamJoinSQl(mapParam,map, sourceRelation);
        }
        else{
            mapParam.put("sourceTableSql", map.get("createTableSql"));
        }

        //waterMark
        mapParam.put("waterMark", map.get("waterMarkName") + "|" + map.get("waterMarkTime"));

        // 有维表时
        JSONArray dimensionRelation = JSON.parseArray(map.get("dimensionRelation").toString());
        if (dimensionRelation != null && dimensionRelation.size() > 0) {
            String s = "";
            String t = "";
            List listNew = new ArrayList();
            for (int i = 0; i < dimensionRelation.size(); i++) {
                JSONObject o = (JSONObject) dimensionRelation.get(i);
                String dimensionName = o.get("dimensionName").toString();
                if (!"".equals(dimensionName)) {
                    s = s + dimensionName + "_join_";
                    t = t + dimensionName + ".*,";
                    listNew.add(dimensionName);
                }

            }
            if (listNew.size() > 0) {
                List<TDimensionTable> dimensionTables = tDimensionTableMapper.getTDimensionTableListByNames(listNew.toArray());
                List list = new ArrayList();
                String m = "";
                String es = "";
                if (dimensionTables != null && dimensionTables.size() > 0) { // 有维表
                    for (int i = 0; i < dimensionTables.size(); i++) {
                        TDimensionTable table = dimensionTables.get(i);
                        Map mapTmp = new HashMap();
                        for (int j = 0; j < dimensionRelation.size(); j++) {
                            JSONObject o = (JSONObject) dimensionRelation.get(j);
                            String dimensionName = o.get("dimensionName").toString();

                            if (dimensionName.equals(table.getDimensionName())) {
                                if ("02".equals(table.getConnectorType())) { // jdbc
                                    String sourceDabField = o.get("sourceDabField").toString();
                                    mapTmp.put("dimensionTableSql", table.getJdbcCreateSql());
                                    m = m + " left join " + dimensionName + " FOR SYSTEM_TIME AS OF s.proctime AS " +
                                            dimensionName + " ON s." + sourceDabField + " = " + dimensionName + "." + table.getJdbcPrimaryKey();
                                    mapTmp.put("testDimType", "02");
                                } else if ("03".equals(table.getConnectorType())) { // hbase
                                    String sourceDabField = o.get("sourceDabField").toString();
                                    mapTmp.put("dimensionTableSql", table.getHbaseCreateSql());
                                    mapTmp.put("testDimType", "03");
                                    m = m + " left join " + dimensionName + " FOR SYSTEM_TIME AS OF s.proctime AS " +
                                            dimensionName + " ON s." + sourceDabField + " = " + dimensionName + "." + table.getRowkey();
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
                                        mapTmp.put("dimensionTableSql", table.getEsCreateSql());
                                        mapTmp.put("testDimType", table.getConnectorType());
                                        if (!"".equals(es)) {
                                            es = es + ";";
                                        }
                                        for (int k = 0; k < r.size(); k++) {
                                            JSONObject a = (JSONObject) r.get(k);
                                            String dimensionDabField = a.get("dimensionDabField").toString();
                                            String sourceDabField = a.get("sourceDabField").toString();
                                            es = es + sourceDabField + " = " + dimensionName + "." + dimensionDabField;
                                            if (k < r.size() - 1) {
                                                es = es + " AND ";
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        if (mapTmp.size() > 0) list.add(mapTmp);
                    }
                    mapParam.put("dimensionTable", list);
                    StringBuilder sb = new StringBuilder();
                    // 新表 create TABLE xxxxx as select s.*,t.* from blackList s left join jdbc2 t on s.id = t.id；
                    String dimensionNameJOIN = s.substring(0, s.length() - 6);
                    if ("".equals(m)) {
                        sb.append(es);
                    } else if ("".equals(es)) {
                        sb.append("create table `" + map.get("tableName") + "_join_" + dimensionNameJOIN
                                + "` (select s.*," + t.substring(0, t.length() - 1) + " from " + map.get("tableName") + " s " + m + ")");
                    } else if (!"".equals(m) && !"".equals(es)) {
                        sb.append("create table `" + map.get("tableName") + "_join_" + dimensionNameJOIN
                                + "` (select s.*," + t.substring(0, t.length() - 1) + " from " + map.get("tableName") + " s " + m + ");" + es);
                    }
                    mapParam.put("joinSql", sb.toString());
                }
            }


        }

        // 原始变量
        Object originalVariable1 = pkManager.getOriginalVariable();
        int num = 0;
        if (originalVariable1 != null) {
            JSONArray originalVariable = (JSONArray) JSON.parseArray(originalVariable1.toString());
            mapParam.put("originalVariableSql", pkManager.getOriginalVariableSql());
            for (int i = 0; i < originalVariable.size(); i++) {
                String s = originalVariable.get(i).toString();
                String[] split = s.split("\\.");
                if (!map.get("schemaPrimaryKey").equals(split[1])) {
                    num = num + 1;
                }
            }
        }

        // 拼接条件运行sql
        StringBuilder sb2 = new StringBuilder();
        StringBuilder sb3 = new StringBuilder();
        for (int i = 0; i < variableListByIds.size(); i++) {
            if ("02".equals(variableListByIds.get(i).getVariableType())
                    && "01".equals(variableListByIds.get(i).getDeriveVariableModelType())) { // 如果是派生变量 且是四则运算，加上表名  .append(" tmp_").append(pkManager.getVariablePackEn()).append(";")
                sb2.append(variableListByIds.get(i).getSqlContext());
            } else if ("01".equals(variableListByIds.get(i).getVariableType())) {
                sb3.append(variableListByIds.get(i).getSqlContext()).append(";");
            }
        }
        if (sb2.length() > 0) {
            mapParam.put("deVariableSqls", sb2.toString());
        }
        if (sb3.length() > 0) {
            mapParam.put("variableSqls", sb3.toString().substring(0, sb3.length() - 1));
        }

        // 字段输出个数   数据加工可能会有多个
        ArrayList variableId = (ArrayList) pkManager.getVariableId();
        mapParam.put("fieldOutNum", variableListByIds.size() + 1 + num);

        // 数据源表主键
        mapParam.put("sourcePrimaryKey", map.get("schemaPrimaryKey").toString());
        // 运行参数 测试
        mapParam.put("runMode", "02");
        //
        mapParam.put("kafkaZK", "master:2181");
        // 输出参数sql
        mapParam.put("sinkSql", pkManager.getResultTableSql());
        // 数据结果表的参数
        TDataResultSource resultSource = resultSourceService.selectTDataResultSourceByTableName(pkManager.getResultTable());
        if (resultSource != null) {
            mapParam.put("connectorType", resultSource.getConnectorType());
            if ("01".equals(resultSource.getConnectorType())) {
                mapParam.put("kafkaTopic", resultSource.getTopicName());
                mapParam.put("kafkaZK", resultSource.getZookeeperAddress());
                mapParam.put("kafkaAddress", resultSource.getKafkaAddress());
            } else if ("02".equals(resultSource.getConnectorType())) {
                mapParam.put("jdbcURL", resultSource.getJdbcUrlAddress());
                mapParam.put("jdbcDrive", resultSource.getJdbcDrive());
                mapParam.put("jdbcUserName", resultSource.getJdbcUserName());
                mapParam.put("jdbcUserPwd", resultSource.getJdbcUserPwd());
            } else if ("03".equals(resultSource.getConnectorType())) {
                mapParam.put("hbaseZK", resultSource.getHbaseZKAddress());
            } else if ("04".equals(resultSource.getConnectorType())) {
                mapParam.put("esAddress", resultSource.getEsAddress());
            }
        }

        // 启动资源配置
        String source = sourceConfiguration(pkManager);
        // point 的配置
        String points = joinPoints(pkManager);

        // variablePackEn /  sourceTableSql  / dimensionTableSql  /  joinSql  /  variableSqls  / fieldOutNum /
        // sourcePrimaryKey /  runParam /  concurrency  /  jobManagerMemory   / taskManagerMemory  / sinkSql
        // connectorType /kafkaTopic/kafkaZK/kafkaAddress/jdbcURL/jdbcDrive/jdbcUserName/jdbcUserPwd/hbaseZK/esAddress
        LOG.info("----参数:" + JSONObject.toJSONString(mapParam));
        return new String[]{"sh", PropertiesUtil.getPro(PACKAGETESTPATH) + PropertiesUtil.getPro(PACKAGETESTNAME),
                base64(JSONObject.toJSONString(mapParam)) + " " + source + " " + points};
    }

    @Override
    public String[] joinOraclePath(TVariablePackageManager pkManager){
        Map mapParam = new HashMap();
        mapParam.put("sinkSql", JSON.parseObject(pkManager.getResultTableSql()));
        // 启动资源配置
        String source = sourceConfiguration(pkManager);
        // point 的配置
        String points = joinPoints(pkManager);

        LOG.info("----参数:" + JSONObject.toJSONString(mapParam));
        return new String[]{"sh", PropertiesUtil.getPro(PACKAGETESTPATH) + PropertiesUtil.getPro(PACKAGETESTNAME),
                base64(JSONObject.toJSONString(mapParam)) + " " + source + " " + points};
    }

    private String joinPoints(TVariablePackageManager pkManager) {
        String result = PropertiesUtil.getPro(PACKAGESTARTJOBID);
        if (pkManager.getJobId() != null) {
            result = result + pkManager.getJobId();
        }
        result = result + " " + PropertiesUtil.getPro(PACKAGESTARTVARDIR) + pkManager.getVarDir();
        return result;
    }

    // base64加密
    private static String base64(String message) {
        byte[] bytes = message.getBytes();
        //Base64 加密
        String encoded = Base64.getEncoder().encodeToString(bytes);
        return encoded;

    }

    private String sourceConfiguration(TVariablePackageManager pkManager) {
        String source = "";
        if ("radio1".equals(pkManager.getConfigurationType())) { // 智能配置
            source = "1 1024 1024";
        } else if ("radio2".equals(pkManager.getConfigurationType())) { // 手动配置
            source = pkManager.getConcurrency() + " " + pkManager.getJobMemory() + " " + pkManager.getTaskMemory();
        } else if ("radio3".equals(pkManager.getConfigurationType())) { // 使用上次资源配置
            source = pkManager.getConcurrency() + " " + pkManager.getJobMemory() + " " + pkManager.getTaskMemory();
        }
        return source;
    }

    // 启动入口
    @Override
    public Map exe(String[] path) {
        Map<String, Object> map = new HashMap();
        try {
            Runtime runtime = Runtime.getRuntime();
            LOG.info("----------------exec--------start------------");
            Process pro = runtime.exec(path);
            LOG.info("----------------exec--------end------------");

            BufferedReader br = new BufferedReader(new InputStreamReader(pro.getInputStream()));
            StringBuffer strbr = new StringBuffer();
            String line;
            String jobId = null;
            String applicationId = null;
            while ((line = br.readLine()) != null) {
                strbr.append(line).append("\n");
                if (line.contains("Submitted application")) {
                    String[] words = line.split(" ");
                    applicationId = words[words.length - 1];
                    LOG.info("-----------------applicationId为：" + applicationId);

                }
                if (line.contains("Job has been submitted with JobID")) {
                    String[] words = line.split(" ");
                    jobId = words[words.length - 1];
                    LOG.info("-----------------启动成功！JobID为：" + jobId);
                    break;
                }
            }
            map.put("applicationId", applicationId);
            map.put("jobId", jobId);
            LOG.info("--------------------------strbr:" + strbr.toString());
            LOG.info("--------------------------strbr------------------------end----------------");

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
                map.put("exception", errline.toString());
            }
        } catch (IOException ec) {
            ec.printStackTrace();
            throw new CustomException("该作业启动异常！");
        }
        return map;
    }

    @Override
    public Map<String, String> jobKill(TVariablePackageManager pk) {
        Map<String, String> map = new HashMap<>();
        String pointDir = Strings.isNullOrEmpty(pk.getSavePointDir()) ? PropertiesUtil.getPro(PACKAGESTARTVARDIR) + pk.getVarDir() : pk.getSavePointDir();

        String[] path = new String[]{"sh", PropertiesUtil.getPro(PACKAGETESTKILLPATH) + PropertiesUtil.getPro(PACKAGETESTKILLNAME),
                pk.getApplicationId() + " " + pk.getJobId() + " " + pointDir + " " + pk.getStartParamBase()};
        LOG.info("-----------------path+name：" + path[1]);
        LOG.info("-----------------appid：" + path[2]);
        try {
            Runtime runtime = Runtime.getRuntime();
            Process pro = runtime.exec(path);
            BufferedReader br = new BufferedReader(new InputStreamReader(pro.getInputStream()));
            String line;
            while ((line = br.readLine()) != null) {
                LOG.info("-----------------line--：" + line);
                if (line.contains("Cancelled job " + pk.getJobId() + ". Savepoint stored in " + PropertiesUtil.getPro(PACKAGESTARTVARDIR) + pk.getVarDir())) { // 成功停止
                    int i = line.indexOf("Savepoint stored in ");
                    String substring = line.substring(i + 20, line.length() - 1);
                    String savepoint = substring + "/_metadata";
                    map.put("savepoint", savepoint);
                }
                if ((line.contains("Job with JobID") && line.contains("finished")) || line.contains("It has previously completed with final status: FAILED")) { // 成功停止

                    map.put("result", "success");
                    LOG.info("-----------------成功停止：" + pk.getJobId());
                }
            }
        } catch (IOException ec) {
            ec.printStackTrace();
            map.put("result", "unsuccess");
            throw new CustomException("作业停止失败！");

        }
        return map;
    }


    // 拼接建表sql
    private String joinCreatSql(String sourceTableName, String dimentionId, String testRun) {
        //-----------------------------------------CreateSourceTable Start-----------------------------------------
        String sqlString = "";

        // 数据表表sql
//        if (sourceTableName != null) {
//            TDataSource sourceTable = tDataSourceMapper.selectTDataSourceByTableName(sourceTableName);
//            String sourceTableSchema = schemaTransform(sourceTable.getSchemaDefine());
//            //01代表kafka连接器
//            if ("01".equals(sourceTable.getConnectorType())) {
//                String consumerMode;
//                String topicName;
//                if ("1".equals(testRun)) {
//                    topicName = "ferghana_test_" + sourceTable.getTopicName();
//                    consumerMode = "earliest-offset";
//                } else {
//                    if ("01".equals(sourceTable.getConsumerMode())) {
//                        consumerMode = "latest-offset";
//                    } else {
//                        consumerMode = "earliest-offset";
//                    }
//                    topicName = sourceTable.getTopicName();
//                }
//                String waterMark = sourceTable.getWaterMarkName() != null ?
//                        sourceTable.getWaterMarkName() + " as " + sourceTable.getWaterMarkName() + " - INTERVAL '" + sourceTable.getWaterMarkTime() + "' SECOND"
//                        : "proctime as proctime - INTERVAL '" + sourceTable.getWaterMarkTime() + "' SECOND";
//                String waterSchema = sourceTable.getWaterMarkName() != null ? "," + sourceTable.getWaterMarkName() + " TIMESTAMP" : " ";
//                // proctime AS PROCTIME(),WATERMARK FOR TRADE_TIME AS TRADE_TIME - INTERVAL '0' SECOND
//                sqlString = "CREATE TABLE " + sourceTable.getTableName() + "(" + sourceTableSchema + waterSchema +
//                        ",proctime AS PROCTIME(),WATERMARK FOR " + waterMark +
//                        ") WITH ('connector.type' = 'kafka','connector.version' = '0.11','connector.topic' = '"
//                        + topicName + "','connector.properties.zookeeper.connect' = '" + sourceTable.getZookeeperAddress()
//                        + "','connector.properties.bootstrap.servers' = '" + sourceTable.getKafkaAddress()
//                        + "','connector.properties.group.id' = '" + sourceTable.getConsumerGroup()
//                        + "','connector.startup-mode' = '" + consumerMode + "','format.type' = 'json')";
//            }
//        }


//    CREATE TABLE test_jdbc -维表表名 (CUST_NO STRING,name STRING,age INT,PRIMARY KEY (CUST_NO -主键) NOT ENFORCED) WITH
//            ('connector' = 'jdbc','url' = 'jdbc:mysql://192.168.43.105:3306/upwisdom','driver' = 'com.mysql.cj.jdbc.Driver',
//                     'username' = 'root','password' = 'qwer','table-name' = 'users' -维表表名  )

        // 数据维表sql
//        if (dimentionId != null) {
//            StringBuilder sb = new StringBuilder();
//            sb.append(";");
//            TDimensionTable table = tDimensionTableMapper.selectTDimensionTableById(new Long(dimentionId));
//            if ("02".equals(table.getConnectorType())) { // jdbc
//                sb.append(" CREATE TABLE ").append(table.getDimensionName())
//                        .append(" ( ").append(schemaTransform(table.getSchemaDefine()))
//                        .append(" ,PRIMARY KEY (").append(table.getJdbcPrimaryKey()).append(")").append(" NOT ENFORCED) WITH ")
//                        .append(" ('connector' = 'jdbc', 'url' = '").append(table.getJdbcUrlAddress()).append("',")
//                        .append("'driver' = '").append(table.getJdbcDrive()).append("',")
//                        .append("'username' = '").append(table.getJdbcUserName()).append("',")
//                        .append("'password' = '").append(table.getJdbcUserPwd()).append("',")
//                        .append("'table-name' = '").append(table.getDimensionName()).append("')");
//            }
//            sqlString = sqlString + sb.toString();
//        }

        return sqlString;
        //-----------------------------------------CreateSourceTable End-----------------------------------------
    }

//    private String schemaTransform(String schemaDefine) {
//        schemaDefine = StringUtils.strip(schemaDefine, "[");
//        schemaDefine = StringUtils.strip(schemaDefine, "]");
//
//        StringBuilder sb = new StringBuilder();
//        String[] split = schemaDefine.split(",");
//        for (int i = 0; i < split.length; i++) {
//            JSONObject jsonObj = JSON.parseObject(split[i]);
//            for (Map.Entry<String, Object> entry : jsonObj.entrySet()) {
//                sb.append(entry.getKey()).append(" ").append(entry.getValue()).append(",");
//            }
//        }
//        return sb.substring(0, sb.length() - 1);
//    }

    // 拼接运行sql
    private String joinRuningSql(String variableId) {
        String sqlString = "";
        Object parse = JSON.parse(variableId);
        return sqlString;
    }
}
