package com.skyon.project.system.service.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Strings;
import com.skyon.common.exception.CustomException;
import com.skyon.common.utils.SecurityUtils;
import com.skyon.framework.aspectj.lang.annotation.DataScope;
import com.skyon.project.system.domain.TSelfFunction;
import com.skyon.project.system.mapper.TSelfFunctionMapper;
import com.skyon.project.system.service.ITSelfFunctionService;
import com.skyon.project.system.util.PropertiesUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.Security;
import java.util.*;

import static org.apache.flink.optimizer.Optimizer.LOG;

/**
 * 自定义函数Service业务层处理
 *
 *
 * @date 2020-06-18
 */
@Service
public class TSelfFunctionServiceImpl implements ITSelfFunctionService {

    private static final String FLINKCLUSTERRESTARTPATH = "flink_cluster_restart_path"; // 启动脚本路径
    private static final String FLINKCLUSTERRESTARTNAME = "flink_cluster_restart_name"; // 启动脚本文件名

    @Autowired
    private TSelfFunctionMapper tSelfFunctionMapper;

    /**
     * 查询自定义函数
     *
     * @param selfFunctionId 自定义函数ID
     * @return 自定义函数
     */
    @Override
    public TSelfFunction selectTSelfFunctionById(Long selfFunctionId) {
        TSelfFunction tSelfFunction = tSelfFunctionMapper.selectTSelfFunctionById(selfFunctionId);
        String inputParam = tSelfFunction.getInputParam();
        tSelfFunction.setDynamicItem(setParam(inputParam));
        String outputParam = tSelfFunction.getOutputParam();
        tSelfFunction.setDynamicItemOut(setParam(outputParam));
        return tSelfFunction;
    }

    private Object[] setParam(String param) {
        if (!Strings.isNullOrEmpty(param)) {
            JSONArray parse = (JSONArray) JSONObject.parse(param);
            Object[] oArr = new Object[parse.size()];
            for (int i = 0; i < parse.size(); i++) {
                Map o = (Map) parse.get(i);
                Map map = new HashMap();
                map.put("schemaDefine", o.get("schemaDefine"));
                map.put("dataBaseType", o.get("dataBaseType"));
                map.put("fieldType", o.get("fieldType"));
                map.put("schemaName", o.get("schemaName"));
                oArr[i] = map;
            }
            return oArr;
        }
        return null;
    }

    /**
     * 查询自定义函数列表
     *
     * @param tSelfFunction 自定义函数
     * @return 自定义函数
     */
    @Override
    @DataScope(serviceTable = "1")
    public List<TSelfFunction> selectTSelfFunctionList(TSelfFunction tSelfFunction) {
        return tSelfFunctionMapper.selectTSelfFunctionList(tSelfFunction);
    }
    @Override
    public List<TSelfFunction> selectTSelfFunctionList() {
        return tSelfFunctionMapper.selectTSelfAndFunctionList();
    }

    /**
     * 新增自定义函数
     *
     * @param tSelfFunction 自定义函数
     * @return 结果
     */
    @Override
    public int insertTSelfFunction(TSelfFunction tSelfFunction) {
        tSelfFunction.setCreateBy(SecurityUtils.getUsername());
        tSelfFunction.setCreateId(SecurityUtils.getUserId());
        return tSelfFunctionMapper.insertTSelfFunction(tSelfFunction);
    }

    /**
     * 修改自定义函数
     *
     * @param tSelfFunction 自定义函数
     * @return 结果
     */
    @Override
    public int updateTSelfFunctionIncludeJar(TSelfFunction tSelfFunction, String jarPath) {
        TSelfFunction tSelfFunction2 = tSelfFunctionMapper.selectTSelfFunctionById(tSelfFunction.getSelfFunctionId());
        String filePath = tSelfFunction2.getFilePath();
        File file = new File(filePath);
        if (file.exists() && file.isFile() && !filePath.equals(jarPath)) {
            file.delete();
        }
        tSelfFunction.setUpdateBy(SecurityUtils.getUsername());
        return tSelfFunctionMapper.updateTSelfFunction(tSelfFunction);
    }

    @Override
    public int updateTSelfFunction(TSelfFunction tSelfFunction) {
        tSelfFunction.setUpdateBy(SecurityUtils.getUsername());
        return tSelfFunctionMapper.updateTSelfFunction(tSelfFunction);
    }

    /**
     * 批量删除自定义函数
     *
     * @param selfFunctionIds 需要删除的自定义函数ID
     * @return 结果
     */
    @Override
    public int deleteTSelfFunctionByIds(Long[] selfFunctionIds) {
        List<TSelfFunction> tSelfFunctions = tSelfFunctionMapper.selectTSelfFunctionByIds(selfFunctionIds);
        for (int i = 0; i < tSelfFunctions.size(); i++) {
            TSelfFunction tSelfFunction = tSelfFunctions.get(i);
            String filePath = tSelfFunction.getFilePath();
            File file = new File(filePath);
            if (file.exists() && file.isFile()) {
                file.delete();
            }
            String filePathTwo = tSelfFunction.getFilePathTwo();
            File fileTwo = new File(filePathTwo);
            if (fileTwo.exists() && fileTwo.isFile()) {
                fileTwo.delete();
            }
        }
        return tSelfFunctionMapper.deleteTSelfFunctionByIds(selfFunctionIds);
    }

    /**
     * 删除自定义函数信息
     *
     * @param selfFunctionId 自定义函数ID
     * @return 结果
     */
    @Override
    public int deleteTSelfFunctionById(Long selfFunctionId) {
        return tSelfFunctionMapper.deleteTSelfFunctionById(selfFunctionId);
    }

    @Override
    public void selectTSelfFunctionByNameCn(String selfFunctionNameCn) {
        List<TSelfFunction> tSelfFunctions = tSelfFunctionMapper.selectTSelfFunctionByNameCn(selfFunctionNameCn);
        if (tSelfFunctions != null && tSelfFunctions.size() > 0) {
            throw new CustomException("该函数中文名已存在！");
        }
    }

    @Override
    public void selectTSelfFunctionByName(String name) {
        List<TSelfFunction> tSelfFunctions = tSelfFunctionMapper.selectTSelfFunctionByName(name);
        if (tSelfFunctions != null && tSelfFunctions.size() > 0) {
            throw new CustomException("该函数名已存在！");
        }
    }

    @Override
    public void selectTSelfFunctionByPackage(String packagePath) {
        List<TSelfFunction> tSelfFunctions = tSelfFunctionMapper.selectTSelfFunctionByPackage(packagePath);
        if (tSelfFunctions != null && tSelfFunctions.size() > 0) {
            throw new CustomException("该函数路径已存在！");
        }
    }

    @Override
    public void selectTSelfFunctionByNameCnNoId(TSelfFunction tSelfFunction) {
        List<TSelfFunction> tSelfFunctions = tSelfFunctionMapper.selectTSelfFunctionByNameCnNoId(tSelfFunction);
        if (tSelfFunctions != null && tSelfFunctions.size() > 0) {
            throw new CustomException("该函数中文名已存在！");
        }
    }

    @Override
    public void selectTSelfFunctionByNameNoId(TSelfFunction tSelfFunction) {
        List<TSelfFunction> tSelfFunctions = tSelfFunctionMapper.selectTSelfFunctionByNameNoId(tSelfFunction);
        if (tSelfFunctions != null && tSelfFunctions.size() > 0) {
            throw new CustomException("该函数名已存在！");
        }
    }

    @Override
    public void selectTSelfFunctionByPackageNoId(TSelfFunction tSelfFunction) {
        List<TSelfFunction> tSelfFunctions = tSelfFunctionMapper.selectTSelfFunctionByPackageNoId(tSelfFunction);
        if (tSelfFunctions != null && tSelfFunctions.size() > 0) {
            throw new CustomException("该函数路径已存在！");
        }
    }


    /**
     * 重启测试
     */
    @Override
    public void restartFlink() {
        //第一个变量是sh命令，第二个变量是需要执行的脚本路径，从第三个变量开始是我们要传到脚本里的参数。
        try {
            Runtime runtime = Runtime.getRuntime();
            //  参数加密
            // 启动脚本
            String[] path = new String[]{"sh", PropertiesUtil.getPro(FLINKCLUSTERRESTARTPATH) + PropertiesUtil.getPro(FLINKCLUSTERRESTARTNAME)};

            Process pro = runtime.exec(path);
            BufferedReader br = new BufferedReader(new InputStreamReader(pro.getInputStream()));
            StringBuffer strbr = new StringBuffer();
            String line;
            Boolean jobId = false;
            if (true) { // 变量测试
                while ((line = br.readLine()) != null) {
                    strbr.append(line).append("\n");
                    if (line.contains("Starting taskexecutor")) { // 成功
                        jobId = true;
                        LOG.info("集群重启成功！");
                        break;
                    }
                }
            }


            if (jobId == false) {
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
            throw new CustomException("集群重启失败！");
        }
    }


}
