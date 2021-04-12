package com.skyon.project.system.controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.skyon.common.utils.file.FileUploadUtils;
import com.skyon.common.utils.poi.ExcelUtil;
import com.skyon.framework.aspectj.lang.annotation.Log;
import com.skyon.framework.aspectj.lang.enums.BusinessType;
import com.skyon.framework.config.RuoYiConfig;
import com.skyon.framework.web.controller.BaseController;
import com.skyon.framework.web.domain.AjaxResult;
import com.skyon.framework.web.page.TableDataInfo;
import com.skyon.project.system.domain.TSelfFunction;
import com.skyon.project.system.service.ITSelfFunctionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.*;

/**
 * 自定义函数Controller
 * 
 *
 * @date 2020-06-18
 */
@RestController
@RequestMapping("/variable/funcdevelop")
public class TSelfFunctionController extends BaseController
{
    @Autowired
    private ITSelfFunctionService tSelfFunctionService;

    /**
     * 查询自定义函数列表
     */
//    @PreAuthorize("@ss.hasPermi('variable:funcdevelop:list')")
    @GetMapping("/list")
    public TableDataInfo list(TSelfFunction tSelfFunction)
    {
        startPage();
        List<TSelfFunction> list = tSelfFunctionService.selectTSelfFunctionList(tSelfFunction);
        return getDataTable(list);
    }


    // 查询作用函数和内置函数
    @GetMapping("/listFunction")
    public TableDataInfo listFunction()
    {
        List<TSelfFunction> list = tSelfFunctionService.selectTSelfFunctionList();
        return getDataTable(list);
    }

    /**
     * 导出自定义函数列表
     */
    @Log(title = "自定义函数", businessType = BusinessType.EXPORT)
    @GetMapping("/export")
    public AjaxResult export(TSelfFunction tSelfFunction)
    {
        List<TSelfFunction> list = tSelfFunctionService.selectTSelfFunctionList(tSelfFunction);
        ExcelUtil<TSelfFunction> util = new ExcelUtil<TSelfFunction>(TSelfFunction.class);
        return util.exportExcel(list, "function");
    }

    /**
     * 新增自定义函数
     */
    @Log(title = "自定义函数", businessType = BusinessType.INSERT)
    @PostMapping
    public AjaxResult add(@RequestBody TSelfFunction tSelfFunction)
    {


        return toAjax(tSelfFunctionService.insertTSelfFunction(tSelfFunction));
    }

    /**
     * 获取【请填写功能名称】详细信息
     */
    @GetMapping(value = "/{selfFunctionId}")
    public AjaxResult getInfo(@PathVariable("selfFunctionId") Long selfFunctionId)
    {
        return AjaxResult.success(tSelfFunctionService.selectTSelfFunctionById(selfFunctionId));
    }

    /**
     * 获取数据源表的schema字段
     */
    @RequestMapping(value = "/querySchema", method = RequestMethod.GET)
    @ResponseBody
    public TableDataInfo getInfoSchema(@RequestParam("selfFunctionId") String selfFunctionId)
    {
        List list = new ArrayList();
        List list1 = new ArrayList();
        String inputParam = tSelfFunctionService.selectTSelfFunctionById(new Long(selfFunctionId)).getInputParam();
        JSONArray parse = (JSONArray) JSONObject.parse(inputParam);
        for (int i = 0; i < parse.size(); i++) {
            list1.add(parse.get(i));
        }
        list.add(list1);

        List list2 = new ArrayList();
        String outputParam = tSelfFunctionService.selectTSelfFunctionById(new Long(selfFunctionId)).getOutputParam();
        JSONArray parse2 = (JSONArray) JSONObject.parse(outputParam);
        for (int i = 0; i < parse2.size(); i++) {
            list2.add(parse.get(i));
        }
        list.add(list2);
        return getDataTable(list);
    }

    /**
     * 修改自定义函数
     */
    @Log(title = "自定义函数", businessType = BusinessType.UPDATE)
    @PutMapping
    public AjaxResult edit(@RequestParam("functionJar") MultipartFile file,
                           @RequestParam("selfFunctionId") String selfFunctionId,
                           @RequestParam("selfFunctionNameCn") String selfFunctionNameCn,
                           @RequestParam("selfFunctionDesc") String selfFunctionDesc,
                           @RequestParam("functionName") String functionName,
                           @RequestParam("functionType") String functionType,
                           @RequestParam("functionPackagePath") String functionPackagePath,
                           @RequestParam("inputParam") String inputParam,
                           @RequestParam("outputParam") String outputParam) throws IOException {


        TSelfFunction tSelfFunction = new TSelfFunction();
        tSelfFunction.setSelfFunctionId(new Long(selfFunctionId));
        tSelfFunction.setSelfFunctionNameCn(selfFunctionNameCn);
        tSelfFunction.setFunctionName(functionName);
        tSelfFunction.setFunctionType(functionType);
        tSelfFunction.setFunctionPackagePath(functionPackagePath);
        tSelfFunction.setSelfFunctionDesc(selfFunctionDesc);
        tSelfFunction.setInputParam(inputParam);
        tSelfFunction.setOutputParam(outputParam);

        // 校驗 函数中文名 函数名 函数路径是否已经存在
        tSelfFunctionService.selectTSelfFunctionByNameCnNoId(tSelfFunction);
        tSelfFunctionService.selectTSelfFunctionByNameNoId(tSelfFunction);
        tSelfFunctionService.selectTSelfFunctionByPackageNoId(tSelfFunction);

        String jarPath = "";
        String jarPathTwo = "";
        if (!file.isEmpty())
        {
            jarPath = FileUploadUtils.uploadJar(RuoYiConfig.getProfile(), file);
            logger.info(jarPath);
            jarPathTwo = FileUploadUtils.uploadJar(RuoYiConfig.getProfileTwo(), file);
        }
        tSelfFunction.setFilePath(jarPath);
        tSelfFunction.setFilePathTwo(jarPathTwo);
        int i = tSelfFunctionService.updateTSelfFunctionIncludeJar(tSelfFunction,jarPath);
        return toAjax(i);
    }
    /**
     * 修改自定义函数
     */
    @Log(title = "自定义函数", businessType = BusinessType.UPDATE)
    @PutMapping("/updateNoJar")
    public AjaxResult updateNoJar(@RequestParam("selfFunctionId") String selfFunctionId,
                           @RequestParam("selfFunctionNameCn") String selfFunctionNameCn,
                           @RequestParam("selfFunctionDesc") String selfFunctionDesc,
                           @RequestParam("functionName") String functionName,
                           @RequestParam("functionType") String functionType,
                           @RequestParam("functionPackagePath") String functionPackagePath,
                           @RequestParam("inputParam") String inputParam,
                           @RequestParam("outputParam") String outputParam) throws IOException {

        TSelfFunction tSelfFunction = new TSelfFunction();
        tSelfFunction.setSelfFunctionId(new Long(selfFunctionId));
        tSelfFunction.setSelfFunctionNameCn(selfFunctionNameCn);
        tSelfFunction.setFunctionName(functionName);
        tSelfFunction.setFunctionType(functionType);
        tSelfFunction.setFunctionPackagePath(functionPackagePath);
        tSelfFunction.setSelfFunctionDesc(selfFunctionDesc);
        tSelfFunction.setInputParam(inputParam);
        tSelfFunction.setOutputParam(outputParam);

        // 校驗 函数中文名 函数名 函数路径是否已经存在
        tSelfFunctionService.selectTSelfFunctionByNameCnNoId(tSelfFunction);
        tSelfFunctionService.selectTSelfFunctionByNameNoId(tSelfFunction);
        tSelfFunctionService.selectTSelfFunctionByPackageNoId(tSelfFunction);



        return toAjax(tSelfFunctionService.updateTSelfFunction(tSelfFunction));
    }

    /**
     * 删除自定义函数
     */
    @Log(title = "自定义函数", businessType = BusinessType.DELETE)
	@DeleteMapping("/{selfFunctionIds}")
    public AjaxResult remove(@PathVariable Long[] selfFunctionIds)
    {
        return toAjax(tSelfFunctionService.deleteTSelfFunctionByIds(selfFunctionIds));
    }

    /**
     * 新增自定义函数
     * @param file
     * @param selfFunctionNameCn
     * @param selfFunctionDesc
     * @param functionName
     * @param functionPackagePath
     * @param inputParam
     * @param outputParam
     * @return
     * @throws IOException
     */
    @PostMapping("/upload")
    public AjaxResult uploadJar(@RequestParam("functionJar") MultipartFile file,
                                @RequestParam("selfFunctionNameCn") String selfFunctionNameCn,
                                @RequestParam("selfFunctionDesc") String selfFunctionDesc,
                                @RequestParam("functionName") String functionName,
                                @RequestParam("functionType") String functionType,
                                @RequestParam("functionPackagePath") String functionPackagePath,
                                @RequestParam("inputParam") String inputParam,
                                @RequestParam("outputParam") String outputParam) throws IOException {
        // 校驗 函数中文名 函数名 函数路径是否已经存在
        tSelfFunctionService.selectTSelfFunctionByNameCn(selfFunctionNameCn);
        tSelfFunctionService.selectTSelfFunctionByName(functionName);
        tSelfFunctionService.selectTSelfFunctionByPackage(functionPackagePath);

        TSelfFunction tSelfFunction = new TSelfFunction();
        tSelfFunction.setSelfFunctionNameCn(selfFunctionNameCn);
        tSelfFunction.setFunctionName(functionName);
        tSelfFunction.setFunctionType(functionType);
        tSelfFunction.setFunctionPackagePath(functionPackagePath);
        tSelfFunction.setSelfFunctionDesc(selfFunctionDesc);
        tSelfFunction.setInputParam(inputParam);
        tSelfFunction.setOutputParam(outputParam);
        tSelfFunction.setCreateTime(new Date());



        String jarPath = "";
        String jarPathTwo = "";
        if (!file.isEmpty())
        {
            jarPath = FileUploadUtils.uploadJar(RuoYiConfig.getProfile(), file);
            logger.info(jarPath);
            // 新增一个上传地点
            jarPathTwo = FileUploadUtils.uploadJar(RuoYiConfig.getProfileTwo(), file);
        }
        tSelfFunction.setFilePath(jarPath);
        tSelfFunction.setFilePathTwo(jarPathTwo);
        return toAjax(tSelfFunctionService.insertTSelfFunction(tSelfFunction));
    }

    /**
     *  校验jar是否已存在
     * @param file
     * @return
     */
    @PostMapping("/checkJar")
    public AjaxResult checkJar(@RequestParam("functionJar") MultipartFile file) throws IOException {
        // 先校验集群是否重启成功
        tSelfFunctionService.restartFlink();

        String exists = "";
        if (!file.isEmpty())
        {
            exists = FileUploadUtils.checkJarNameExist(RuoYiConfig.getProfile(), file);
        }
        return AjaxResult.success(exists);
    }

}
