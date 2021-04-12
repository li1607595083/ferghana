package com.skyon.project.monitor.controller;

import com.alibaba.excel.EasyExcelFactory;
import com.alibaba.excel.ExcelWriter;
import com.alibaba.excel.metadata.BaseRowModel;
import com.alibaba.excel.metadata.Sheet;
import com.google.common.collect.Lists;
import com.skyon.framework.aspectj.lang.annotation.Log;
import com.skyon.framework.aspectj.lang.enums.BusinessType;
import com.skyon.framework.config.RuoYiConfig;
import com.skyon.framework.web.controller.BaseController;
import com.skyon.framework.web.domain.AjaxResult;
import com.skyon.framework.web.page.TableDataInfo;
import com.skyon.project.monitor.service.impl.KeyindicatorsServiceImpl;
import com.skyon.project.system.domain.vo.KeyIndicators1Vo;
import com.skyon.project.system.domain.vo.Model1;
import net.sf.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.*;

/**
 * 关键指标数据1_controller
 * 
 * @author
 */
@RestController
@RequestMapping("/system/indicators_key/development")
public class KeyindicatorsController extends BaseController {

    @Autowired
    private KeyindicatorsServiceImpl keyindicatorsServiceImpl;
    /**
     * 获取用户列表
     */
//    @PreAuthorize("@ss.hasPermi('system:user:list')")
    @GetMapping("/list")
    public TableDataInfo list( ) {

        return getDataTable(keyindicatorsServiceImpl.list());
    }
    @GetMapping("/orglist")
    public AjaxResult orgList( ) {

        List<Map<String,Object>> list = Lists.newArrayList();
        Map<String,Object> map1=new HashMap<>();
        map1.put("key","5");
        map1.put("value","合计");
        list.add(map1);



        Map<String,Object> map2=new HashMap<>();
        map2.put("key","4");
        map2.put("value","马鞍山分行");
        list.add(map2);
        Map<String,Object> map3=new HashMap<>();
        map3.put("key","2");
        map3.put("value","芜湖分行");
        list.add(map3);
        Map<String,Object> map4=new HashMap<>();
        map4.put("key","3");
        map4.put("value","安庆分行");
        list.add(map4);
        return  AjaxResult.success(list);
    }



//    导出
    @Log(title = "导出信用卡重要指标数据", businessType = BusinessType.EXPORT)
//    @PreAuthorize("@ss.hasPermi('system:user:export')")
    @GetMapping("/export")
    public AjaxResult export() throws FileNotFoundException {List<KeyIndicators1Vo> lists=new ArrayList<>();
        List<Map<String,Object>> list = keyindicatorsServiceImpl.list();
        for (Map<String, Object> objectMap : list) {
            KeyIndicators1Vo vo =new KeyIndicators1Vo();

            vo.setDkdr(objectMap.get("dkdr").toString());
            vo.setOrgname(objectMap.get("orgname").toString());
            vo.setDkjsr(objectMap.get("dkjsr").toString());
            lists.add(vo);
        }
//  ExcelUtil<KeyIndicators1Vo> util = new ExcelUtil<KeyIndicators1Vo>(KeyIndicators1Vo.class);
//     return util.exportExcel(lists, "信用卡重要指标数据");

        return  test();
    }



    public Object getRequestDate(String date, Class objClass) {
        JSONObject json = JSONObject.fromObject(date);

        Object obj = JSONObject.toBean(json, objClass);

        return obj;
    }

public   AjaxResult test() throws FileNotFoundException {

    String filename = UUID.randomUUID().toString() + "_" + "信用卡重要指标数据" + ".xlsx";
    OutputStream out = new FileOutputStream(getAbsoluteFile(filename));
    ExcelWriter excelWriter =EasyExcelFactory.getWriter(out);
    Sheet sheet =new Sheet(1,0,Model1.class );
    sheet.setSheetName("my_three_excel");
    excelWriter.write(createModelList2(),sheet);
    excelWriter.finish();
    return AjaxResult.success(filename);
}
    private List<? extends BaseRowModel> createModelList2() {
        List<Model1> list = new ArrayList<>();
        for(int i=0; i<20;i++){
            Model1 excelMode = new Model1();
            excelMode.setName("1");
            excelMode.setName1("2");
            excelMode.setName2("3");
            excelMode.setName3("4");
            excelMode.setName4("5");
            list.add(excelMode);
        }
        return list;
    }
    public String getAbsoluteFile(String filename)
    {
        String downloadPath = RuoYiConfig.getDownloadPath() + filename;
        File desc = new File(downloadPath);
        if (!desc.getParentFile().exists())
        {
            desc.getParentFile().mkdirs();
        }
        return downloadPath;
    }
}