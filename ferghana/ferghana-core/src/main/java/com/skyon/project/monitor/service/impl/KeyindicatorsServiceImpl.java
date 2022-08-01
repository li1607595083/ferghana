package com.skyon.project.monitor.service.impl;





import com.google.common.collect.Lists;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class KeyindicatorsServiceImpl {
    public List list(){
        List<Map<String,Object>> list = Lists.newArrayList();
        Map<String,Object> map1=new HashMap<>();
        map1.put("orgname","发改委");
        map1.put("dkdr","3");
        map1.put("dkjsr","1");
        list.add(map1);

        Map<String,Object> map2=new HashMap<>();
        map2.put("orgname","发改委");
        map2.put("dkdr","1");
        map2.put("dkjsr","2");
        list.add(map2);
        Map<String,Object> map3=new HashMap<>();
        map3.put("orgname","发改委");
        map3.put("dkdr","1");
        map3.put("dkjsr","2");
        list.add(map3);
        Map<String,Object> map4=new HashMap<>();
        map4.put("orgname","发改委");
        map4.put("dkdr","1");
        map4.put("dkjsr","2");
        list.add(map4);
        Map<String,Object> map5=new HashMap<>();
        map5.put("orgname","发改委");
        map5.put("dkdr","1");
        map5.put("dkjsr","2");
        list.add(map5);
        Map<String,Object> map6=new HashMap<>();
        map6.put("orgname","发改委");
        map6.put("dkdr","1");
        map6.put("dkjsr","2");
        list.add(map6);
        Map<String,Object> map7=new HashMap<>();
        map7.put("orgname","发改委");
        map7.put("dkdr","1");
        map7.put("dkjsr","2");
        list.add(map7);
        Map<String,Object> map8=new HashMap<>();
        map8.put("orgname","发改委");
        map8.put("dkdr","1");
        map8.put("dkjsr","2");
        list.add(map8);
        return  list;
    };


}
