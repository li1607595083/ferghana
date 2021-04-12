package com.skyon.project.system.tuil;

import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

// 配置文件读取类
@Component
public class PropertiesUtil {

    private static String propertiesName = "pathConfig.perproties";

    public static String getPro(String name) {
        Properties properties = new Properties();
        try {
            // 使用ClassLoader加载properties配置文件生成对应的输入流
            InputStream in = PropertiesUtil.class.getClassLoader().getResourceAsStream(propertiesName);
            BufferedReader bf = new BufferedReader(new InputStreamReader(in, "UTF-8"));
            properties.load(bf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // 使用properties对象加载输入流
        //获取key对应的value值
        return properties.getProperty(name);
    }

}



