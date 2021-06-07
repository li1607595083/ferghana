package com.skyon.project.system.util;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;

public class HttpUtil {


    //get方式：参数放置在url上
    public static String httpConn(String url) {
        StringBuilder result = new StringBuilder();
        try {
            URL urldemo = new URL(url);
            //打开url的连接
            URLConnection conn = urldemo.openConnection();
            //设置连接属性
            conn.setConnectTimeout(6 * 1000);
            //获得输入流，并封装为字符
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(conn.getInputStream()));//获得网络返回的输入流
            String line;

            while ((line = in.readLine()) != null) {
                result.append(line).append("\n");
            }
            result = new StringBuilder(new String(result.toString().getBytes(), "UTF-8"));
            in.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

        }
        return result.toString();
    }

    //get方式：参数放置在url上
    public static String httpConnLog(String url) {
        StringBuilder result = new StringBuilder();
        try {
            URL urldemo = new URL(url);
            //打开url的连接
            URLConnection conn = urldemo.openConnection();
            //设置连接属性
            conn.setConnectTimeout(6 * 1000);
            //获得输入流，并封装为字符
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(conn.getInputStream()));//获得网络返回的输入流
            String line;

            int i = 1;
            while ((line = in.readLine()) != null) {
                result.append(i + "    ").append(line).append("\n");
                ++i;
            }
            result = new StringBuilder(new String(result.toString().getBytes(), "UTF-8"));
            in.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

        }
        return result.toString();
    }

}
