package com.skyon.common.utils.ip;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alibaba.fastjson.JSONObject;
import com.skyon.common.utils.StringUtils;
import com.skyon.common.utils.http.HttpUtils;
import com.skyon.framework.config.RuoYiConfig;

/**
 * 获取地址类
 * 
 *
 */
public class AddressUtils
{
    private static final Logger log = LoggerFactory.getLogger(AddressUtils.class);

    public static final String IP_URL = "http://ip-api.com/json/?lang=zh-CN";

    public static String getRealAddressByIP(String ip)
    {
        String address = "XX XX";
        // 内网不查询
        if (IpUtils.internalIp(ip))
        {
            return "内网IP";
        }
        if (RuoYiConfig.isAddressEnabled())
        {
            String rspStr = HttpUtils.sendPost(IP_URL, "ip=" + ip);
            if (StringUtils.isEmpty(rspStr))
            {
                log.error("获取地理位置异常 {}", ip);
                return address;
            }
            JSONObject obj = JSONObject.parseObject(rspStr);
//            JSONObject data = obj.getObject("data", JSONObject.class);
//            String region = data.getString("region");
//            String city = data.getString("city");
            String region = obj.getString("region");
            String city = obj.getString("city");
            address = region + " " + city;
        }
        return address;
    }
}
