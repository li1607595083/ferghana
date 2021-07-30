/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.config;

import org.apache.flink.util.Preconditions;

import com.dtstack.flinkx.oraclelogminer.format.LogParser;
import com.dtstack.flinkx.util.GsonUtil;
import com.dtstack.flinkx.util.MapUtil;
import com.google.gson.internal.LinkedTreeMap;

import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The class of Data transfer task configuration
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class DataTransferConfig extends AbstractConfig {

    JobConfig job;

    public DataTransferConfig(Map<String, Object> map) {
        super(map);
        job = new JobConfig((Map<String, Object>) map.get("job"));
    }

    public JobConfig getJob() {
        return job;
    }

    public void setJob(JobConfig job) {
        this.job = job;
    }

    String monitorUrls;

    public String getMonitorUrls() {
        return monitorUrls;
    }

    public void setMonitorUrls(String monitorUrls) {
        this.monitorUrls = monitorUrls;
    }

    String pluginRoot;

    public String getPluginRoot() {
        return pluginRoot;
    }

    public void setPluginRoot(String pluginRoot) {
        this.pluginRoot = pluginRoot;
    }

    String remotePluginPath;

    public String getRemotePluginPath() {
        return remotePluginPath;
    }

    public void setRemotePluginPath(String remotePluginPath) {
        this.remotePluginPath = remotePluginPath;
    }

    private static void checkConfig(DataTransferConfig config) {
        Preconditions.checkNotNull(config);

        JobConfig jobConfig = config.getJob();
        Preconditions.checkNotNull(jobConfig, "Must specify job element");

        List<ContentConfig> contentConfig = jobConfig.getContent();
        Preconditions.checkNotNull(contentConfig, "Must specify content array");
        Preconditions.checkArgument(contentConfig.size() != 0, "Must specify at least one content element");

        // 暂时只考虑只包含一个Content元素的情况
        ContentConfig content =  contentConfig.get(0);

        // 检查reader配置
        ReaderConfig readerConfig = content.getReader();
        Preconditions.checkNotNull(readerConfig, "Must specify a reader element");
        Preconditions.checkNotNull(readerConfig.getName(), "Must specify reader name");
        ReaderConfig.ParameterConfig readerParameter = readerConfig.getParameter();
        Preconditions.checkNotNull(readerParameter, "Must specify parameter for reader");


        // 检查writer配置
        WriterConfig  writerConfig = content.getWriter();
        Preconditions.checkNotNull(writerConfig, "Must specify a writer element");
        Preconditions.checkNotNull(writerConfig.getName(), "Must specify the writer name");
        WriterConfig.ParameterConfig writerParameter = writerConfig.getParameter();
        Preconditions.checkNotNull(writerParameter, "Must specify parameter for the writer");

    }

    public static DataTransferConfig parse(String json) {
        Map<String,Object> map = GsonUtil.GSON.fromJson(json, GsonUtil.gsonMapTypeToken);
        map = MapUtil.convertToHashMap(map);
        HashMap job =  ((HashMap)map.get("job"));
        LinkedTreeMap reader = ((LinkedTreeMap)((LinkedTreeMap)((ArrayList)job.get("content")).get(0)).get("reader"));
        String schema = reader.get("schema").toString();

        LinkedTreeMap parameter = (LinkedTreeMap)reader.get("parameter");
        String cat = parameter.get("cat").toString();
        String newcat = "";
        if (cat.contains("U")) {
            newcat = newcat + "UPDATE,";
        }
        if (cat.contains("I")) {
            newcat = newcat + "INSERT,";
        }
        if (cat.contains("D")) {
            newcat = newcat + "DELETE,";
        }
        newcat.substring(0,newcat.length() - 2);
        parameter.put("cat",newcat);
        parameter.put("pavingData",true);
//        parameter.put("queryTimeout",3000);
        parameter.put("schema",schema);

        Map log = new LinkedTreeMap();
        log.put("isLogger",false);
        log.put("level","debug");
        log.put("path","");
        log.put("pattern","");

        Map speed = new LinkedTreeMap();
        speed.put("channel",1);

        Map restore = new LinkedTreeMap();
        speed.put("isRestore",true);
        speed.put("isStream",true);

        Map setting = new LinkedTreeMap();
        setting.put("log",log);
        setting.put("speed",speed);
        setting.put("restore",restore);

        job.put("setting",setting);

        DataTransferConfig config = new DataTransferConfig(map);
        checkConfig(config);
        return config;
    }

    public static DataTransferConfig parse(Reader reader) {
        DataTransferConfig config = GsonUtil.GSON.fromJson(reader, DataTransferConfig.class);
        checkConfig(config);
        return config;
    }

}


