package com.skyon.sink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.*;

public class EsSink {
    public static  ElasticsearchSink.Builder<String> insertOrUpdateEs(String esAddress, String index, int numMaxActions){
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        for (String s : esAddress.split(",")) {
            String[] split = s.split(":", 2);
            httpHosts.add(new HttpHost(split[0], Integer.parseInt(split[1]), "http"));
        }

        // use a ElasticsearchSink.Builder to create an ElasticsearchSink
        ElasticsearchSink.Builder<String> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<String>() {
                    public IndexRequest createIndexRequest(String element) {
                        if (element.contains("null")){
                            HashMap<String, String> stringStringHashMap = new HashMap<>();
                            Iterator<Map.Entry<String, Object>> iterator = JSON.parseObject(element).entrySet().iterator();
                            while (iterator.hasNext()){
                                Map.Entry<String, Object> next = iterator.next();
                                String key = next.getKey();
                                String value = next.getValue().toString();
                                if (!"null".equals(value)){
                                 stringStringHashMap.put(key, value);
                                }
                            }
                            element = JSON.toJSONString(stringStringHashMap);
                        }
                        return Requests.indexRequest()
                                .index(index)
                                .source(element,XContentType.JSON);
                    }

                    @Override
                    public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );

        // configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
        esSinkBuilder.setBulkFlushMaxActions(numMaxActions);

        // provide a RestClientFactory for custom configuration on the internally created REST client
//        esSinkBuilder.setRestClientFactory(
//                restClientBuilder -> {}
//        );
        return esSinkBuilder;
    }
}
