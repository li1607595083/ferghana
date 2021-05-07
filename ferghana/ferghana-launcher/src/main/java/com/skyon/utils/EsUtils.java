package com.skyon.utils;

import com.alibaba.fastjson.JSON;
import com.skyon.type.TypeTrans;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.*;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

/**
 * @author jiangshine
 * @version 1.0
 * @date 2021/4/6 16:42
 * @Content
 */
public class EsUtils {
    public  RestHighLevelClient client;

    @Before
    public  void  getRestHighLevelClient() throws Exception {
        client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("master", Integer.parseInt("9200"), "http")));
        IndicesClient indices = client.indices();
        boolean flag = indices.exists(new GetIndexRequest("twitter"), RequestOptions.DEFAULT);
    }

    public  RestHighLevelClient  getRestHighLevelClient2(String hosts,String index) throws Exception {
        client = new RestHighLevelClient(
                RestClient.builder(new HttpHost(hosts, Integer.parseInt("9200"), "http")));
        IndicesClient indices = client.indices();
        boolean flag = indices.exists(new GetIndexRequest(index), RequestOptions.DEFAULT);
        return client;
    }

    public void addIndex() throws Exception {
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("user", "kimchy");
        jsonMap.put("postDate", new Date());
        jsonMap.put("message", "trying out Elasticsearch");
        IndexRequest indexRequest = new IndexRequest("posts").id("1").source(jsonMap);
        IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);

    }

    public void getData() throws Exception {
        GetRequest getRequest = new GetRequest("index", "1");
        GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);
    }

    /**
     * Create index and specify Mapping
     *
     * @param flag
     * @param indices
     * @param index
     * @param fieldHash
     * @param client
     * @throws Exception
     */
    private void esOperator(Boolean flag, IndicesClient indices, String index, HashMap<String, String> fieldHash, RestHighLevelClient client) throws Exception {
        if (flag) {
            String oldMapp = "";
            GetMappingsRequest getMappingsRequest = new GetMappingsRequest();
            GetMappingsResponse mapping = indices.getMapping(getMappingsRequest, RequestOptions.DEFAULT);
            Iterator<Map.Entry<String, Object>> iterator2 = mapping.mappings().get(index).getSourceAsMap().entrySet().iterator();
            while (iterator2.hasNext()) {
                Map.Entry<String, Object> next = iterator2.next();
                String key = next.getKey();
                Object value = next.getValue();
                if (key.equals("properties")) {
                    oldMapp = value.toString().trim();
                }
            }
            oldMapp = oldMapp.substring(1, oldMapp.length() - 1);
            String[] split = oldMapp.split("},");
            for (String s : split) {
                String fieldName = s.split("=", 2)[0].trim();
                String values = fieldHash.get(fieldName);
                if (values != null) {
                    fieldHash.remove(fieldName);
                }
            }

            if (fieldHash.size() > 0) {
                String newJson = getEsJson(fieldHash);
                PutMappingRequest putMappingRequest = new PutMappingRequest(index);
                putMappingRequest.source(newJson, XContentType.JSON);
                indices.putMapping(putMappingRequest, RequestOptions.DEFAULT);
            }

        } else {
            CreateIndexRequest indexRequest = new CreateIndexRequest(index);
            Settings settings = Settings.builder()
                    .put("number_of_shards", 5)
                    .put("number_of_replicas", 1)
                    .build();
            indexRequest.settings(settings);
            String json = getEsJson(fieldHash);
            indexRequest.mapping(json, XContentType.JSON);
            client.indices().create(indexRequest, RequestOptions.DEFAULT);
        }

        if (client != null) {
            client.close();
        }
    }

    /**
     * json format for mapping
     *
     * @param fieldHash
     * @return
     */
    private String getEsJson(HashMap<String, String> fieldHash) {
        Iterator<Map.Entry<String, String>> iterator = fieldHash.entrySet().iterator();
        HashMap<String, String> esMap = TypeTrans.typeAsEs();
        String newJson = "{\"properties\":{";
        while (iterator.hasNext()) {
            Map.Entry<String, String> next = iterator.next();
            String key = next.getKey();
            String value = next.getValue();
            String esKey = TypeTrans.getTranKey(value);
            String kv = "\"" + key + "\"" + ":{" + esMap.get(esKey) + "},";
            newJson = newJson + kv;
        }
        return newJson.substring(0, newJson.length() - 1) + "}}";
    }

    /**
     * 创建索引 和 映射
     *
     * @throws Exception
     */
    @Test
    public void createIndex() throws Exception {
        //创建名称为blog2的索
        CreateIndexRequest request = new CreateIndexRequest("twitter");
        String str = " {" +
                " \"properties\": {" +
                "           \"name\": {" +
                "              \"type\": \"text\"," +
                "              \"analyzer\":\"ik_max_word\"," +
                "              \"search_analyzer\":\"ik_smart\"" +
                "           }," +
                "           \"uid\": {" +
                "              \"type\": \"text\"," +
                "              \"analyzer\":\"ik_max_word\"," +
                "              \"search_analyzer\":\"ik_smart\"" +
                "           }," +
                "           \"description\": {" +
                "              \"type\": \"text\"," +
                "              \"analyzer\":\"ik_max_word\"," +
                "              \"search_analyzer\":\"ik_smart\"" +
                "           }," +
                "           \"studymodel\": {" +
                "              \"type\": \"keyword\"" +
                "           }," +
                "           \"price\": {" +
                "              \"type\": \"float\"" +
                "           }" +
                "        }" +
                "}";

        //设置映射 doc type名称
        request.mapping(str, XContentType.JSON);

        CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
        System.out.println( JSON.toJSONString( createIndexResponse ) );

        //释放资源
        client.close();
    }

    /**
     * 删除索引
     */
    //删除索引库
    @Test
    public void testDeleteIndex() throws IOException {
        boolean acknowledged = false;
        try {
            //删除索引请求对象
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("blog2");
            //删除索引 同步
            AcknowledgedResponse deleteIndexResponse = client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
            //删除索引响应结果
            acknowledged = deleteIndexResponse.isAcknowledged();
        } catch (ElasticsearchException exception) {
            if (exception.status() == RestStatus.NOT_FOUND) {//找不到index

            }
        }
        //异步删除
        /*
         ActionListener<AcknowledgedResponse> listener = new ActionListener<AcknowledgedResponse>() {
            @Override //成功返回
            public void onResponse(AcknowledgedResponse acknowledgedResponse) {

            }

            @Override //失败
            public void onFailure(Exception e) {

            }
        };
        client.indices().deleteAsync(deleteIndexRequest, RequestOptions.DEFAULT, listener);
         */
        System.out.println(acknowledged);
    }

    /**
     * 插入文档
     */
    @Test
    public void testAddDocument() throws IOException {
        //准备json数据
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put( "name", "11spring cloud实战" );
        jsonMap.put( "uid", "C00001" );
        jsonMap.put( "description", "11本课程主要从四个章节进行讲解： 1.微服务架构入门 2.spring cloud基础入门 3.实战Spring Boot 4.注册中心eureka。" );
        jsonMap.put( "studymodel", "201001" );
        jsonMap.put( "price", 5.6f );

        //索引请求对象
        IndexRequest indexRequest = new IndexRequest( "twitter", "_doc" ).id( "11" );
        //指定索引文档内容
        indexRequest.source( jsonMap );

        //索引响应对象
        IndexResponse index = client.index( indexRequest,RequestOptions.DEFAULT );

        //获取响应结果
        DocWriteResponse.Result result = index.getResult();
        System.out.println( result );

    }

    /**
     * 查询文档  根据ID查询
     */
    @Test
    public void getDocumentById() throws IOException {
        GetRequest getRequest = new GetRequest( "blog1", "_doc", "11" );

        GetResponse response = client.get( getRequest ,RequestOptions.DEFAULT);

        boolean exists = response.isExists();

        Map<String, Object> sourceAsMap = response.getSourceAsMap();
        System.out.println( sourceAsMap );

    }

    /**
     * 更新文档
     */
    @Test
    public void updateDocument() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest( "blog1", "11" );

        Map<String, Object> map = new HashMap<>();//要注意这里的Map的泛型结构要和 之前插入的数据结构相同
        map.put( "name", "spring cloud实战222" );
        updateRequest.doc( map );

        UpdateResponse update = client.update( updateRequest,RequestOptions.DEFAULT);

        RestStatus status = update.status();

        System.out.println( status );
    }

    /**
     * 搜索管理  查询所有文档
     */
    @Test
    public void testSearchAll() throws IOException {
        SearchRequest searchRequest = new SearchRequest( "blog1" );
        searchRequest.types( "_doc" );
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query( QueryBuilders.matchAllQuery() );
        //source源字段过虑
        searchSourceBuilder.fetchSource( new String[]{"name", "studymodel", "description"}, new String[]{} );
        searchRequest.source( searchSourceBuilder );
        SearchResponse searchResponse = client.search( searchRequest,RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.getTotalHits().value;

        System.out.println( "total=" + totalHits );

        SearchHit[] searchHits = hits.getHits();

        for (SearchHit searchHit : searchHits) {
            String index = searchHit.getIndex();
            System.out.println( "index=" + index );
            String id = searchHit.getId();
            System.out.println( "id=" + id );
            String sourceAsString = searchHit.getSourceAsString();
            System.out.println( sourceAsString );
            Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
            //String articleId = String.valueOf( sourceAsMap.get( "id" ) );
            String title = (String) sourceAsMap.get( "name" );
            String content = (String) sourceAsMap.get( "description" );
            //System.out.println("articleId="+articleId);
            System.out.println( "title=" + title );
            System.out.println( "content=" + content );
        }
    }

    /**
     * 搜索管理 根据ID查询
     */
    @Test
    public void testSearchByID() throws IOException {
        //创建查询，设置索引
        SearchRequest searchRequest = new SearchRequest( "blog1" );
        //设置type
        searchRequest.types( "_doc" );
        //设置查询条件
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        String[] ids = {"11", "12"};
        List<String> stringList = Arrays.asList( ids );
        //searchSourceBuilder.query( QueryBuilders.termsQuery( "_id", stringList ) );
        searchSourceBuilder.query( QueryBuilders.matchQuery("name", "spring"));
        searchSourceBuilder.query( QueryBuilders.matchQuery("studymodel", "201001"));
        //source源字段过虑
        searchSourceBuilder.fetchSource( new String[]{"name", "studymodel", "description"}, new String[]{} );
        searchRequest.source( searchSourceBuilder );

        SearchResponse search = client.search( searchRequest,RequestOptions.DEFAULT);
        SearchHits hits = search.getHits();
        long totalHits = hits.getTotalHits().value;
        System.out.println( "总条数：" + totalHits );

        for (SearchHit searchHit : hits.getHits()) {
            String sourceAsString = searchHit.getSourceAsString();
            System.out.println( sourceAsString );
        }
    }

    /**
     * 搜索管理 根据字段查询，key 字段名，value 字段值
     */
    public Map<String, Object> testSearchByField(Map<String,Object> field,String[] returnfield,String index) throws IOException {
        //创建查询，设置索引
        SearchRequest searchRequest = new SearchRequest( index);
        //设置type
        searchRequest.types( "_doc" );
        //设置查询条件
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        String[] ids = {"11", "12"};
        List<String> stringList = Arrays.asList( ids );
        //searchSourceBuilder.query( QueryBuilders.termsQuery( "_id", stringList ) );
        Set<Map.Entry<String,Object>> entries = field.entrySet();
        for (Map.Entry entry : entries) {
            searchSourceBuilder.query(QueryBuilders.matchQuery(entry.getKey().toString(), entry.getValue().toString()));
        }
        //source源字段过虑
        searchSourceBuilder.fetchSource(returnfield, new String[]{} );
        searchRequest.source( searchSourceBuilder );

        SearchResponse search = client.search( searchRequest,RequestOptions.DEFAULT);
        SearchHits hits = search.getHits();
        long totalHits = hits.getTotalHits().value;
        System.out.println( "总条数：" + totalHits );
        Map<String, Object> sourceAsMap = new HashMap<>();
        for (SearchHit searchHit : hits.getHits()) {
            sourceAsMap = searchHit.getSourceAsMap();
        }
        return sourceAsMap;
    }


    /**
     * 搜索管理  Term Query精确查找 ，在搜索时会整体匹配关键字，不再将关键字分词 ，
     * 下面的语句：查询title 包含 spring 的文档
     */
    @Test
    public void testSearchTerm() throws IOException {
        //创建查询，设置索引
        SearchRequest searchRequest = new SearchRequest( "blog1" );
        //设置type
        searchRequest.types( "_doc" );
        //设置查询条件
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query( QueryBuilders.termQuery( "name", "spring" ) );
        //source源字段过虑
        //searchSourceBuilder.fetchSource(new String[]{"title","id","content"},new String[]{});
        searchSourceBuilder.fetchSource( new String[]{"name", "studymodel", "description"}, new String[]{} );
        searchRequest.source( searchSourceBuilder );

        SearchResponse search = client.search(searchRequest,RequestOptions.DEFAULT);
        SearchHits hits = search.getHits();
        long totalHits = hits.getTotalHits().value;
        System.out.println( "总条数：" + totalHits );

        for (SearchHit searchHit : hits.getHits()) {
            String sourceAsString = searchHit.getSourceAsString();
            System.out.println( sourceAsString );
        }
    }

    /**
     * 搜索管理 同时搜索多个Field
     */
    @Test
    public void testSearchMultiMatch() throws IOException {
        //创建查询，设置索引
        SearchRequest searchRequest = new SearchRequest( "blog1" );
        //设置type
        searchRequest.types( "_doc" );
        //设置查询条件
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //匹配关键字
        MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery( "Boot 开发", "name", "description" )
                .minimumShouldMatch( "50%" );
        multiMatchQueryBuilder.field( "name",10 );//提升boost 表示权重提升10倍

        searchSourceBuilder.query( multiMatchQueryBuilder );
        //source源字段过虑
        searchSourceBuilder.fetchSource( new String[]{"name", "studymodel", "description"}, new String[]{} );
        searchRequest.source( searchSourceBuilder );

        SearchResponse search = client.search(searchRequest,RequestOptions.DEFAULT);
        SearchHits hits = search.getHits();
        long totalHits = hits.getTotalHits().value;
        System.out.println( "总条数：" + totalHits );

        for (SearchHit searchHit : hits.getHits()) {
            String sourceAsString = searchHit.getSourceAsString();
            System.out.println( sourceAsString );
        }
    }


    public static void main(String[] args) throws Exception {
        EsUtils esUtils = new EsUtils();
        esUtils.getRestHighLevelClient();
//        esUtils.createIndex();
        Map<String,Object> field = new HashMap<>();
        field.put("uid","C00001");
        field.put("price",5.6);
        String[] returnfield = {"uid","name"};
        esUtils.testSearchByField(field,returnfield,"twitter");


    }
}
