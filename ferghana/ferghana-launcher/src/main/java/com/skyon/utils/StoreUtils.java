package com.skyon.utils;

import com.skyon.bean.DimensionType;
import com.skyon.bean.ParameterConfigName;
import com.skyon.bean.ParameterConfigValue;
import com.skyon.bean.SinkType;
import com.skyon.type.TypeTrans;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.http.HttpHost;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.*;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import java.io.IOException;
import java.sql.*;
import java.util.*;

public class StoreUtils {
    public String metate;
    public String fieldStr;

    public StoreUtils() {}

    public StoreUtils(String sql){
        schemaAndMetate(sql);
    }

    public static StoreUtils of(String sql) {
        return new StoreUtils(sql);
    }

    /**
     * json format for mapping
     * @param fieldHash
     * @return
     */
    private  String getEsJson(HashMap<String, String> fieldHash) {
        Iterator<Map.Entry<String, String>> iterator = fieldHash.entrySet().iterator();
        HashMap<String, String> esMap = TypeTrans.typeAsEs();
        String newJson = "{\"properties\":{";
        while (iterator.hasNext()){
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
     * Create index and specify Mapping
     * @param flag
     * @param indices
     * @param index
     * @param fieldHash
     * @param client
     * @throws Exception
     */
    private  void esOperator(Boolean flag, IndicesClient indices, String index, HashMap<String, String> fieldHash, RestHighLevelClient client) throws Exception {
        if (flag){
            String oldMapp = "";
            GetMappingsRequest getMappingsRequest = new GetMappingsRequest();
            GetMappingsResponse mapping = indices.getMapping(getMappingsRequest, RequestOptions.DEFAULT);
            Iterator<Map.Entry<String, Object>> iterator2 = mapping.mappings().get(index).getSourceAsMap().entrySet().iterator();
            while (iterator2.hasNext()){
                Map.Entry<String, Object> next = iterator2.next();
                String key = next.getKey();
                Object value = next.getValue();
                if (key.equals("properties")){
                    oldMapp = value.toString().trim();
                }
            }
            oldMapp = oldMapp.substring(1, oldMapp.length() - 1);
            String[] split = oldMapp.split("},");
            for (String s : split) {
                String fieldName = s.split("=", 2)[0].trim();
                String values = fieldHash.get(fieldName);
                if (values  != null){
                    fieldHash.remove(fieldName);
                }
            }

            if (fieldHash.size()  > 0){
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

        if (client != null){
            client.close();
        }
    }

    /**
     * Create indexes
     * @throws IOException
     */
    public  void createIndexWithMappings() throws Exception {
        HashMap<String, String> meta = getMeta();
        String esAddress = meta.get("hosts");
        String[] hostAndPort = esAddress.replaceAll("http://", "").split(";")[0].split(":");
        HashMap<String, String> fieldHash = new HashMap<>();
        for (String kv : fieldStr.split(",")) {
            String[] split = kv.trim().split("\\s+");
            if (split.length == 2) {
                fieldHash.put(split[0], split[1]);
            }
        }
        String index = meta.get("index");
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost(hostAndPort[0], Integer.parseInt(hostAndPort[1]), "http")));
        IndicesClient indices = client.indices();
        boolean flag = indices.exists(new GetIndexRequest(index), RequestOptions.DEFAULT);
        esOperator(flag, indices, index, fieldHash, client);
        client.close();
    }

    /**
     * Get the connection to hbase
     * @return
     * @throws IOException
     */
    public   org.apache.hadoop.hbase.client.Connection hbaseConnection(HashMap<String, String> meta) throws Exception {
        return HBaseUtil.getHbaseConnection(meta.get(ParameterConfigName.ZK_QUORUM), ParameterConfigValue.ZK_CLIENT_PORT);

    }

    /**
     * @desc 创建或者更新 hbase 表
     * @throws IOException
     */
    public void  createOrUpdateHbaseTABLE() throws Exception {
        HashMap<String, String> serverHashMap = getMeta();
        org.apache.hadoop.hbase.client.Connection conn = hbaseConnection(serverHashMap);
        Admin admin = HBaseUtil.getHbaseAdmin(conn);
        String nameSpacheAndTableName = serverHashMap.get(ParameterConfigName.TABLE_NAME);
        HBaseUtil.createNamespace(nameSpacheAndTableName.split(":",2)[1], admin);
        List<String> hbaseFamily = getHbaseFamily(fieldStr);
        TableName tableName = TableName.valueOf(nameSpacheAndTableName);
        if (!admin.tableExists(tableName)){
            HBaseUtil.createTable(admin, tableName, hbaseFamily);
        } else {
            HBaseUtil.updateTable(admin,tableName, hbaseFamily);
        }
        HBaseUtil.closeAdmin(admin);
        HBaseUtil.closeConn(conn);
    }

    private List<String> getHbaseFamily(String fieldStr){
        ArrayList<String> familyName = new ArrayList<>();
        for (String s : fieldStr.split(",")) {
            if (s.trim().contains("ROW(") || s.trim().contains("ROW<")){
                familyName.add(s.trim().split("\\s+")[0].trim());
            }
        }
        return familyName;
    }


    /**
     * @desc 获取创建表的连接信息
     * @return
     */
    public HashMap<String, String> getMeta() {
        HashMap<String, String> metaHashMap = new HashMap<>();
        for (String s : metate.split("','")) {
            String[] kv_meta = s.replaceAll("'", "").split("=", 2);
            metaHashMap.put(kv_meta[0].trim(), kv_meta[1].trim());
        }
        return metaHashMap;
    }

    /**
     * @desc 创建 jdbc 连接
     * @return
     * @throws Exception
     */
    public Connection jdbcConnection() throws Exception {
        HashMap<String, String> metaHashMap = getMeta();
        String url = metaHashMap.get(ParameterConfigName.TABLE_URL);
        String user = metaHashMap.get(ParameterConfigName.TABLE_USERNAME);
        String password = metaHashMap.get(ParameterConfigName.TABLE_PASSWORD);
        String driver = metaHashMap.get(ParameterConfigName.TABLE_DRIVER);
        Class.forName(driver);
        return DriverManager.getConnection(url, user, password);
    }


    /**
     * @desc 获取创建表的 schema 信息
     * @param fieldStr
     * @param msHash
     * @return
     */
    private Tuple3<HashMap<String, String>, ArrayList<String>, String> sqlSchema(String fieldStr, Map<String, String> msHash){
        Tuple3<HashMap<String,String>, ArrayList<String>, String> tp4 = Tuple3.of(null, null ,null);
        HashMap<String, String> nameAndType = new HashMap<>();
        ArrayList<String> msVarcherFieldNmae = new ArrayList<>();
        String dealField = fieldStr;
        while (dealField.length() > 0){
            String[] split = dealField.split("\\s+", 2);
            if (split[0].toUpperCase().equals("PRIMARY")){
                String[] va = split[1].split("\\s+");
                tp4.f2 = va[1].replace("(", "").replace(")", "").trim().replaceAll("`", "");
                dealField = "";
            } else {
                // 获取字段名字
                String name = split[0].replaceAll("`", "");
                // 获取当前字段的Flink Table 类型
                String type = TypeTrans.getType(split[1]);
                // 对 Flink 类型进一步处理，去掉括号这些特殊符号
                String msKey = TypeTrans.getTranKey(type);
                // 获取 Flink 类型对应的数据库类型
                String msTy = msHash.get(msKey);
                // 特殊类型做记录，再插入数据的时候会用到
                if ("VARCHAR2(255)".equals(msTy.toUpperCase()) || "VARCHAR(255)".equals(msTy.toUpperCase()) || "DATETIME".equals(msTy.toUpperCase()) || "DATE".equals(msTy.toUpperCase()) || "TIMESTAMP".equals(msTy.toUpperCase())){
                    msVarcherFieldNmae.add(name);
                }
                nameAndType.put(name, type.replaceFirst(msKey, msTy));

                dealField = dealField.split(name, 2)[1].trim().substring(type.length()).split(",", 2)[1].trim();
            }
        }
        tp4.f0 = nameAndType;
        tp4.f1 = msVarcherFieldNmae;
        return tp4;
    }

    /**
     * @desc 创建或更新表
     * @param table_name
     * @param mySqlSchema
     * @throws Exception
     */
    public void createOrUpdateJdbcTable(String table_name, Tuple2<HashMap<String, String>, String> mySqlSchema, boolean isMysql, boolean test) throws Exception {
        boolean isExist = false;
        Connection conn = jdbcConnection();
        if (isMysql){
            isExist = conn.getMetaData().getTables(null, null, table_name, null).next();
        } else {
            isExist = conn.prepareStatement("SELECT TABLE_NAME FROM ALL_TABLES WHERE TABLE_NAME = '" + table_name + "'").executeQuery().next();
        }

        if (isExist && test){
            conn.prepareStatement("DROP TABLE " + table_name).execute();
            isExist = false;
        }

        HashMap<String, String> nameAndType = mySqlSchema.f0;
        String primaryKey = mySqlSchema.f1;
        if (isExist){
            getAddField(table_name, isMysql, conn, nameAndType);
            if (!nameAndType.isEmpty()){
                Iterator<Map.Entry<String, String>> iterator = nameAndType.entrySet().iterator();
                while (iterator.hasNext()){
                    updateJdbcTable(table_name, conn, iterator, isMysql);
                }
            }
        } else {
            createJdbcTable(table_name, isMysql, conn, nameAndType, primaryKey);
        }
        MySqlUtils.closeConnection(conn);
    }

    /**
     * @desc 更新表
     * @param table_name
     * @param isMysql
     * @param conn
     * @param nameAndType
     * @param primaryKey
     * @throws SQLException
     */
    private void createJdbcTable(String table_name, boolean isMysql, Connection conn, HashMap<String, String> nameAndType, String primaryKey) throws SQLException {
        StringBuilder stringBuilder = new StringBuilder("CREATE TABLE ").append(table_name).append("(");
        Iterator<Map.Entry<String, String>> iterator = nameAndType.entrySet().iterator();
        boolean isfirst = true;
        while (iterator.hasNext()){
            if (isfirst) {} else  {stringBuilder.append(", ");}
            Map.Entry<String, String> next = iterator.next();
            String name = next.getKey();
            String type = next.getValue();
            if (isMysql){
                if (type.equals("TIMESTAMP")){
                    type = "TIMESTAMP(3)";
                }
            } else if (isMysql){
                if (type.equals("DATETIME")){
                    type = "DATETIME(3)";
                }
            }
            if (isMysql)  stringBuilder.append("`" + name + "`" + " " + type  + " "); else stringBuilder.append( name + " " + type  + " ");
            if (name.equals(primaryKey)) stringBuilder.append("NOT NULL PRIMARY KEY");
            isfirst = false;
        }
        stringBuilder.append(")");
        conn.prepareStatement(stringBuilder.toString()).execute();
    }

    /**
     * @desc 更新表
     * @param table_name
     * @param conn
     * @param iterator
     * @throws SQLException
     */
    private void updateJdbcTable(String table_name, Connection conn, Iterator<Map.Entry<String, String>> iterator, boolean isMysql) throws SQLException {
        Map.Entry<String, String> next = iterator.next();
        String key = next.getKey();
        String value = next.getValue();
        String sql_add =  isMysql ? "ALTER TABLE " + table_name + " ADD " + "`" + key + "`" + " " + value :
                "ALTER TABLE " + table_name + " ADD "  + key  + " " + value;
        conn.prepareStatement(sql_add).execute();
    }

    /**
     * @desc 获取更新表的字段
     * @param table_name
     * @param isMysql
     * @param conn
     * @param nameAndType
     * @throws SQLException
     */
    private void getAddField(String table_name, boolean isMysql, Connection conn, HashMap<String, String> nameAndType) throws SQLException {
        String sql_query = !isMysql
                ? "SELECT * FROM "+ table_name + " WHERE rownum=1" : "SELECT * FROM " + table_name + " LIMIT 1";
        ResultSetMetaData metaData = conn.prepareStatement(sql_query).getMetaData();
        int columnCount = metaData.getColumnCount();
        for (int i = 1; i <= columnCount; i++){
            String name = metaData.getColumnName(i);
            if (nameAndType.get(name) != null){
                nameAndType.remove(name);
            }
        }
    }



    /**
     * @desc 获取表的字段信息，其中 Tuple3.of(字段名和字段类型, 写入数据时需要特殊处理的字段, 主键)
     */
    public  Tuple3<HashMap<String, String>, ArrayList<String>, String> getSchema(Boolean isMysql) {
        return sqlSchema(fieldStr, (isMysql ? TypeTrans.typeAsMySql() : TypeTrans.typeAsOracle()));
    }



    /**
     * @desc 拆分创建表语句，获取连接信息和字段信息
     * @param sql
     * @return
     */
    private void schemaAndMetate(String sql) {
        String[] scheAndMeta = sql.split("WITH", 2);
        String metateInit = ParameterUtils.removeTailLastSpecialSymbol(scheAndMeta[1], ")", true).trim();
        metate = metateInit.substring(1, metateInit.length() - 1).trim();
        String fieldStrInit = scheAndMeta[0].split("\\(", 2)[1].trim();
        fieldStr = fieldStrInit.substring(0, fieldStrInit.length() - 1).trim();
    }

}
