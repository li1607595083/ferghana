package com.skyon.utils;

import com.skyon.app.AppDealOperation;
import com.skyon.type.TypeTrans;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.http.HttpHost;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.*;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import scala.Tuple2;
import java.io.IOException;
import java.sql.*;
import java.util.*;

public class StoreUtils {
    public ArrayList<String> msVarcherFieldNmae = new ArrayList<String>();
    public String metate;
    public String fieldStr;
    public String jdbcPk;

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
    public   org.apache.hadoop.hbase.client.Connection hbaseConnection() throws IOException {
        HashMap<String, String> serverHashMap = getMeta();
        Configuration hbaseConfig = HBaseConfiguration.create();
        hbaseConfig.set("hbase.zookeeper.quorum", serverHashMap.get("zookeeper.quorum"));
        return ConnectionFactory.createConnection(hbaseConfig);
    }

    /**
     * Check whether the Namespace of Hbase exists, and create it if it does not
     * @param nameSpacheAndTableName
     * @param admin
     * @throws IOException
     */
    private  void hbaseNameSpaceOperator(String nameSpacheAndTableName, Admin admin) throws IOException {
        if (nameSpacheAndTableName.contains(":")){
            String[] nata = nameSpacheAndTableName.split(":");
            String namespace = nata[0];
            boolean flag = true;
            for (NamespaceDescriptor descriptor : admin.listNamespaceDescriptors()) {
                if (descriptor.getName().equals(namespace)){
                    flag = false;
                }
            }
            if (flag){
                NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();
                admin.createNamespace(namespaceDescriptor);
            }
        }
    }

    /**
     * Operate on the Hbaes Table
     * @param tableName
     * @param admin
     * @throws IOException
     */
    private void hbaseTableOperator(TableName tableName, Admin admin, org.apache.hadoop.hbase.client.Connection conn) throws IOException {
        ArrayList<String> familyName = new ArrayList<>();
        for (String s : fieldStr.split(",")) {
            if (s.trim().contains("ROW")){
                familyName.add(s.trim().split("\\s+")[0].trim());
            }
        }
        if (!admin.tableExists(tableName)){
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            for (String s : familyName) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(s);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            admin.createTable(hTableDescriptor);
        } else {
            HTableDescriptor tableDescriptor = admin.getTableDescriptor(tableName);
            for(HColumnDescriptor fdescriptor : tableDescriptor.getColumnFamilies()){
                familyName.remove(fdescriptor.getNameAsString());
            }
            if (familyName.size() > 0){
                admin.disableTable(tableName);
                for (String fam : familyName) {
                    HColumnDescriptor hColumnDescriptor=new HColumnDescriptor(fam);
                    tableDescriptor.addFamily(hColumnDescriptor);
                    admin.modifyTable(tableName, tableDescriptor);
                }
                admin.enableTable(tableName);
            }
        }
        if (admin != null){
            admin.close();
        }
        if (conn != null){
            conn.close();
        }
    }

    /**
     * Hbase table creation
     * @throws IOException
     */
    public void  createHbaseTABLE() throws IOException {
        HashMap<String, String> serverHashMap = getMeta();
        org.apache.hadoop.hbase.client.Connection conn = hbaseConnection();
        Admin admin = conn.getAdmin();
        String nameSpacheAndTableName = serverHashMap.get("table-name");
        hbaseNameSpaceOperator(nameSpacheAndTableName, admin);
        TableName tableName = TableName.valueOf(nameSpacheAndTableName);
        hbaseTableOperator(tableName, admin,conn);
    }


    /**
     *  Gets metadata information about the connection to MySql
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
     * Get a connection to MySql
     * @return
     * @throws Exception
     */
    public Connection mySqlConnection() throws Exception {
        HashMap<String, String> metaHashMap = getMeta();
        String mysql_url = metaHashMap.get("url");
        String mysql_user = metaHashMap.get("username");
        String mysql_password = metaHashMap.get("password");
        return DriverManager.getConnection(mysql_url, mysql_user, mysql_password);
    }


    /**
     * Get the schema for the table
     * @param fieldStr
     * @param msHash
     * @return
     */
    private Tuple2<HashMap<String, String>, String> sqlSchema(String fieldStr, HashMap<String, String> msHash){
        HashMap<String, String> nameAndType = new HashMap<>();
        String pk = null;
        String dealField = fieldStr;
        while (dealField.length() > 0){
            String[] split = dealField.split("\\s+", 2);
            if (split[0].toUpperCase().equals("PRIMARY")){
                String[] va = split[1].split("\\s+");
                pk = split[0] + " " + va[0] + " " + va[1];
                dealField = "";
                jdbcPk = va[1].replace("(", "").replace(")", "").trim();
            } else {
                String name = split[0].replaceAll("`", "");
                String type = TypeTrans.getType(split[1]);
                String msKey = TypeTrans.getTranKey(type);
                String msTy = msHash.get(msKey);
                if ("VARCHAR(255)".equals(msTy.toUpperCase()) || "DATETIME".equals(msTy.toUpperCase()) || "DATE".equals(msTy.toUpperCase()) || "TIMESTAMP".equals(msTy.toUpperCase())){
                    msVarcherFieldNmae.add(name);
                }
                nameAndType.put(name, type.replaceFirst(msKey, msTy));
                dealField = dealField.split(name, 2)[1].trim().substring(type.length()).split(",", 2)[1].trim();
            }
        }
        return Tuple2.apply(nameAndType, pk);
    }

    /**
     * Create or update MySql table
     * @param table_name
     * @param mySqlSchema
     * @throws Exception
     */
    private void mySqlTableOperator(String table_name, Tuple2<HashMap<String, String>, String> mySqlSchema, String jdbcType) throws Exception {
        Connection conn = mySqlConnection();
        ResultSet ifExists = conn.getMetaData().getTables(null, null, table_name, null);
        HashMap<String, String> nameAndType = mySqlSchema._1;
        String pk = mySqlSchema._2;
        if (ifExists.next()){
            String sql_query = "";
            if ("oracle".equals(jdbcType.toLowerCase())){
                sql_query = "SELECT * FROM "+ table_name + " WHERE rownum=1";
            } else if ("mysql".equals(jdbcType.toLowerCase())){
                sql_query = "SELECT * FROM " + table_name + " LIMIT 1";
            }
            ResultSetMetaData metaData = conn.prepareStatement(sql_query).getMetaData();
            int columnCount = metaData.getColumnCount();
            for (int i = 1; i <= columnCount; i++){
                String name = metaData.getColumnName(i);
                if (nameAndType.get(name) != null){
                    nameAndType.remove(name);
                }
            }
            if (!nameAndType.isEmpty()){
                Iterator<Map.Entry<String, String>> iterator = nameAndType.entrySet().iterator();
                while (iterator.hasNext()){
                    Map.Entry<String, String> next = iterator.next();
                    String key = next.getKey();
                    String value = next.getValue();
                    String sql_add = "";
                    if ("oracle".equals(jdbcType.toLowerCase())){
                        sql_add =  "ALTER TABLE " + table_name + " ADD " + "(\"" + key + "\" " + value + ")";
                    } else if ("mysql".equals(jdbcType.toLowerCase())){
                        sql_add = "ALTER TABLE " + table_name + " ADD COLUMN " + "`" + key + "`" + " " + value;
                    }
                    conn.prepareStatement(sql_add).execute();
                }
            }

        } else {
            String fieldNameAndType = "";
            String sqlCreateTable = "CREATE TABLE ";
            Iterator<Map.Entry<String, String>> iterator = nameAndType.entrySet().iterator();
            while (iterator.hasNext()){
                Map.Entry<String, String> next = iterator.next();
                String name = next.getKey();
                String type = next.getValue();
                if ("oracle".equals(jdbcType.toLowerCase())){
                    if (type.equals("TIMESTAMP")){
                        type = "TIMESTAMP(3)";
                    }
                    fieldNameAndType = fieldNameAndType  + "\"" + name + "\"" + " " + type + ",";
                } else if ("mysql".equals(jdbcType.toLowerCase())){
                    if (type.equals("DATETIME")){
                        type = "DATETIME(3)";
                    }
                    fieldNameAndType = fieldNameAndType  + "`" + name + "`" + " " + type + ",";
                }
            }
            fieldNameAndType = fieldNameAndType + pk;
            Statement statement = conn.createStatement();
            sqlCreateTable = sqlCreateTable + " "  + table_name   + "(" + fieldNameAndType + ")";
            statement.execute(sqlCreateTable);
            if (statement != null){
                statement.close();
            }
            if (statement != null){
                conn.close();
            }
        }
    }


    /**
     * MySql table creation
     * @throws Exception
     */
    public void createSqlTable(String jdbcTypes) throws Exception{
        HashMap<String, String> msHash;
        if ("mysql".equals(jdbcTypes.toLowerCase())){
            msHash = TypeTrans.typeAsMySql();
        } else {
            msHash = TypeTrans.typeAsOracle();
        }
        mySqlTableOperator(getMeta().get("table-name"), sqlSchema(fieldStr, msHash), jdbcTypes);
    }

    /**
     * Split the SQL statement to get chemA and meta (connection information)
     * @param sql
     * @return
     */
    private void schemaAndMetate(String sql) {
        String[] scheAndMeta = sql.split("WITH", 2);
        String metateInit = scheAndMeta[1].trim();
        metate = metateInit.substring(1, metateInit.length() - 1).trim();
        String fieldStrInit = scheAndMeta[0].split("\\(", 2)[1].trim();
        fieldStr = fieldStrInit.substring(0, fieldStrInit.length() - 1).trim();
    }

}
