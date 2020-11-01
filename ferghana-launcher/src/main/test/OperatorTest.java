
import com.alibaba.fastjson.JSON;
import com.skyon.udf.NullForObject;
import com.skyon.utils.FlinkUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.http.HttpHost;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.*;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;

import java.io.IOException;
import java.sql.*;
import java.util.*;

public class OperatorTest {

    public static String str(String init, HashMap<String, String> fieldHash) {
        Iterator<Map.Entry<String, String>> iterator = fieldHash.entrySet().iterator();
        while (iterator.hasNext()){
            Map.Entry<String, String> next = iterator.next();
            String key = next.getKey();
            String value = next.getValue();
            String str = "\"" + key + "\"" + ":{";
            if (value.equals("STRING")){
                str = str + "\"type\":\"text\",\"index\":\"true\",\"analyzer\":\"ik_max_word\"";
            } else {
                if (value.equals("BIGINT")){
                    value = "INTEGER";
                }
                str = str + "\"type\":" + "\"" + value.toLowerCase() + "\"," + "\"index\":\"false\"";
            }
            init = init + str + "},";
        }
        return init = init.substring(0, init.length() - 1) + "}}";
    }

    /**
     * 创建索引知道你映射
     * @throws IOException
     */
    public static void testCreateIndexWithMappings(String sql) throws IOException {

        HashMap<String, String> fieldHash = new HashMap<>();
        String fiedlAndType = sql.split("WITH", 2)[0]
                .replaceFirst("\\(", "|")
                .split("[|]", 2)[1]
                .trim();
        fiedlAndType = fiedlAndType.substring(0, fiedlAndType.length() - 1)
                .trim();
        for (String kv : fiedlAndType.split(",")) {
            String[] split = kv.trim().split("\\s+");
            if (split.length == 2) {
                fieldHash.put(split[0], split[1]);
            }
        }

        String index = sql.split("WITH", 2)[1]
                .split("index")[1]
                .replaceFirst("=", "")
                .split(",")[0]
                .replaceAll("'", "")
                .replace(")", "")
                .trim();

        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("spark01", 9200, "http")));
        IndicesClient indices = client.indices();
        boolean flag = indices.exists(new GetIndexRequest(index), RequestOptions.DEFAULT);

        if (flag == true){
            String oldMapp = "";
            GetMappingsRequest getMappingsRequest = new GetMappingsRequest();
            GetMappingsResponse mapping = indices.getMapping(getMappingsRequest, RequestOptions.DEFAULT);
            Iterator<Map.Entry<String, Object>> iterator2 = mapping.mappings().get(index).getSourceAsMap().entrySet().iterator();
            while (iterator2.hasNext()){
                Map.Entry<String, Object> next = iterator2.next();
                String key = next.getKey();
                Object value = next.getValue();
                if (key.equals("properties")){
                    oldMapp = value.toString();
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
                String newJson = "{\"properties\":{";
                newJson = str(newJson, fieldHash);
                PutMappingRequest putMappingRequest = new PutMappingRequest(index);
                putMappingRequest.source(newJson,XContentType.JSON);
                indices.putMapping(putMappingRequest, RequestOptions.DEFAULT);
            }

        } else {
            CreateIndexRequest indexRequest = new CreateIndexRequest(index);
            Settings settings = Settings.builder()
                    .put("number_of_shards", 5)
                    .put("number_of_replicas", 1)
                    .build();
            indexRequest.settings(settings);

            String json = "{\"dynamic\":\"true\",\"properties\":{";
            json = str(json, fieldHash);
            indexRequest.mapping(json, XContentType.JSON);
            client.indices().create(indexRequest, RequestOptions.DEFAULT);
        }
        client.close();
    }

    /**
     * MySQL表创建
     * @param sql
     * @throws Exception
     */
    public static void createMySqlTable(String sql) throws Exception{
        HashMap<String, String> fieldHashMap = new HashMap<>();
        fieldHashMap.put("BIGINT", "BIGINT");
        fieldHashMap.put("INT", "INT");
        fieldHashMap.put("FLOAT", "FLOAT");
        fieldHashMap.put("DOUBLE", "DOUBLE");
        fieldHashMap.put("BOOLEAN", "BOOLEAN");
        fieldHashMap.put("DATE", "DATE");
        fieldHashMap.put("TIMESTAMP", "DATETIME");
        fieldHashMap.put("STRING", "VARCHAR(255)");
        String[] split_2 = sql.split("WITH", 2);
        String metate = split_2[1].trim().replaceFirst("\\(", "");
        HashMap<String, String> metaHashMap = new HashMap<>();
        for (String s : metate.substring(0, metate.length() - 1).trim().split(",")) {
            String[] kv_meta = s.replaceAll("'", "").split("=", 2);
            metaHashMap.put(kv_meta[0].trim(), kv_meta[1].trim());
        }
        String mysql_driver = metaHashMap.get("driver");
        String mysql_url = metaHashMap.get("url");
        String mysql_user = metaHashMap.get("username");
        String mysql_password = metaHashMap.get("password");
        String table_name = metaHashMap.get("table-name");
        Class.forName(mysql_driver);
        Connection conn = DriverManager.getConnection(mysql_url, mysql_user, mysql_password);
        System.out.println(sql);
        String fieldStr = split_2[0].replaceFirst("\\(", "|")
                .split("[|]", 2)[1].trim();
        fieldStr = fieldStr.substring(0, fieldStr.length() - 1);
        HashMap<String, String> nameAndType = new HashMap<>();
        String pk = null;
        for (String s : fieldStr.trim().split(",")) {
            String[] split = s.split("\\s+");
            if (split.length == 2){
                nameAndType.put(split[0].trim(), split[1].trim());
            } else {
                if (split[0].equals("PRIMARY") && split.length == 3){
                    pk = split[0] + " " + split[1] + " " + split[2] + ",";
                }
            }
        }
        ResultSet ifExists = conn.getMetaData().getTables(null, null, table_name, null);
        if (ifExists.next()){
            PreparedStatement preparedStatement = conn.prepareStatement("SELECT * FROM " + table_name +" LIMIT 1");
            ResultSetMetaData metaData = preparedStatement.getMetaData();
            int columnCount = metaData.getColumnCount();
            for (int i = 1; i <= columnCount; i++){
                String name = metaData.getColumnName(i);
                String type = metaData.getColumnTypeName(i);
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
                    conn.prepareStatement("ALTER TABLE " + table_name + " ADD COLUMN " + key + " " + value).execute();
                }
            }

        } else {
            String fieldNameAndType = "";
            String sqlCreateTable = "CREATE TABLE IF NOT EXISTS ";
            Iterator<Map.Entry<String, String>> iterator = nameAndType.entrySet().iterator();
            while (iterator.hasNext()){
                Map.Entry<String, String> next = iterator.next();
                String name = next.getKey();
                String type = next.getValue();
                if (type.contains("TIMESTAMP")){
                    type = type.replaceFirst("TIMESTAMP", fieldHashMap.get("TIMESTAMP"));
                } else {
                    type = fieldHashMap.get(type);
                }
                fieldNameAndType = fieldNameAndType + "`" + name + "`" + " " + type + ",";
            }
            fieldNameAndType = fieldNameAndType + pk;
            Statement statement = conn.createStatement();
            sqlCreateTable = sqlCreateTable + " " + "`" + table_name  + "`" + "(" + fieldNameAndType + ")";
            System.out.println(sqlCreateTable);
            statement.execute(sqlCreateTable);
            if (statement != null){
                statement.close();
            }
            if (conn != null){
                conn.close();
            }
        }
    }


    public static void createHbaseTABLE(String sql) throws IOException {
        String[] split = sql.split("WITH", 2);
        String serverMessage = split[1].trim();
        serverMessage = serverMessage.substring(1, serverMessage.length() - 1);
        HashMap<String, String> serverHashMap = new HashMap<>();
        for (String s : serverMessage.split("','")) {
            String[] kv = s.split("=");
            String key = kv[0].replaceAll("'", "").trim();
            String values = kv[1].replaceAll("'", "").trim();
            serverHashMap.put(key, values);
        }
        Configuration hbaseConfig = HBaseConfiguration.create();
        hbaseConfig.set("hbase.zookeeper.quorum", serverHashMap.get("zookeeper.quorum"));
        org.apache.hadoop.hbase.client.Connection conn = ConnectionFactory.createConnection(hbaseConfig);
        Admin admin = conn.getAdmin();
        String nameSpacheAndTableName = serverHashMap.get("table-name");
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
        TableName tableName = TableName.valueOf(serverHashMap.get("table-name"));
        String schemaMessage = split[0].trim();
        String[] schemaSpl = schemaMessage.split("\\(", 2);
        ArrayList<String> familyName = new ArrayList<>();
        for (String s : schemaSpl[1].split(",", 2)) {
            if (s.trim().contains("ROW")){
                s = s.substring(0, s.length() - 1);
                String[] arrFam = s.split(",");
                for (String s1 : arrFam) {
                    if (s1.contains("ROW")){
                        familyName.add(s1.split("\\s+")[0].trim());
                    }
                }
            }
        }
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
        for (String s : familyName) {
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(s);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }
        if (!admin.tableExists(tableName)){
            admin.createTable(hTableDescriptor);
        }
        if (admin != null){
            admin.close();
        }
        if (conn != null){
            conn.close();
        }
    }

    @Test
    public void createJson(){
        HashMap<String, Object> stringStringHashMap = new HashMap<>();
        stringStringHashMap.put("kafkaZK", "master:2181");
        stringStringHashMap.put("sourceTableSql", "CREATE TABLE order_topic (produce_id STRING,number INT,order_time TIMESTAMP(3),proctime AS PROCTIME(),WATERMARK FOR order_time AS order_time - INTERVAL '0' SECOND) WITH ('connector' = 'kafka-0.11','topic' = 'order_topic','properties.bootstrap.servers' = 'spark01:9092,spark02:9092,spark03:9092','properties.group.id' = 'ts1','format' = 'json','scan.startup.mode' = 'latest-offset')");
//        stringStringHashMap.put("dimensionTableSql", "CREATE TABLE products (produce_id STRING,price DOUBLE,PRIMARY KEY (produce_id) NOT ENFORCED) WITH ('connector' = 'jdbc','url' = 'jdbc:mysql://spark02:3306/test','table-name' =  'producets','username' = 'root','password' = '147268Tr', 'driver' = 'com.mysql.cj.jdbc.Driver')");
        stringStringHashMap.put("dimensionTableSql", "CREATE TABLE products (produce_id STRING, INFO ROW<price DOUBLE>, PRIMARY KEY (produce_id) NOT ENFORCED) WITH ('connector' = 'hbase-1.4','table-name' = 'frqe:mytable','zookeeper.quorum' = 'spark01:2181,spark02:2181,spark03:2181')");
        stringStringHashMap.put("joinSql", "SELECT o.produce_id, o.number, o.order_time, o.proctime, o.number * p.INFO.price  turnover FROM  order_topic AS o LEFT JOIN products FOR SYSTEM_TIME AS OF o.proctime AS p ON o.produce_id = p.produce_id");
        stringStringHashMap.put("runMode", "01");
        stringStringHashMap.put("variablePackEn", "fgge");
        String jsonString = JSON.toJSONString(stringStringHashMap);
        jsonString = jsonString.substring(0, jsonString.length() - 1) + "," + "\"testSourcedata\":[{\"produce_id\":\"000123\",\"number\":\"4\",\"order_time\":\"2020-08-04 08:37:32.581\"},{\"produce_id\":\"000124\",\"number\":\"5\",\"order_time\":\"2020-08-05 12:37:32.581\"}],\"testDimdata\":[{\"rowkey\":\"000123\",\"INFO.price\":\"18000\"},{\"rowkey\":\"000124\",\"INFO.price\":\"28000\"}]}";
        System.out.println(jsonString);
    }

    @Test
    public void  tesffw1f() throws Exception {

        StreamExecutionEnvironment dbEnv = FlinkUtils.dbEnv();

        dbEnv.setParallelism(1);

        StreamTableEnvironment dbTableEnv = FlinkUtils.dbTableEnv(dbEnv);

        dbTableEnv.createTemporarySystemFunction("ifFalseSetNull", new NullForObject());

        dbTableEnv.executeSql("CREATE TABLE trade_info_table(CUST_NO STRING," +
                "TRADE_AMOUNT DOUBLE," +
                "TRADE_ACCOUNT STRING,OTHER_ACCOUNT STRING,TRADE_ID STRING,TRADE_TIME TIMESTAMP,proctime AS PROCTIME(),WATERMARK FOR TRADE_TIME as TRADE_TIME - INTERVAL '10' SECOND) WITH ('connector' = 'kafka-0.11' ,'topic' = 'trade_info_topic','properties.bootstrap.servers' = 'spark01:9092,spark02:9092,spark03:9092','properties.group.id' = 'ts1','scan.startup.mode' = 'latest-offset','format' = 'json')");

        Table sqlQuery = dbTableEnv.sqlQuery("SELECT TRADE_ID , sum(TRADE_AMOUNT)  over (  PARTITION BY CUST_NO ORDER BY proctime RANGE BETWEEN INTERVAL '1' MINUTE preceding AND CURRENT ROW)  AS s FROM (SELECT TRADE_ID, IF(TRADE_AMOUNT > 1000, TRADE_AMOUNT, CAST(ifFalseSetNull() AS DOUBLE)) AS TRADE_AMOUNT, CUST_NO, proctime FROM trade_info_table) AS tmp");

        sqlQuery.printSchema();

        DataStream<Row> rowDataStream = dbTableEnv.toAppendStream(sqlQuery, Row.class);

        rowDataStream.print();

        dbEnv.execute();

    }

    @Test
    public void jiami(){
        String str = "{\"sourceTableSql\":\"CREATE TABLE trade_info_table(CUST_NO STRING,TRADE_AMOUNT DOUBLE,TRADE_ACCOUNT STRING,OTHER_ACCOUNT STRING,TRADE_ID STRING,TRADE_TIME TIMESTAMP,proctime AS PROCTIME(),WATERMARK FOR TRADE_TIME as TRADE_TIME - INTERVAL '0' SECOND) WITH ('connector' = 'kafka-0.11' ,'topic' = 'trade_info_topic','properties.bootstrap.servers' = 'master:9092','properties.group.id' = 'ts1','scan.startup.mode' = 'latest-offset','format' = 'json')\",\"sinkSql\":\" insert into mysql_sink_table (select TRADE_ID,paishengceshi1 from test_var_topic)\",\"connectorType\":\"02\",\"deVariableSqls\":\"select TRADE_ID , sum(TRADE_AMOUNT)  over( ORDER BY TRADE_TIME RANGE BETWEEN INTERVAL '10' MINUTE preceding AND CURRENT ROW)  AS tongjichaxun1 FROM trade_info_table;select TRADE_ID , count(TRADE_AMOUNT)  over( ORDER BY TRADE_TIME RANGE BETWEEN INTERVAL '10' MINUTE preceding AND CURRENT ROW)  AS tongjichaxun2 FROM trade_info_table@SELECT TRADE_ID,tongjichaxun1/ tongjichaxun2  as paishengceshi1 FROM 123veerg321@123veerg321@3\",\"kafkaZK\":\"master:2181\",\"jdbcUserPwd\":\"Ferghana@1234\",\"runMode\":\"01\",\"testSourcedata\":[{\"TRADE_ID\":\"1\",\"TRADE_AMOUNT\":\"100\",\"TRADE_TIME\":\"2020-10-18 18:50:15.234\"},{\"TRADE_ID\":\"2\",\"TRADE_AMOUNT\":\"200\",\"TRADE_TIME\":\"2020-10-18 18:50:17.234\"}],\"concurrency\":\"1\",\"fieldOutNum\":2,\"jdbcURL\":\"jdbc:mysql://master:3306/test?characterEncoding=UTF-8\",\"sourcePrimaryKey\":\"TRADE_ID\",\"jdbcUserName\":\"ferghana\",\"jdbcDrive\":\"com.mysql.cj.jdbc.Driver\"}";
        byte[] bytes = str.getBytes();
        //Base64 加密
        String encoded = Base64.getEncoder().encodeToString(bytes);
        System.out.println("Base 64 加密后：" + encoded);

        System.out.println("()".contains(")"));

        System.out.println("freg;rfer;erer;".split(";").length);
    }

}
