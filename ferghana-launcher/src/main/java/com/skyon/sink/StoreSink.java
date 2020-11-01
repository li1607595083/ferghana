package com.skyon.sink;

import com.skyon.type.TypeTrans;
import com.skyon.utils.KafkaUtils;
import com.skyon.utils.StoreUtils;
import kafka.utils.ZkUtils;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import java.util.Properties;

public class StoreSink {

    /*The table name of the resulting table*/
    private String rstbname;
    /*Insert the output statement of the result table*/
    private String selesql;
    /*Result table creation statement*/
    private String resultTable;
    /*parameter configuration*/
    private Properties properties;
    /*table execution environment*/
    private  StreamTableEnvironment dbTableEnv;

    public StoreSink(){}

    public StoreSink(StreamTableEnvironment dbTableEnv,Properties properties){
        this.properties = properties;
        this.dbTableEnv =  dbTableEnv;
    }


    /**
     * The data base(sink)
     * @param dbTableEnv
     * @param properties
     * @throws Exception
     */
    public static void sink(StreamTableEnvironment dbTableEnv, Properties properties) throws Exception {
        StoreSink storeSink = new StoreSink(dbTableEnv, properties);
        storeSink.initPre(properties.getProperty("sinkSql"));
        storeSink.sinkTable(storeSink.getSchemaAndTable());
    }


    /**
     * Initializes the result table prefix
     * @param sinkSql
     */
    private void initPre(String sinkSql){
        String[] sp = sinkSql.trim().substring(0, sinkSql.length() - 1).split("\\(", 2);
        rstbname = sp[0].trim().split("\\s+")[2];
        selesql = sp[1].trim();
        resultTable = "CREATE TABLE " +  rstbname +" (";
        if (properties.getProperty("connectorType").equals("03")){
            resultTable = "CREATE TABLE " +  rstbname.split(":")[1] +" (";
        }
    }

    /**
     * Gets the schema for the table and returns the table
     * @return
     */
    private Table getSchemaAndTable(){
        Table table = dbTableEnv.sqlQuery(selesql);
        TableSchema schema = table.getSchema();
        String[] fieldNames = schema.getFieldNames();
        for (String fieldName : fieldNames) {
            DataType dataType = schema.getFieldDataType(fieldName).get();
            String fieldType = TypeTrans.getType(dataType.toString());
            if (fieldType.contains("ROW")){
                String[] nt = selesql.split("FROM", 2)[0].trim()
                        .split(",", 2)[1].trim()
                        .split("AS", 2);
                String rowType = nt[0].trim();
                rowType = rowType.substring(0, rowType.length() - 1);
                String[] lieName = rowType.replaceFirst("ROW\\(", "").split(",");
                int count = 0;
                for (String s : lieName) {
                    fieldType = fieldType.replace("`EXPR$" + count + "`", s.trim());
                    count++;
                }
            }
            resultTable = resultTable + fieldName + " " + fieldType  + ",";
        }
        return table;
    }

    /**
     * To sink to an external storage system
     * @param table
     * @throws Exception
     */
    private void sinkTable(Table table) throws Exception {
        switch (properties.getProperty("connectorType")){
            case "01":
                ZkUtils zkUtils = KafkaUtils.getZkUtils(properties.getProperty("kafkaZK"));
                KafkaUtils.createKafkaTopic(zkUtils, properties.getProperty("kafkaTopic"));
                KafkaUtils.clostZkUtils(zkUtils);
                resultTable = resultTable.substring(0, resultTable.length() - 1)
                        + ") WITH ("
                        + "'connector' = 'kafka-0.11',"
                        + "'topic' = '" + properties.getProperty("kafkaTopic") + "',"
                        + "'properties.bootstrap.servers' = '" + properties.getProperty("kafkaAddress") + "',"
                        + "'sink.partitioner' = 'round-robin',"
                        + "'format' = 'json')";break;
            case "02":
                resultTable = resultTable + "PRIMARY KEY (" + properties.getProperty("sourcePrimaryKey") + ") NOT ENFORCED"
                        + ") WITH ("
                        + "'connector' = 'jdbc',"
                        + "'url' = '" + properties.getProperty("jdbcURL") + "',"
                        + "'table-name' = '" + rstbname + "',"
                        + "'username' = '" + properties.getProperty("jdbcUserName") + "',"
                        + "'password' = '" + properties.getProperty("jdbcUserPwd") + "',"
                        + "'driver' = '" + properties.getProperty("jdbcDrive") + "'"
                        + ")";
                StoreUtils.of(resultTable).createMySqlTable(); break;
            case "03":
                resultTable = resultTable + " PRIMARY KEY  (" + properties.getProperty("sourcePrimaryKey") + ") NOT ENFORCED"
                        + ") WITH ("
                        + "'connector' = 'hbase-1.4',"
                        + "'table-name' = '" + rstbname + "',"
                        + "'zookeeper.quorum' = '" + properties.getProperty("hbaseZK") + "'"
                        + ")";
                StoreUtils.of(resultTable).createHbaseTABLE(); break;
            case "04":
                resultTable = resultTable + "PRIMARY KEY  (" + properties.getProperty("sourcePrimaryKey") + ") NOT ENFORCED"
                        + ") WITH ("
                        + "'connector' = 'elasticsearch-7',"
                        + "'hosts' = '" + properties.getProperty("esAddress") + "',"
                        + "'index' = '" + rstbname + "'"
                        + ")";
                StoreUtils.of(resultTable).createIndexWithMappings();
        }
        dbTableEnv.executeSql(resultTable);

        if (rstbname.contains(":")){
            table.executeInsert(rstbname.split(":")[1]);
        } else {
            table.executeInsert(rstbname);
        }
        dbTableEnv.toAppendStream(table, Row.class).print();
    }

}
