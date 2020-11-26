package com.skyon.sink;

import com.skyon.app.AppDealOperation;
import com.skyon.type.TypeTrans;
import com.skyon.utils.KafkaUtils;
import com.skyon.utils.StoreUtils;
import kafka.utils.ZkUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;

import java.io.IOException;
import java.util.*;

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
    private HashMap<String, String> nt;

    public StoreSink(){}

    public StoreSink(StreamTableEnvironment dbTableEnv,Properties properties){
        this.properties = properties;
        this.dbTableEnv =  dbTableEnv;
        this.nt = new HashMap<>();
        initPre(properties.getProperty("sinkSql"));
        getSchemaAndTable();
    }

    /**
     * Initializes the result table prefix
     * @param sinkSql
     */
    private void initPre(String sinkSql){
        String[] sp = sinkSql.trim().substring(0, sinkSql.length() - 1).split("\\(", 2);
        selesql = sp[1].trim();
        if (!"01".equals(properties.getProperty("connectorType"))){
            rstbname = sp[0].trim().split("\\s+")[2];
            resultTable = "CREATE TABLE " +  rstbname +" (";
            if (properties.getProperty("connectorType").equals("03")){
                resultTable = "CREATE TABLE " +  rstbname.split(":")[1] +" (";
            }
        }
    }

    /**
     * Gets the schema for the table and returns the table
     * @return
     */
    private void getSchemaAndTable(){
        Table table = dbTableEnv.sqlQuery(selesql);
        TableSchema schema = table.getSchema();
        String[] fieldNames = schema.getFieldNames();
        String hbaseColumn = "";
        Boolean flag = true;
        for (String fieldName : fieldNames) {
            DataType dataType = schema.getFieldDataType(fieldName).get();
            String fieldType = TypeTrans.getType(dataType.toString());
            if (fieldType.contains("ROW")){
                if (flag){
                    hbaseColumn =  selesql.split("FROM", 2)[0].split(",", 2)[1].trim();
                    flag  = false;
                }
                String[] nt = hbaseColumn.split("AS", 2);
                String rowType = nt[0].trim();
                hbaseColumn = nt[1];
                if (hbaseColumn.contains("ROW")){
                    hbaseColumn = hbaseColumn.split(",", 2)[1];
                }
                rowType = rowType.substring(0, rowType.length() - 1);
                String[] lieName = null;
                if (rowType.contains("ROW(")){
                    lieName = rowType.replaceFirst("ROW\\(", "").split(",");
                } else if (rowType.contains("ROW<")){
                    lieName = rowType.replaceFirst("ROW<", "").split(",");
                }
                int count = 0;
                for (String s : lieName) {
                    fieldType = fieldType.replace("`EXPR$" + count + "`", s.trim());
                    count++;
                }
            }
            resultTable = resultTable + fieldName + " " + fieldType  + ",";
            nt.put(fieldName, fieldType);
        }
        if (selesql.contains("ROW")){
            String s = selesql.split("FROM", 2)[1].trim();
            s = s.substring(1, s.length() - 1);
            selesql = StringUtils.reverse(StringUtils.reverse(s).split("\\)", 2)[1]);
        }
    }

    /**
     * To sink to an external storage system(Hbase, JDBC，ES)
     * @throws Exception
     */
    public void sinkTable() throws Exception {
        AppDealOperation dealOperation = AppDealOperation.of();
        DataStream<String> stringDataStream = dealOperation.sqlQueryAndUnion(dbTableEnv, selesql);
            switch (properties.getProperty("connectorType")){
                case "01":
//                    ZkUtils zkUtils = KafkaUtils.getZkUtils(properties.getProperty("kafkaZK"));
//                    KafkaUtils.createKafkaTopic(zkUtils, properties.getProperty("kafkaTopic"));
//                    KafkaUtils.clostZkUtils(zkUtils);
//                    resultTable = resultTable.substring(0, resultTable.length() - 1)
//                            + ") WITH ("
//                            + "'connector' = 'kafka-0.11',"
//                            + "'topic' = '" + properties.getProperty("kafkaTopic") + "',"
//                            + "'properties.bootstrap.servers' = '" + properties.getProperty("kafkaAddress") + "',"
//                            + "'sink.partitioner' = 'round-robin',"
//                            + "'format' = 'json')";break;
                    sinkKafa(stringDataStream);break;
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
                    StoreUtils.of(resultTable).createMySqlTable();
                    sinkJdbc(stringDataStream);break;
                case "03":
                    resultTable = resultTable + " PRIMARY KEY  (" + properties.getProperty("sourcePrimaryKey") + ") NOT ENFORCED"
                            + ") WITH ("
                            + "'connector' = 'hbase-1.4',"
                            + "'table-name' = '" + rstbname + "',"
                            + "'zookeeper.quorum' = '" + properties.getProperty("hbaseZK") + "'"
                            + ")";
                    StoreUtils.of(resultTable).createHbaseTABLE().close();
                    sinkHbase(stringDataStream);break;
                case "04":
                    String esAddr = "";
                    String esAddress = properties.getProperty("esAddress");
                    for (String addr : esAddress.split(",")) {
                        addr = "http://" + addr;
                        esAddr = esAddr + addr + ";";
                    }
                    esAddr = esAddr.substring(0, esAddr.length() - 1);
                    resultTable = resultTable + "PRIMARY KEY  (" + properties.getProperty("sourcePrimaryKey") + ") NOT ENFORCED"
                            + ") WITH ("
                            + "'connector' = 'elasticsearch-7',"
                            + "'hosts' = '" + esAddr + "',"
                            + "'index' = '" + rstbname + "'"
                            + ")";
                    StoreUtils.of(resultTable).createIndexWithMappings();
                    sinkEs(stringDataStream, esAddress, rstbname);

            }
//            dbTableEnv.executeSql(resultTable);
//            if (rstbname.contains(":")){
//                table.executeInsert(rstbname.split(":")[1]);
//            } else {
//                table.executeInsert(rstbname);
//            }

    }

    private void sinkHbase(DataStream<String> stringDataStream) throws IOException {
        HbaseSink hbaseZK = new HbaseSink(rstbname, properties.getProperty("hbaseZK"), nt);
        stringDataStream.addSink(hbaseZK);
    }

    private void sinkEs(DataStream<String> stringDataStream, String esAddress, String index){
        stringDataStream.addSink(EsSink.insertOrUpdateEs(esAddress, index).build());
    }

    private void sinkKafa(DataStream<String> stringDataStream){
        ZkUtils zkUtils = KafkaUtils.getZkUtils(properties.getProperty("kafkaZK"));
        KafkaUtils.createKafkaTopic(zkUtils, properties.getProperty("kafkaTopic"));
        KafkaUtils.clostZkUtils(zkUtils);
        if ("02".equals(properties.getProperty("runMode"))){
            stringDataStream.addSink(KafkaSink.transaction(properties.getProperty("kafkaTopic"), properties.getProperty("kafkaAddress")));
        } else if ("01".equals(properties.getProperty("runMode"))){
            stringDataStream.addSink(KafkaSink.untransaction(properties.getProperty("kafkaTopic"), properties.getProperty("kafkaAddress")));
        }

    }

    private void sinkJdbc(DataStream<String> stringDataStream){
        String columnNames = "(";
        String columnValues = "(";
        String updateValue = "";
        Set<Map.Entry<String, String>> entries = nt.entrySet();
        Iterator<Map.Entry<String, String>> iterator = entries.iterator();
        while (iterator.hasNext()){
            String name = iterator.next().getKey();
            columnNames = columnNames + name + ",";
            columnValues = columnValues + "?,";
            updateValue = updateValue + name + "=?,";
        }
        columnNames = columnNames.substring(0, columnNames.length() -1) + ")";
        columnValues = columnValues.substring(0, columnValues.length() - 1) + ")";
        updateValue = updateValue.substring(0, updateValue.length() -1 );
        String insertsql = "INSERT INTO " + rstbname + " " + columnNames + " VALUES " + columnValues + " ON DUPLICATE KEY UPDATE "  + updateValue;
        stringDataStream.addSink(MySqlSink.inserOrUpdateMySQL(properties, insertsql, entries));
    }

}
