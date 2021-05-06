package com.skyon.sink;

import com.alibaba.fastjson.JSON;
import com.skyon.app.AppDealOperation;
import com.skyon.type.TypeTrans;
import com.skyon.utils.KafkaUtils;
import com.skyon.utils.StoreUtils;
import kafka.utils.ZkUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.scala.OutputTag;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Collector;
import scala.Int;

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
    private LinkedHashMap<String, String> nt;
    private HashMap<String, String> indexfieldNameAndType;

    public StoreSink(){}

    public StoreSink(StreamTableEnvironment dbTableEnv,Properties properties, HashMap<String, String> indexfieldNameAndType){
        this.properties = properties;
        this.dbTableEnv =  dbTableEnv;
        this.nt = new LinkedHashMap<>();
        this.indexfieldNameAndType = indexfieldNameAndType;
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
            if (fieldType.equals("STRING")){
                fieldType = indexfieldNameAndType.getOrDefault(fieldName, fieldType);
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


    public void sinkRedis(){
            AppDealOperation testOperation = AppDealOperation.of(properties);
            DataStream<String> stringDataStream = testOperation.resultToString(dbTableEnv, selesql, "sinkSql");
            stringDataStream.addSink(new RedisSink());
    }

    /**
     * To sink to an external storage system(Hbase, JDBC，ES)
     * @throws Exception
     */
    public void sinkTable(Boolean sideOut) throws Exception {
        SingleOutputStreamOperator<String> stringDataStream = null;
        if (sideOut){
            AppDealOperation dealOperation = AppDealOperation.of();
            DataStream<String> splitDataStream = dealOperation.resultToString(dbTableEnv, selesql, "sinkSql");
            OutputTag<String> outputTag = new OutputTag<String>("side-output", Types.STRING){};
            stringDataStream = splitDataStream.process(new ProcessFunction<String, String>() {
                @Override
                public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                    out.collect(value);
                    ctx.output(outputTag, value);
                }
            });
            DataStream<String> sideOutput = stringDataStream.getSideOutput(outputTag);
            sideOutput.addSink(KafkaSink.untransaction(properties.getProperty("testTopicName"), properties.getProperty("testBrokeList")));
        } else {
            AppDealOperation dealOperation = AppDealOperation.of();
            stringDataStream = dealOperation.resultToString(dbTableEnv, selesql, "sinkSql");
        }
        switch (properties.getProperty("connectorType")){
                case "01":
                    sinkKafa(stringDataStream);break;
                case "02":
                    String type = "mysql";
                    if (properties.getProperty("jdbcDrive").startsWith("oracle.jdbc")){
                        type = "oracle";
                    }
                    if ("02".equals(properties.getProperty("runMode"))){
                    resultTable = resultTable + "PRIMARY KEY (" + properties.getProperty("sourcePrimaryKey") + ") NOT ENFORCED"
                            + ") WITH ("
                            + "'connector' = 'jdbc',"
                            + "'url' = '" + properties.getProperty("jdbcURL") + "',"
                            + "'table-name' = '" + rstbname + "',"
                            + "'username' = '" + properties.getProperty("jdbcUserName") + "',"
                            + "'password' = '" + properties.getProperty("jdbcUserPwd") + "',"
                            + "'driver' = '" + properties.getProperty("jdbcDrive") + "'"
                            + ")";
                    } else {
                        if ("oracle".equals(type)){
                            resultTable = resultTable + "PRIMARY KEY (" + properties.getProperty("sourcePrimaryKey") + ") NOT ENFORCED"
                                    + ") WITH ("
                                    + "'connector' = 'jdbc',"
                                    + "'url' = '" + properties.getProperty("testOracleDimensionUrl") + "',"
                                    + "'table-name' = '"  + rstbname + "',"
                                    + "'username' = '" + properties.getProperty("testOracleUserName") + "',"
                                    + "'password' = '" + properties.getProperty("testOraclePassWord") + "',"
                                    + "'driver' = '" + properties.getProperty("testOracleDriver") + "'"
                                    + ")";
                        } else {
                            resultTable = resultTable + "PRIMARY KEY (" + properties.getProperty("sourcePrimaryKey") + ") NOT ENFORCED"
                                    + ") WITH ("
                                    + "'connector' = 'jdbc',"
                                    + "'url' = '" + properties.getProperty("testDimensionUrl") + "',"
                                    + "'table-name' = '" + rstbname + "',"
                                    + "'username' = '" + properties.getProperty("testUserName") + "',"
                                    + "'password' = '" + properties.getProperty("testPassWord") + "',"
                                    + "'driver' = '" + properties.getProperty("testDriver") + "'"
                                    + ")";
                        }

                    }
                    StoreUtils.of(resultTable).createSqlTable(type);
                    sinkJdbc(stringDataStream);break;
                case "03":
                    if ("02".equals(properties.getProperty("runMode"))){
                        resultTable = resultTable + " PRIMARY KEY  (" + properties.getProperty("sourcePrimaryKey") + ") NOT ENFORCED"
                                + ") WITH ("
                                + "'connector' = 'hbase-1.4',"
                                + "'table-name' = '" + rstbname + "',"
                                + "'zookeeper.quorum' = '" + properties.getProperty("hbaseZK") + "'"
                                + ")";
                    } else {
                        resultTable =  resultTable + " PRIMARY KEY  (" + properties.getProperty("sourcePrimaryKey") + ") NOT ENFORCED"
                                + ") WITH ("
                                + "'connector' = 'hbase-1.4',"
                                + "'table-name' = '"  + rstbname + "',"
                                + "'zookeeper.quorum' = '" + properties.getProperty("testZK") + "'"
                                + ")";
                    }
                    StoreUtils.of(resultTable).createHbaseTABLE();
                    sinkHbase(stringDataStream);break;
                case "04":
                    String esAddr = "";
                    String esAddress = null;
                    if ("02".equals(properties.getProperty("runMode"))){
                    esAddress = properties.getProperty("esAddress");
                    } else {
                        esAddress = properties.getProperty("testEsAddress");
                    }
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
    }

    private void sinkHbase(DataStream<String> stringDataStream) throws IOException {
        HbaseSink hbaseZK = new HbaseSink(rstbname, properties.getProperty(properties.getProperty("runMode").equals("02") ? "hbaseZK" : "testZK"), nt, Integer.parseInt(properties.getProperty("batchSize", 1+"")));
        stringDataStream.addSink(hbaseZK);
    }

    private void sinkEs(DataStream<String> stringDataStream, String esAddress, String index){
        stringDataStream.addSink(EsSink.insertOrUpdateEs(esAddress, index, Integer.parseInt(properties.getProperty("batchSize", 1+""))).build());
    }

    private void sinkKafa(DataStream<String> stringDataStream){
        ZkUtils zkUtils = null;
        if ("02".equals(properties.getProperty("runMode"))){
            zkUtils = KafkaUtils.getZkUtils(properties.getProperty("kafkaZK"));
        } else {
            zkUtils = KafkaUtils.getZkUtils(properties.getProperty("testZK"));
        }
        // 副本作为配置文件参数
        if ("02".equals(properties.getProperty("runMode"))){
            KafkaUtils.createKafkaTopic(zkUtils, properties.getProperty("kafkaTopic"), Integer.parseInt(properties.getProperty("kafkaProducersPoolSize")), Integer.parseInt(properties.getProperty("topic_replication")));
        } else {
            KafkaUtils.createKafkaTopic(zkUtils, properties.getProperty("kafkaTopic"), Integer.parseInt(properties.getProperty("kafkaProducersPoolSize")), 1);
        }
        KafkaUtils.clostZkUtils(zkUtils);
        if ("02".equals(properties.getProperty("runMode"))){
            if ("TRUE".equals(properties.getProperty("CDC_SYNC"))){
                stringDataStream.addSink(KafkaSink.untransaction(properties.getProperty("kafkaTopic"), properties.getProperty("kafkaAddress")));
            } else {
                stringDataStream.addSink(KafkaSink.transaction(properties.getProperty("kafkaTopic"), properties.getProperty("kafkaAddress"), properties.getProperty("kafkaProducersPoolSize")));
            }
        } else if ("01".equals(properties.getProperty("runMode"))){
            stringDataStream.addSink(KafkaSink.untransaction(properties.getProperty("kafkaTopic"), properties.getProperty("testBrokeList")));
        }

    }

    private void sinkJdbc(DataStream<String> stringDataStream){
        String pk = properties.getProperty("sourcePrimaryKey");
        String jdbcDrive = properties.getProperty("jdbcDrive");
        String columnNames = "(";
        String columnValues = "(";
        String updateValue = "";
        String jdbcType = "mysql";
        String pkType = "";
        Set<Map.Entry<String, String>> entries = nt.entrySet();
        Iterator<Map.Entry<String, String>> iterator = entries.iterator();
        while (iterator.hasNext()){
            Map.Entry<String, String> next = iterator.next();
            String name = next.getKey();
            columnNames = columnNames + name + ",";
            columnValues = columnValues + "?,";
            if (jdbcDrive.startsWith("oracle.jdbc") && !name.equals(pk)){
                updateValue = updateValue + name + "=?,";
                jdbcType = "oracle";
            } else if (jdbcDrive.startsWith("com.mysql")){
                updateValue = updateValue + name + "=?,";
            } else {
                pkType = next.getValue();
            }

        }
        columnNames = columnNames.substring(0, columnNames.length() -1) + ")";
        columnValues = columnValues.substring(0, columnValues.length() - 1) + ")";
        updateValue = updateValue.substring(0, updateValue.length() -1 );
        String insertsql = "";
        if (jdbcDrive.startsWith("com.mysql")){
            insertsql = "INSERT INTO " + rstbname + " " + columnNames + " VALUES " + columnValues + " ON DUPLICATE KEY UPDATE "  + updateValue;
        } else if (jdbcDrive.startsWith("oracle.jdbc")){
            insertsql = "MERGE INTO " + rstbname  +" skyonc USING DUAL "
                    + "ON (skyonc." + pk + " = ?) WHEN NOT MATCHED THEN INSERT " + columnNames + " "
                    + "VALUES " + columnValues + " "
                    + "WHEN MATCHED THEN UPDATE SET "
                    + updateValue;
        }
        stringDataStream.addSink(OracelAndMysqlSink.inserOrUpdateJdbc(properties, insertsql, entries, Integer.parseInt(properties.getProperty("batchSize", 1+"")),pk ,jdbcType, pkType));
    }

}
