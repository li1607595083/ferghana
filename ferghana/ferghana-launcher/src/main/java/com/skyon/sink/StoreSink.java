package com.skyon.sink;

import com.skyon.app.AppPerFormOperations;
import com.skyon.bean.*;
import com.skyon.type.TypeTrans;
import com.skyon.utils.KafkaUtils;
import com.skyon.utils.StoreUtils;
import kafka.utils.ZkUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.scala.OutputTag;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Collector;

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


    /**
     * To sink to an external storage system(Hbase, JDBC，ES)
     * @throws Exception
     */
    public void sinkTable(Boolean sideOut) throws Exception {
        SingleOutputStreamOperator<String> stringDataStream;
        if (sideOut){
            stringDataStream = resultSideOut();
        } else {
            stringDataStream = AppPerFormOperations.resultToString(dbTableEnv, selesql, ParameterName.SINK_SQL);
        }
        if (properties.getProperty(ParameterName.SINK_TYPE).equals(SinkType.SINK_KAFKA)) {
            sinkKafa(stringDataStream);
        } else if (properties.getProperty(ParameterName.SINK_TYPE).equals(SinkType.SINK_JDBC)) {
            String type = getMySqlCreateTable();
            StoreUtils storeUtils = StoreUtils.of(resultTable);
            Tuple3<HashMap<String, String>, ArrayList<String>, String> tableSchema = storeUtils.getTableSchema(type);
            storeUtils.createOrUpdateJdbcTable(rstbname, Tuple2.of(tableSchema.f0,tableSchema.f2),type);
            sinkJdbc(stringDataStream);
        } else if (properties.getProperty(ParameterName.SINK_TYPE).equals(SinkType.SINK_HBASE)) {
            getHbaseCreateTable();
            StoreUtils storeUtils1 = StoreUtils.of(resultTable);
            storeUtils1.createOrUpdateHbaseTABLE();
            sinkHbase(stringDataStream);
        }  else {
            String esAddress = getEsCreateTable();
            StoreUtils.of(resultTable).createIndexWithMappings();
            sinkEs(stringDataStream, esAddress, rstbname);
            }
    }

    private String getEsCreateTable() {
        String esAddr = "";
        String esAddress;
        if (RunMode.START_MODE.equals(properties.getProperty(ParameterName.RUM_MODE))){
        esAddress = properties.getProperty(ParameterName.ES_ADDRESS);
        } else {
        esAddress = properties.getProperty(ParameterName.TEST_ES_ADDRESS);
        }
        for (String addr : esAddress.split(",")) {
            addr = "http://" + addr;
            esAddr = esAddr + addr + ";";
        }
        esAddr = esAddr.substring(0, esAddr.length() - 1);
        resultTable = resultTable + "PRIMARY KEY  (" + properties.getProperty(ParameterName.SOURCE_PRIMARY_KEY) + ") NOT ENFORCED"
                + ") WITH ("
                + "'connector' = 'elasticsearch-7',"
                + "'hosts' = '" + esAddr + "',"
                + "'index' = '" + rstbname + "'"
                + ")";
        return esAddress;
    }

    private SingleOutputStreamOperator<String> resultSideOut() {
        SingleOutputStreamOperator<String> stringDataStream;
        DataStream<String> splitDataStream = AppPerFormOperations.resultToString(dbTableEnv, selesql, "sinkSql");
        OutputTag<String> outputTag = new OutputTag<String>(ParameterValue.SIDE_OUTPUT, Types.STRING){};
        stringDataStream = splitDataStream.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                out.collect(value);
                ctx.output(outputTag, value);
            }
        });
        DataStream<String> sideOutput = stringDataStream.getSideOutput(outputTag);
        sideOutput.addSink(KafkaSink.untransaction(properties.getProperty(ParameterName.TEST_TOPIC_NAME), properties.getProperty(ParameterName.TEST_BROKER_LIST)));
        return stringDataStream;
    }

    private void getHbaseCreateTable() {
        if ("02".equals(properties.getProperty("runMode"))){
            resultTable = resultTable + " PRIMARY KEY  (" + properties.getProperty(ParameterName.SOURCE_PRIMARY_KEY) + ") NOT ENFORCED"
                    + ") WITH ("
                    + "'connector' = 'hbase-1.4',"
                    + "'table-name' = '" + rstbname + "',"
                    + "'zookeeper.quorum' = '" + properties.getProperty(ParameterName.HBASE_ZK) + "'"
                    + ")";
        } else {
            resultTable =  resultTable + " PRIMARY KEY  (" + properties.getProperty(ParameterName.SOURCE_PRIMARY_KEY) + ") NOT ENFORCED"
                    + ") WITH ("
                    + "'connector' = 'hbase-1.4',"
                    + "'table-name' = '"  + rstbname + "',"
                    + "'zookeeper.quorum' = '" + properties.getProperty(ParameterName.TEST_ZK) + "'"
                    + ")";
        }
    }

    private String getMySqlCreateTable() {
        String type = SinkType.SINK_JDBC_MYSQL;
        if (properties.getProperty(ParameterName.JDBC_DRIVER).startsWith("oracle.jdbc")){
            type = SinkType.SINK_JDBC_ORACLE;
        }
        if (RunMode.START_MODE.equals(properties.getProperty(ParameterName.RUM_MODE))){
            resultTable = resultTable + "PRIMARY KEY (" + properties.getProperty(ParameterName.SOURCE_PRIMARY_KEY) + ") NOT ENFORCED"
                    + ") WITH ("
                    + "'connector' = 'jdbc',"
                    + "'url' = '" + properties.getProperty(ParameterName.JDBC_URL) + "',"
                    + "'table-name' = '" + rstbname + "',"
                    + "'username' = '" + properties.getProperty(ParameterName.JDBC_USER_NAME) + "',"
                    + "'password' = '" + properties.getProperty(ParameterName.JDBC_USER_PWD) + "',"
                    + "'driver' = '" + properties.getProperty(ParameterName.JDBC_DRIVER) + "'"
                    + ")";
        } else {
            if ("oracle".equals(type)){
                resultTable = resultTable + "PRIMARY KEY (" + properties.getProperty(ParameterName.SOURCE_PRIMARY_KEY) + ") NOT ENFORCED"
                        + ") WITH ("
                        + "'connector' = 'jdbc',"
                        + "'url' = '" + properties.getProperty(ParameterName.TEST_ORACLE_DIM_URL) + "',"
                        + "'table-name' = '"  + rstbname + "',"
                        + "'username' = '" + properties.getProperty(ParameterName.TEST_ORACLE_USERNAME) + "',"
                        + "'password' = '" + properties.getProperty(ParameterName.TEST_ORACLE_PASSWORD) + "',"
                        + "'driver' = '" + properties.getProperty(ParameterName.TEST_ORACLE_DRIVER) + "'"
                        + ")";
            } else {
                resultTable = resultTable + "PRIMARY KEY (" + properties.getProperty(ParameterName.SOURCE_PRIMARY_KEY) + ") NOT ENFORCED"
                        + ") WITH ("
                        + "'connector' = 'jdbc',"
                        + "'url' = '" + properties.getProperty(ParameterName.TEST_MYSQL_DIM_URL) + "',"
                        + "'table-name' = '" + rstbname + "',"
                        + "'username' = '" + properties.getProperty(ParameterName.TEST_USER_NAME) + "',"
                        + "'password' = '" + properties.getProperty(ParameterName.TEST_PASSWORD) + "',"
                        + "'driver' = '" + properties.getProperty(ParameterName.TEST_DRIVER) + "'"
                        + ")";
            }
        }
        return type;
    }

    private void sinkHbase(DataStream<String> stringDataStream) throws IOException {
        HbaseSink hbaseZK = new HbaseSink(rstbname, properties.getProperty(properties.getProperty(ParameterName.RUM_MODE).equals(RunMode.START_MODE) ? ParameterName.HBASE_ZK : ParameterName.TEST_ZK), nt, Integer.parseInt(properties.getProperty(ParameterName.BATH_SIZE, 1+"")));
        stringDataStream.addSink(hbaseZK).name("SINK_OUTPUT_RESUTL");
    }

    private void sinkEs(DataStream<String> stringDataStream, String esAddress, String index){
        stringDataStream.addSink(EsSink.insertOrUpdateEs(esAddress, index, Integer.parseInt(properties.getProperty(ParameterName.BATH_SIZE, 1+""))).build())
                .name("SINK_OUTPUT_RESUTL");
    }

    private void sinkKafa(DataStream<String> stringDataStream){
        ZkUtils zkUtils;
        if (RunMode.START_MODE.equals(properties.getProperty(ParameterName.RUM_MODE))){
            zkUtils = KafkaUtils.getZkUtils(properties.getProperty(ParameterName.KAFKA_ZK));
        } else {
            zkUtils = KafkaUtils.getZkUtils(properties.getProperty(ParameterName.TEST_ZK));
        }
        // 副本作为配置文件参数
        if (RunMode.START_MODE.equals(properties.getProperty(ParameterName.RUM_MODE))){
            KafkaUtils.createKafkaTopic(zkUtils, rstbname, Integer.parseInt(properties.getProperty(ParameterName.KAFKA_PARTITION)), Integer.parseInt(properties.getProperty(ParameterName.TOPIC_REPLICATION)));
        } else {
            KafkaUtils.createKafkaTopic(zkUtils, rstbname, Integer.parseInt(properties.getProperty(ParameterName.KAFKA_PARTITION)), 1);
        }
        KafkaUtils.clostZkUtils(zkUtils);
        if (RunMode.START_MODE.equals(properties.getProperty(ParameterName.RUM_MODE))){
            if (SourceType.MYSQL_CDC.equals(properties.getProperty(ParameterName.SOURCE_TYPE))){
                stringDataStream.addSink(KafkaSink.untransaction(rstbname, properties.getProperty(ParameterName.KAFKA_ADDRESS))).name("SINK_OUTPUT_RESUTL");
            } else {
                stringDataStream.addSink(KafkaSink.transaction(rstbname, properties.getProperty(ParameterName.KAFKA_ADDRESS))).name("SINK_OUTPUT_RESUTL");
            }
        } else if (RunMode.TEST_MODE.equals(properties.getProperty(ParameterName.RUM_MODE))){
            stringDataStream.addSink(KafkaSink.untransaction(rstbname, properties.getProperty(ParameterName.TEST_BROKER_LIST))).name("SINK_OUTPUT_RESUTL");
        }

    }

    private void sinkJdbc(DataStream<String> stringDataStream){
        String pk = properties.getProperty(ParameterName.SOURCE_PRIMARY_KEY);
        String jdbcDrive = properties.getProperty(ParameterName.JDBC_DRIVER);
        Set<Map.Entry<String, String>> entries = nt.entrySet();
        Iterator<Map.Entry<String, String>> iterator = entries.iterator();
        String jdbcType = SinkType.SINK_JDBC_MYSQL;
        InsertInfo insertMeta = new InsertInfo(pk, jdbcDrive, iterator, jdbcType).invoke();
        jdbcType = insertMeta.getJdbcType();
        String pkType = insertMeta.getPkType();
        String insertsql = insertMeta.getInsertsql();
        stringDataStream.addSink(OracelAndMysqlSink.inserOrUpdateJdbc(properties, insertsql, entries,pk ,jdbcType, Integer.parseInt(properties.getProperty(ParameterName.BATH_SIZE)), pkType))
        .name("SINK_OUTPUT_RESUTL");
    }

    private class InsertInfo {
        private String pk;
        private String jdbcDrive;
        private Iterator<Map.Entry<String, String>> iterator;
        private String jdbcType;
        private String pkType;
        private String insertsql;

        public InsertInfo(String pk, String jdbcDrive, Iterator<Map.Entry<String, String>> iterator, String jdbcType) {
            this.pk = pk;
            this.jdbcDrive = jdbcDrive;
            this.iterator = iterator;
            this.jdbcType = jdbcType;
        }

        public String getJdbcType() {
            return jdbcType;
        }

        public String getPkType() {
            return pkType;
        }

        public String getInsertsql() {
            return insertsql;
        }

        public InsertInfo invoke() {
            String columnNames = "";
            String columnValues = "";
            String updateValue = "";
            pkType = "";
            while (iterator.hasNext()){
                Map.Entry<String, String> next = iterator.next();
                String name = next.getKey();
                columnNames = columnNames + name + ",";
                columnValues = columnValues + "?,";
                if (jdbcDrive.startsWith("oracle.jdbc") && !name.equals(pk)){
                    updateValue = updateValue + name + "=?,";
                    jdbcType = SinkType.SINK_JDBC_ORACLE;
                } else if (jdbcDrive.startsWith("com.mysql")){
                    updateValue = updateValue + name + "=?,";
                } else {
                    pkType = next.getValue();
                }

            }
            columnNames = "(" +  columnNames.substring(0, columnNames.length() -1) + ")";
            columnValues = "(" + columnValues.substring(0, columnValues.length() - 1) + ")";
            updateValue = updateValue.substring(0, updateValue.length() -1 );
            insertsql = "";
            if (jdbcDrive.startsWith("com.mysql")){
                insertsql = "INSERT INTO " + rstbname + " " + columnNames + " VALUES " + columnValues + " ON DUPLICATE KEY UPDATE "  + updateValue;
            } else if (jdbcDrive.startsWith("oracle.jdbc")){
                insertsql = "MERGE INTO " + rstbname  +" skyonc USING DUAL "
                        + "ON (skyonc." + pk + " = ?) WHEN NOT MATCHED THEN INSERT " + columnNames + " "
                        + "VALUES " + columnValues + " "
                        + "WHEN MATCHED THEN UPDATE SET "
                        + updateValue;
            }
            return this;
        }
    }
}
