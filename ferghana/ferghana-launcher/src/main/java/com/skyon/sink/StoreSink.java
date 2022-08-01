package com.skyon.sink;

import com.skyon.bean.*;
import com.skyon.function.FunMapValueMoveTypeAndFieldNmae;
import com.skyon.type.TypeTrans;
import com.skyon.utils.DataStreamToTable;
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
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.NetUtils;

import java.io.IOException;
import java.util.*;

import static com.skyon.app.AppPerFormOperations.getUid;
import static com.skyon.utils.ParameterUtils.removeBeforeFirstSpecialSymbol;
import static com.skyon.utils.ParameterUtils.removeBeforeLastSpecialSymbol;
import static com.skyon.utils.ParameterUtils.removeTailLastSpecialSymbol;

public class StoreSink {

    Table table;
    /*The table name of the resulting table*/
    private String rstbname;
    /*Insert the output statement of the result table*/
    private String selesql;
    /*Result table creation statement*/
    private String resultTable;
    /*parameter configuration*/
    private Properties properties;
    /*table execution environment*/
    private StreamTableEnvironment dbTableEnv;
    private Map<String, String> nt;
    private Map<String, String> indexfieldNameAndType;

    public StoreSink(){}

    public StoreSink(StreamTableEnvironment dbTableEnv,Properties properties, Map<String, String> indexfieldNameAndType){
        this.properties = properties;
        this.dbTableEnv =  dbTableEnv;
        this.nt = new LinkedHashMap<>();
        this.indexfieldNameAndType = indexfieldNameAndType;
        if (RunMode.START_MODE.equals(properties.getProperty(ParameterName.RUM_MODE))){
            initPre(properties.getProperty(ParameterName.SINK_SQL));
            getSchemaAndTable();
        } else {
            selesql = removeBeforeFirstSpecialSymbol(removeTailLastSpecialSymbol(properties.getProperty(ParameterName.SINK_SQL),")", true), "(",true);
            table = dbTableEnv.sqlQuery(selesql);
        }

    }

    /**
     * Initializes the result table prefix
     * @param sinkSql
     */
    private void initPre(String sinkSql){
        String[] sp = removeTailLastSpecialSymbol(sinkSql, ")", true).split("\\(", 2);
        selesql = sp[1].trim();
        String[] sp_2 = selesql.split("\\s+", 2);
        if (!SourceType.MYSQL_CDC.equals(properties.getProperty(ParameterName.SOURCE_TYPE))){
            selesql = sp_2[0] + " " + ParameterValue.PROCTIME + "," + sp_2[1];
            if (SinkType.SINK_HBASE.equals(properties.getProperty(ParameterName.SINK_TYPE))){
                selesql = selesql.replaceAll("from", "FROM");
                String[] split = StringUtils.reverse(selesql).split(" MORF ", 2);
                selesql = StringUtils.reverse(split[1])
                        + ", " + ParameterValue.PROCTIME + " FROM "
                        + StringUtils.reverse(split[0]);
            }
        }
        rstbname = sp[0].trim().split("\\s+")[2];
        resultTable = "CREATE TABLE " +  rstbname +" (";
        if (properties.getProperty(ParameterName.SINK_TYPE).equals(SinkType.SINK_HBASE)){
            resultTable = "CREATE TABLE " +  rstbname.split(":")[1] +" (";
        }
    }

    /**
     * Gets the schema for the table and returns the table
     * @return
     */
    private void getSchemaAndTable(){
        table = dbTableEnv.sqlQuery(selesql);
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
            if (!fieldName.equals(ParameterValue.PROCTIME)){
                resultTable = resultTable + fieldName + " " + fieldType  + ",";
            }
            nt.put(fieldName, fieldType);
        }
        if (selesql.contains("ROW")){
            String s = selesql.split("FROM", 2)[1].trim();
            s = s.substring(1, s.length() - 1);
            selesql = StringUtils.reverse(StringUtils.reverse(s).split("\\)", 2)[1]);
        }
    }

    private String getOutputSql(){
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("INSERT INTO ").append(rstbname).append(" SELECT ");
        for (String s : nt.keySet()) {
            if (!s.equals(ParameterValue.PROCTIME)){
                stringBuilder.append(s).append(",");
            }
        }
        return removeTailLastSpecialSymbol(stringBuilder.toString(), ",", false) + " FROM result_table";
    }

    /**
     * To sink to an external storage system(Hbase, JDBC，ES)
     * @throws Exception
     */
    public void sinkTable() throws Exception {
        if (RunMode.TEST_MODE.equals(properties.getProperty(ParameterName.RUM_MODE))){
            createOutPutTopic();
            testResultOutput();
        } else {
            SingleOutputStreamOperator<String> stringDataStream = resultToString();
            if (properties.getProperty(ParameterName.SINK_TYPE).equals(SinkType.SINK_HBASE) || properties.getProperty(ParameterName.SINK_TYPE).equals(SinkType.SINK_ES)){

            }
            DataStreamToTable.registerTable(dbTableEnv, stringDataStream,"result_table", false, nt, null);
            if (properties.getProperty(ParameterName.SINK_TYPE).equals(SinkType.SINK_KAFKA)) {
                createOutPutTopic();
                getKafkaCreateTable();
            } else if (properties.getProperty(ParameterName.SINK_TYPE).equals(SinkType.SINK_JDBC_MYSQL)
            || properties.getProperty(ParameterName.SINK_TYPE).equals(SinkType.SINK_JDBC_MYSQL)) {
                getMySqlCreateTable();
                boolean ismysql = properties.getProperty(ParameterName.SINK_TYPE).equals(SinkType.SINK_JDBC_MYSQL);
                StoreUtils storeUtils = StoreUtils.of(resultTable);
                Tuple3<HashMap<String, String>, ArrayList<String>, String> tableSchema = storeUtils.getSchema(ismysql);
                storeUtils.createOrUpdateJdbcTable(rstbname.toUpperCase(), Tuple2.of(tableSchema.f0,tableSchema.f2),ismysql, false);
            } else if (properties.getProperty(ParameterName.SINK_TYPE).equals(SinkType.SINK_HBASE)) {
                getHbaseCreateTable();
                StoreUtils.of(resultTable).createOrUpdateHbaseTABLE();
            }  else {
                getEsCreateTable();
                StoreUtils.of(resultTable).createIndexWithMappings();
            }
            dbTableEnv.executeSql(resultTable);
            dbTableEnv.createStatementSet().addInsertSql(getOutputSql()).execute();
        }
    }

    private void getEsCreateTable() {
        String esAddr = "";
        for (String addr : properties.getProperty(ParameterName.ES_ADDRESS).split(",")) {
            esAddr = "http://" + addr  + ";";
        }
        esAddr = esAddr.substring(0, esAddr.length() - 1);
        resultTable = resultTable + "PRIMARY KEY  (" + properties.getProperty(ParameterName.SINK_PRIMARY_KEY) + ") NOT ENFORCED"
                + ") WITH ("
                + "'connector' = 'elasticsearch-7',"
                + "'hosts' = '" + esAddr + "',"
                + "'index' = '" + rstbname + "'"
                + ")";
    }

    public SingleOutputStreamOperator<String> resultToString() {
        return dbTableEnv.toAppendStream(table, Row.class)
                .map(FunMapValueMoveTypeAndFieldNmae.of(table.getSchema().getFieldNames()));
    }

    private void testResultOutput() {
        DataStream<String> testResultDataStream = resultToString();
        testResultDataStream.addSink(KafkaSink.untransaction(properties.getProperty(ParameterName.TEST_TOPIC_NAME), properties.getProperty(ParameterName.TEST_BROKER_LIST)));
    }

    private void getHbaseCreateTable() {
            resultTable = resultTable + " PRIMARY KEY  (" + properties.getProperty(ParameterName.SINK_PRIMARY_KEY) + ") NOT ENFORCED"
                    + ") WITH ("
                    + "'connector' = 'hbase-1.4',"
                    + "'table-name' = '" + rstbname + "',"
                    + "'zookeeper.quorum' = '" + properties.getProperty(ParameterName.HBASE_ZK) + "'"
                    + ")";
    }

    private String getTestOutputCreateTable(){
        return removeTailLastSpecialSymbol(resultTable,",", false)
                + ") WITH ("
                + "'connector' = 'kafka-0.11',"
                + "'topic' = '" + properties.getProperty(ParameterName.TEST_TOPIC_NAME) + "',"
                + "'properties.bootstrap.servers' = '" + properties.getProperty(ParameterName.TEST_BROKER_LIST) + "',"
                + "'properties.group.id' = '" + "gp_" + properties.getProperty(ParameterName.TEST_TOPIC_NAME) + "',"
                + "'format' = 'json'"
                + ")";
    }

    private void getKafkaCreateTable(){
            resultTable = removeTailLastSpecialSymbol(resultTable,",", false)
                    + ") WITH ("
                    + "'connector' = 'kafka-0.11',"
                    + "'topic' = '" + rstbname + "',"
                    + "'properties.bootstrap.servers' = '" + properties.getProperty(ParameterName.KAFKA_ADDRESS) + "',"
                    + "'properties.group.id' = '" + "gp_" + rstbname + "',"
                    + "'format' = 'json'"
                    + ")";
    }

    private void getMySqlCreateTable() {
        resultTable = resultTable + "PRIMARY KEY (" + properties.getProperty(ParameterName.SINK_PRIMARY_KEY) + ") NOT ENFORCED"
                + ") WITH ("
                + "'connector' = 'jdbc',"
                + "'url' = '" + properties.getProperty(ParameterName.JDBC_URL) + "',"
                + "'table-name' = '" + rstbname + "',"
                + "'username' = '" + properties.getProperty(ParameterName.JDBC_USER_NAME) + "',"
                + "'password' = '" + properties.getProperty(ParameterName.JDBC_USER_PWD) + "',"
                + "'driver' = '" + properties.getProperty(ParameterName.JDBC_DRIVER) + "'"
                + ")";
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

    private void createOutPutTopic() {
        ZkUtils zkUtils;
        if (RunMode.START_MODE.equals(properties.getProperty(ParameterName.RUM_MODE))){
            zkUtils = KafkaUtils.getZkUtils(properties.getProperty(ParameterName.KAFKA_ZK));
        } else {
            zkUtils = KafkaUtils.getZkUtils(properties.getProperty(ParameterName.TEST_ZK));
        }
        // 副本作为配置文件参数
        if (RunMode.START_MODE.equals(properties.getProperty(ParameterName.RUM_MODE))){
            KafkaUtils.createKafkaTopic(zkUtils, rstbname, Integer.parseInt(properties.getProperty(ParameterName.KAFKA_PARTITION)), Integer.parseInt(properties.getProperty(ParameterName.TOPIC_REPLICATION, "1")));
        } else {
            KafkaUtils.createKafkaTopic(zkUtils, properties.getProperty(ParameterName.TEST_TOPIC_NAME), 1, 1);
        }
        KafkaUtils.clostZkUtils(zkUtils);
    }

    private void sinkJdbc(DataStream<String> stringDataStream){
        String pk = properties.getProperty(ParameterName.SINK_PRIMARY_KEY);
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
