package com.skyon.utils;

import com.skyon.app.AppPerFormOperations;
import com.skyon.function.FunMapJsonForPars;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.types.Row;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @DESCRIPTION:
 * @NAME: TR
 * @DATE: 2021/7/14
 */
public class DataStreamToTable {

    public static   void registerTable(StreamTableEnvironment dbTableEnv, DataStream<String> dataStream, String tableName, Boolean flag, LinkedHashMap<String, String> singleFieldTypeHashMap) {
        TableSchema tableSchema = new TableSchema(singleFieldTypeHashMap).invoke();
        Expression[] expressions = tableSchema.getExpressions();
        String sch = tableSchema.getSch();
        RowTypeInfo rowTypeInfo = tableSchema.getRowTypeInfo();
        SingleOutputStreamOperator<Row> mapSingleOutputStreamOperator = dataStream.map(FunMapJsonForPars.of(singleFieldTypeHashMap)).returns(rowTypeInfo);
        if (!flag){
            dbTableEnv.createTemporaryView(tableName + "_sky", mapSingleOutputStreamOperator, expressions);
            String sqlTrans = "SELECT " + sch  + " FROM " + tableName + "_sky";
            Table table = dbTableEnv.sqlQuery(sqlTrans);
            dbTableEnv.createTemporaryView("`" + tableName + "`", table);
        } else {
            dbTableEnv.createTemporaryView("`" + tableName + "`", mapSingleOutputStreamOperator, expressions);
        }
    }

    static class TableSchema {
        private LinkedHashMap<String, String> singleFieldTypeHashMap;
        private Expression[] expressions;
        private String sch;
        private RowTypeInfo rowTypeInfo;

        public TableSchema(LinkedHashMap<String, String> singleFieldTypeHashMap) {
            this.singleFieldTypeHashMap = singleFieldTypeHashMap;
        }

        public Expression[] getExpressions() {
            return expressions;
        }

        public String getSch() {
            return sch;
        }

        public RowTypeInfo getRowTypeInfo() {
            return rowTypeInfo;
        }

        public TableSchema invoke() {
            expressions = new Expression[singleFieldTypeHashMap.size()];
            Iterator<Map.Entry<String, String>> iterator = singleFieldTypeHashMap.entrySet().iterator();
            int i = 0;
            sch = "";
            TypeInformation[] typeArr = new TypeInformation[singleFieldTypeHashMap.size()];
            String[] nameArr = new String[singleFieldTypeHashMap.size()];
            while (iterator.hasNext()){
                Map.Entry<String, String> next = iterator.next();
                String name = next.getKey();
                String type = next.getValue();
                nameArr[i] = "`" + name + "`";
                typeArr[i] = Types.STRING;
                expressions[i] = $(name);
                sch = sch + "CAST(" + name + " AS " +  type + ") AS " + name + ", ";
                i++;
            }
            sch  = sch.trim();
            sch = sch.substring(0, sch.length() - 1);
            rowTypeInfo = new RowTypeInfo(typeArr, nameArr);
            return this;
        }
    }

}
