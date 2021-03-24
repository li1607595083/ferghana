package com.skyon.sink;


import com.alibaba.fastjson.JSON;
import com.skyon.utils.HBaseUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.internal.JdbcBatchingOutputFormat;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class HbaseSink extends RichSinkFunction<String> implements CheckpointedFunction {

    private transient Connection connection;
    private String hbaseZk;
    private String port;
    private String tableName;
    private Table table;
    private String rowkey;
    private ArrayList<ArrayList<String>> hashFamAndField;
    List<Put> puts;
    private Integer bathSize;
    private Integer maxRetries = 3;
    private Logger LOG;

    public HbaseSink() throws IOException {}

    public HbaseSink(String tableName, String addressAndPort, HashMap<String, String> nt, Integer bathSize) throws IOException {
        this.bathSize = bathSize;
        String[] split = addressAndPort.split(":", 2);
        hbaseZk = split[0];
        port = split[1];
        this.tableName = tableName;
        hashFamAndField = new ArrayList<>();
        Set<Map.Entry<String, String>> entries = nt.entrySet();
        for (Map.Entry<String, String> entry : entries) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (value.contains("ROW")){
                ArrayList<String> fieldNameArr = new ArrayList<>();
                if (value.contains("ROW(")){
                    fieldNameArr.addAll(Arrays.asList(value.substring(0, value.length() - 1).replaceFirst("ROW\\(", "").split(",")));
                } else if (value.contains("ROW<")){
                    fieldNameArr.addAll(Arrays.asList(value.substring(0, value.length() - 1).replaceFirst("ROW<", "").split(",")));
                }
                fieldNameArr.add(0, key);
                hashFamAndField.add(fieldNameArr);
            } else {
                rowkey = key;
            }
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //获取一个Hbase连接
        connection = HBaseUtil.getHbaseConnection(hbaseZk, Integer.parseInt(port));
        table = connection.getTable(TableName.valueOf(tableName));
        puts = new ArrayList<>();
        LOG = LoggerFactory.getLogger(HbaseSink.class);
    }

    @Override
    public void invoke(String line, Context context) throws Exception {

        HashMap<String, String> hashMap = JSON.parseObject(line, HashMap.class);
        //设置主键,构造一行数据
        Put put = new Put(Bytes.toBytes(hashMap.get(rowkey)));
        for (ArrayList<String> arr : hashFamAndField) {
            String family = arr.get(0);
            arr.remove(0);
            for (String string : arr) {
                String fieldName = string.trim().split("\\s+", 2)[0];
                String fieldValue = hashMap.get(fieldName);
                if (!"null".equals(fieldValue)){
                    put.addColumn(family.getBytes(), fieldName.getBytes(), fieldValue.getBytes());
                }
            }
        }
        puts.add(put);
        if(puts.size() >= bathSize) {
            writeRocord();
            puts.clear();
        }
    }

    @Override
    public synchronized void close() throws Exception {
        super.close();
        if (table != null){
            table.close();
        }
        if (!connection.isClosed()){
            connection.close();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
       writeRocord();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
    }

    public synchronized void writeRocord() throws IOException {
        for (Integer i = 0; i < maxRetries; i++) {
         try {
             table.put(puts);
             break;
         } catch (Exception e){
             if (i >= maxRetries) {
                 throw new IOException(e);
             }
             LOG.error("Failed to insert data into Hbase = {}", i, e);
             try {
                 if (table != null) {
                     table.close();
                    }
                 } catch (Exception e1){
                 LOG.error("Hbase table close failed.", e1);
                 }

             try {
                 if (connection.isClosed()){
                     connection = HBaseUtil.getHbaseConnection(hbaseZk, Integer.parseInt(port));
                 }
             } catch (Exception e2){
                 LOG.error("Reestablish Hbase connection failed = {}", i, e2);
             }

             if (!connection.isClosed()){
                 try {
                     table = connection.getTable(TableName.valueOf(tableName));
                 } catch (Exception e3){
                     LOG.error(e3.getMessage());
                 }
             }
             try {
                 Thread.sleep(1000 * i);
             } catch (InterruptedException ex) {
                 Thread.currentThread().interrupt();
                 throw new IOException("unable to flush; interrupted while doing another attempt", e);
             }
         }
        }
    }
}
