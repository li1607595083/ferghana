package com.skyon.sink;


import com.alibaba.fastjson.JSON;
import com.skyon.utils.HBaseUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

public class HbaseSink extends RichSinkFunction<String> {

    private transient Connection connection;
    private String hbaseZk;
    private String port;
    private String tableName;
    private Table table;
    private HashMap<String, String> nt;
    private String rowkey;
    private ArrayList<String> arrsNameAndType;
    // 用于批量提交
//    private List<Put> puts = new ArrayList<Put>(5000);

    public HbaseSink() throws IOException {}

    public HbaseSink(String tableName, String addressAndPort, HashMap<String, String> nt) throws IOException {
        String[] split = addressAndPort.split(":", 2);
        hbaseZk = split[0];
        port = split[1];
        this.tableName = tableName;
        arrsNameAndType = new ArrayList<>();
        Set<Map.Entry<String, String>> entries = nt.entrySet();
        for (Map.Entry<String, String> entry : entries) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (value.contains("ROW")){
                arrsNameAndType.add(key + "\t" +value);
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

    }

    @Override
    public void invoke(String line, Context context) throws Exception {

        HashMap<String, String> hashMap = JSON.parseObject(line, HashMap.class);
        //设置主键,构造一行数据
        Put put = new Put(Bytes.toBytes(hashMap.get(rowkey)));
        for (String kv : arrsNameAndType) {
            String[] split = kv.split("\t", 2);
            String family = split[0];
            String columns = split[1];
            String[] fieldNameArr = null;
            if (columns.contains("ROW(")){
                fieldNameArr = columns.substring(0, columns.length() - 1).replaceFirst("ROW\\(", "").split(",");
            } else if (columns.contains("ROW<")){
                fieldNameArr = columns.substring(0, columns.length() - 1).replaceFirst("ROW<", "").split(",");
            }
            for (String fieldNameAndType : fieldNameArr) {
                String fieldName = fieldNameAndType.trim().split("\\s+", 2)[0];
                put.addColumn(family.getBytes(), fieldName.getBytes(), hashMap.get(fieldName).getBytes());
            }
        }
        table.put(put);
//        puts.add(put);
//        if(puts.size() == 5000) {
//            Table table = connection.getTable(TableName.valueOf("orderinfo"));
//            //将数据写入到Hbase中
//            table.put(puts);
//            //清空puts
//            puts.clear();
//            table.close();
//        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (table != null){
            table.close();
        }
        if (connection != null){
            connection.close();
        }
    }
}
