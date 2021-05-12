package com.skyon.function;

import com.skyon.utils.EsUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.elasticsearch.client.RestHighLevelClient;

import java.sql.Timestamp;
import java.util.*;

/**
 * @author jiangshine
 * @version 1.0
 * @date 2021/4/6 10:57
 * @Content
 */
public class FunMapESjoin extends RichMapFunction<Row, Row>  {
    public RestHighLevelClient client;
    public String hostIp;
    public String index;
    public Map<Integer, String> searchValue;
    public EsUtils es ;
    public String[] returnfield;
    public int k;

    public FunMapESjoin() {

    }

    public FunMapESjoin(Map<String,String> host, Map<Integer, String> searchValue, String[] returnfield,int k) {
        this.hostIp = host.get("hosts");
        this.index = host.get("index");
        this.searchValue = searchValue;
        this.returnfield = returnfield;
        this.k = k;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        es = new EsUtils();
        client = es.getRestHighLevelClient2(hostIp,index);
    }

    @Override
    public Row map(Row value) throws Exception {
        Row row = new Row(value.getArity() + returnfield.length);
        for (int i = 0;i < value.getArity();i++) {
            row.setField(i,value.getField(i));
        }
        //时间处理
        row.setField(k, Timestamp.valueOf(value.getField(k).toString().replace("T"," ")));
        Map<String,Object> values = new HashMap<>();
        for (Map.Entry<Integer,String> entry : searchValue.entrySet()) {
            values.put(entry.getValue(),value.getField(entry.getKey()).toString());
        }
        Map<String, Object> results = es.testSearchByField(values,returnfield,index);
        int i = 0;
        for (Object v : results.values()) {
            row.setField(value.getArity() + i,v);
            i++;
        }
        return row;
    }

    @Override
    public void close() throws Exception {
        super.close();
        client.close();
    }
}
