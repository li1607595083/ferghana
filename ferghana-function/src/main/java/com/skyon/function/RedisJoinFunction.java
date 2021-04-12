package com.skyon.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.skyon.domain.TDimensionTable;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

@FunctionHint(output = @DataTypeHint("ROW<name String>"))
public class RedisJoinFunction extends TableFunction<Row> {
    public JedisPool jedisPool;
    public Jedis jedis;
    public TDimensionTable dimensionTable;
    public SqlSession session;


    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);

        String resource = "mybatis/mybatis-config.xml";
        InputStream inputStream = null;
        try {
            inputStream = Resources.getResourceAsStream(resource);
        } catch (IOException e) {
            e.printStackTrace();
        }
        SqlSessionFactory sqlSessionFactory = new SqlSessionFactoryBuilder().build(inputStream);
        // 然后根据 sqlSessionFactory 得到 session
        session = sqlSessionFactory.openSession();

        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxIdle(300);
        config.setMaxTotal(600);
        config.setMaxWaitMillis(10000);
        config.setTestOnBorrow(true);
        jedisPool = new JedisPool(config, "localhost", 6379, 10000);
        jedis = jedisPool.getResource();
    }

    public void eval(String redisJoinId, String dataType, String keyName, String fieldName) {
        try {
            String result = null;
            if (dataType.equals("String")) {
                result = jedis.get(keyName);
            } else if (dataType.equals("Hash")) {
                result = jedis.hget(keyName, fieldName);
            } else {
                System.out.println("不支持此数据类型！");
            }
            JSONObject jsonObject = JSONObject.parseObject(result);
            dimensionTable = session.selectOne("selectTDimensionTableById", Long.valueOf(redisJoinId));
            String sourceTableSchema = schemaTransform(dimensionTable.getSchemaDefine());
            String[] fields = sourceTableSchema.split(",");
            StringBuilder returnFields = new StringBuilder();
            for (String field : fields) {
                returnFields.append(jsonObject.getString(field)).append(",");
            }
            System.out.println(returnFields.substring(0, returnFields.length() - 1));
            collect(Row.of(returnFields.substring(0, returnFields.length() - 1)));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            jedis.close();
        }
    }

    public String schemaTransform(String schemaDefine) {
        schemaDefine = StringUtils.strip(schemaDefine, "[");
        schemaDefine = StringUtils.strip(schemaDefine, "]");

        JSONObject jsonObj = JSON.parseObject(schemaDefine);
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Object> entry : jsonObj.entrySet()) {
            sb.append(entry.getKey()).append(",");
        }
        return sb.substring(0, sb.length() - 1);
    }

    @Override
    public void close() throws Exception {
        super.close();
        jedis.close();
    }
}
