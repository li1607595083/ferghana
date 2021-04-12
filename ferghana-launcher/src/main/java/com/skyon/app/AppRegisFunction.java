package com.skyon.app;

import com.skyon.bean.TSelfFunction;
import com.skyon.udf.*;
import com.skyon.utils.MySqlUtils;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class AppRegisFunction {

    // 从指定表读取自定义函数的信息,用以注册函数
    private String sql = "SELECT * FROM t_self_function";

    /**
     * Returns a registered function instance
     * @return
     */
    public static AppRegisFunction of(){
        return new AppRegisFunction();
    }

    /**
     * Register custom functions
     * @param dbTableEnv
     * @throws Exception
     */
    public  void registerFunction(StreamTableEnvironment dbTableEnv, Properties properties) throws Exception {
        // 对项目模块内的自定义函数进行注册
        dbTableEnv.createTemporarySystemFunction("ifFalseSetNull", new NullForObject());
//        dbTableEnv.createTemporarySystemFunction("time_interval", new TimeIntervalFunction());
//        dbTableEnv.createTemporarySystemFunction("redis_query", new RedisQueryFunction());
//        dbTableEnv.createTemporarySystemFunction("age_function", new AgeFunction());
//        dbTableEnv.createTemporarySystemFunction("timestamp_to_string_ymd", new TimestampToStringYMD());
        Connection connection = MySqlUtils.getConnection(properties.getProperty("regisfunUrl"), properties.getProperty("testUserName"), properties.getProperty("testPassWord"), properties.getProperty("testDriver"));
        ResultSet resultSet = MySqlUtils.selectData(connection, sql);
        //对非项目模块内的自定义函数进行注册
        for (TSelfFunction dateTran : dateTrans(resultSet)) {
            if (!"00".equals(dateTran.getFunction_type()))
            dbTableEnv.executeSql("CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS " +  dateTran.getFunction_name() + " AS '" + dateTran.getFunction_package_path() + "' LANGUAGE JAVA");
        }
        MySqlUtils.closeConnection(connection);
    }

    /**
     * Data is encapsulated in the specified class
     * @param resultSet
     * @return
     * @throws SQLException
     */
    private List<TSelfFunction> dateTrans(ResultSet resultSet) throws SQLException {
        List<TSelfFunction> tSelfFunctionArrayList = new ArrayList<>();
        while (resultSet.next()){
            TSelfFunction tSelfFunction = new TSelfFunction();
            tSelfFunction.setSelf_function_id(resultSet.getLong("self_function_id"));
            tSelfFunction.setSelf_function_name_cn(resultSet.getString("self_function_name_cn"));
            tSelfFunction.setModule_type(resultSet.getString("module_type"));
            tSelfFunction.setFunction_name(resultSet.getString("function_name"));
            tSelfFunction.setFunction_package_path(resultSet.getString("function_package_path"));
            tSelfFunction.setFile_path(resultSet.getString("file_path"));
            tSelfFunction.setFunction_params(resultSet.getString("function_params"));
            tSelfFunction.setSelf_function_desc(resultSet.getString("self_function_desc"));
            tSelfFunction.setInput_param(resultSet.getString("input_param"));
            tSelfFunction.setOutput_param(resultSet.getString("output_param"));
            tSelfFunction.setFunction_type(resultSet.getString("function_type"));
            tSelfFunction.setFile_path_two(resultSet.getString("file_path_two"));
            tSelfFunction.setCreate_time(resultSet.getDate("create_time"));
            tSelfFunction.setUpdate_time(resultSet.getDate("update_time"));
            tSelfFunctionArrayList.add(tSelfFunction);
        }
        return tSelfFunctionArrayList;
    }



}
