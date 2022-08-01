package com.skyon.app;

import com.skyon.bean.ParameterName;
import com.skyon.bean.ParameterValue;
import com.skyon.bean.TSelfFunction;
import com.skyon.udf.*;
import com.skyon.utils.MySqlUtils;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import scala.xml.PrettyPrinter;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * @DESCRIBE 对指定函数进行注册
 */
public class AppRegisFunction {

    /** 从指定表读取自定义函数相关信息,用以注册函数 */
    private static String scanSql = "SELECT * FROM t_self_function";

    /**
     * @desc 注册自定义函数
     * @param dbTableEnv
     * @throws Exception
     */
    public static void registerFunction(StreamTableEnvironment dbTableEnv, Properties parameterProperties) throws Exception {
        // 对项目模块内的自定义函数进行注册
        dbTableEnv.createTemporarySystemFunction(ParameterName.IF_FALSE_SET_NULL, new NullForObject());
        // idea 测试 所用
         dbTableEnv.createTemporarySystemFunction("time_interval", new TimeIntervalFunction());
         dbTableEnv.createTemporarySystemFunction("redis_query", new RedisQueryFunction());
         dbTableEnv.createTemporarySystemFunction("age_function", new AgeFunction());
         dbTableEnv.createTemporarySystemFunction("timestamp_to_string_ymd", new TimestampToStringYMD());
         dbTableEnv.createTemporarySystemFunction("credit_level_down", new CreditLevelDownFunction());
////         获取 MySQL 连接
//        Connection connection = MySqlUtils.getConnection(parameterProperties.getProperty(ParameterName.REGIS_FUN_URL), parameterProperties.getProperty(ParameterName.TEST_USER_NAME), parameterProperties.getProperty(ParameterName.TEST_PASSWORD), parameterProperties.getProperty(ParameterName.TEST_DRIVER));
////         扫描全表数据
//        ResultSet resultSet = MySqlUtils.selectData(connection, scanSql);
////         对非项目模块内的自定义函数进行注册
//        for (TSelfFunction dateTran : dateTrans(resultSet)) {
////             对 Flink 非内置函数进行注册，其中函数类型为 00 的话，表示为 Flink  的内置函数
//            if (!ParameterValue.INTERNAEL_FUN_TYPE.equals(dateTran.getFunction_type()) && (
//                    parameterProperties.getOrDefault(ParameterName.AGG_PARTITION_SQL, "").toString().contains(dateTran.getFunction_name()) ||
//                    parameterProperties.getOrDefault(ParameterName.TRANSITION_SQL, "").toString().contains(dateTran.getFunction_name()) ||
//                    parameterProperties.getOrDefault(ParameterName.AGG_NO_PARTITION_SQL, "").toString().contains(dateTran.getFunction_name()) ||
//                    parameterProperties.getOrDefault(ParameterName.DERIVE_SQL, "").toString().contains(dateTran.getFunction_name())
//            ))
//            dbTableEnv.executeSql("CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS " +  dateTran.getFunction_name() + " AS '" + dateTran.getFunction_package_path() + "' LANGUAGE JAVA");
//        }
////         关闭 MySql 连接
//        MySqlUtils.closeConnection(connection);
    }



    /**
     * @param resultSet
     * @return 将查询的结果，存放在集合里面，并返回整个集合
     * @throws SQLException
     */
    private static List<TSelfFunction> dateTrans(ResultSet resultSet) throws SQLException {
        List<TSelfFunction> tSelfFunctionArrayList = new ArrayList<>();
        while (resultSet.next()){
            // 将每条记录存放在 TSelfFunction 对象中
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
