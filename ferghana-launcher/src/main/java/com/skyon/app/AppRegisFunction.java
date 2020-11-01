package com.skyon.app;

import com.skyon.bean.TSelfFunction;
import com.skyon.udf.NullForObject;
import com.skyon.utils.MySqlUtils;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class AppRegisFunction {

    private String sql = "select * from t_self_function";

    public static AppRegisFunction of(){
        return new AppRegisFunction();
    }

    public  void registerFunction(StreamTableEnvironment dbTableEnv) throws Exception {
        dbTableEnv.createTemporarySystemFunction("ifFalseSetNull", new NullForObject());
        Connection connection = MySqlUtils.getConnection();
        ResultSet resultSet = MySqlUtils.selectData(connection, sql);
        for (TSelfFunction dateTran : dateTrans(resultSet)) {
            dbTableEnv.executeSql("CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS " + dateTran.getFunction_name() + " AS '" + dateTran.getFunction_package_path() + "' LANGUAGE JAVA");
        }
        MySqlUtils.closeConnection(connection);
    }

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
            tSelfFunction.setCreate_time(resultSet.getDate("create_time"));
            tSelfFunction.setUpdate_time(resultSet.getDate("update_time"));
            tSelfFunctionArrayList.add(tSelfFunction);
        }
        return tSelfFunctionArrayList;
    }



}
