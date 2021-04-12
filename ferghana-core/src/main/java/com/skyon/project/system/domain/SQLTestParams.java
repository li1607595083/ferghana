package com.skyon.project.system.domain;


import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SQLTestParams {

    // sql
    private String sqlString;
    // 参数值
    private List<Map> values;
    // 数据源表id
    private String sourceTableId;
    // 结果表id
    private String[] resultTables;
    // 作业名
    private String jobName;

    public String getSqlString() {
        return sqlString;
    }

    public void setSqlString(String sqlString) {
        this.sqlString = sqlString;
    }

    public List<Map> getValues() {
        return values;
    }

    public void setValues(List<Map> values) {
        this.values = values;
    }


    public String getSourceTableId() {
        return sourceTableId;
    }

    public void setSourceTableId(String sourceTableId) {
        this.sourceTableId = sourceTableId;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String[] getResultTables() {
        return resultTables;
    }

    public void setResultTables(String[] resultTables) {
        this.resultTables = resultTables;
    }

    @Override
    public String toString() {
        return "SQLTestParams{" +
                "sqlString='" + sqlString + '\'' +
                ", values=" + values +
                ", sourceTableId='" + sourceTableId + '\'' +
                ", resultTables=" + Arrays.toString(resultTables) +
                ", jobName='" + jobName + '\'' +
                '}';
    }
}
