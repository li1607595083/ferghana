package com.skyon.main;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.operations.Operation;

import java.util.List;

/**
 * @DESCRIPTION:
 * @NAME: TR
 * @DATE: 2021/10/20
 */
public class SqlParse {

    private static EnvironmentSettings environmentSettings;
    private static TableEnvironmentImpl tableEnvironment;
    private static Parser sqlParse;
    static {
        environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        tableEnvironment = TableEnvironmentImpl.create(environmentSettings);
        sqlParse = tableEnvironment.getParser();
    }

    public static void  createTable(String sqlSource){
        tableEnvironment.executeSql(sqlSource);
    }

    public static void createTableView(String querSql, String tableName,String... sqlSource){
        for (String s : sqlSource) {
            tableEnvironment.executeSql(s);
        }
        tableEnvironment.createTemporaryView(tableName, tableEnvironment.sqlQuery(querSql));
    }

    public static Parser getSqlParse() {
        return sqlParse;
    }

    public static void main(String[] args) throws InterruptedException {

        String s = "CREATE TABLE `skyon_user__behavior_info`(`COUNT` STRING,`productId` STRING,`productType` STRING,`productAmount` DOUBLE,`clientSide` STRING,`provinces` STRING,`advertDra` BOOLEAN,`draOfSource` STRING,`behaviorTime` TIMESTAMP,proctime AS PROCTIME(),WATERMARK FOR `behaviorTime` as behaviorTime - INTERVAL '5' SECOND) WITH ('connector' = 'kafka-0.11' ,'topic' = 'skyon_user__behavior_info','properties.bootstrap.servers' = '192.168.4.95:9092,192.168.4.96:9092','properties.group.id' = 'sk001','scan.startup.mode' = 'latest-offset','format' = 'json')";

        createTable(s);
        Parser sqlParse = getSqlParse();

        List<Operation> ege = sqlParse.parse("SELECT cc FROM skyon_user__behavior_info");


        ege.stream().forEach(x -> System.out.println(x.asSummaryString()));


    }
}
