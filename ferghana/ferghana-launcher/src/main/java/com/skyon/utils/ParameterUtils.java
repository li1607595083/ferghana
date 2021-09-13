package com.skyon.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.skyon.bean.*;
import javafx.print.PageOrientation;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

/**
 * @DESCRIPTION: 主要是用于处理主应用程序
 */
public class ParameterUtils {

    /**
     * @param arg1 加密参数；
     * @param properties 参数属性
     * @return 解析加密参数, 参数属性；
     */
    public static void parseEncryptionParameter(String arg1, Properties properties) {
        // 对加密(Base64)参数进行解密
        byte[] decoded = Base64.getDecoder().decode(arg1);
        // 直接转换为字符串,转换后为JSON格式
        String meta = new String(decoded);
        JSONObject jsonObject = JSON.parseObject(meta);
        System.out.println(jsonObject.toJSONString());
        // 以迭代器的形式返回
        Set<Map.Entry<String, Object>> encryParameterSet = jsonObject.entrySet();
        // 将解密后参数存放到  Properties 对象中加
        // 替换操作,主要是对于创建表语句的连接信息(eg:"'connector' = 'hbase-1.4', 'table-name' = 'test:ordeproduct')进行格式上的统一
        // 以便后续通过("','")，进行拆分，进而获取连接信息
        encryParameterSet.forEach(kv -> properties.put(kv.getKey(), kv.getValue().toString().replaceAll("'\\s*,\\s*'", "','")));
    }

    /**
     * @param arg2 参数配置文件路径，以及可能会拼接上 SavePoint 的恢复路径，拼接符号（@）
     *             (eg: /instal/flink_1.11.1/conf/parameterConfig@hdfs:///flink/savepoint/jobid/...)
     * @param parameterProperties 参数属性
     * @return 返回参数文件配置路径
     */
    public static String dealconfigFilePath(String arg2 ,Properties  parameterProperties){
        // 通过是否含有@这个拼接符号来确定应用程序是否从 savePoint 进行恢复
        if (arg2.contains("@")){
            String[] configPathAndSavepPath = arg2.split("@", 2);
            arg2 = configPathAndSavepPath[0];
            parameterProperties.put(ParameterName.SAVEPOINT_PATH, configPathAndSavepPath[1]);
        }
        return  arg2;
    }

    /**
     * @param filePath  参数配置文件路径
     * @param properties  参数属性
     * @return 读取配置文件，以集合的形式返回出去
     * @throws IOException
     */
    public static void readerConfigurationParameters(String filePath ,Properties  properties) throws IOException {
        // 使用参数工具类度取配置文件
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(filePath);
        // 获取配置参数，并以集合的形式返回
        Set<Map.Entry<Object, Object>> configParameterSet = parameterTool.getProperties().entrySet();
        // 将读取后参数存放到  Properties 对象中
        configParameterSet.forEach(kv -> properties.put(kv.getKey().toString(), kv.getValue().toString()));
    }

    /**
     * @desc  对 sinkSql 进行处理，调整 sinkSql  的指标顺序
     * @param properties 参数属性
     */
    private static void sinkSqlDeal(Properties properties){
        if (properties.getProperty(ParameterName.SINK_SQL) != null){
            // eg: INSERT INTO index_table1(SELECT name, age, count, id FROM tmp_table);
            // 如果是测试测试模式，需要对参数进行一些修改；
            String sinkSql = properties.getProperty(ParameterName.SINK_SQL);
            StringBuilder stringBuilder = new StringBuilder();
            // 注意，测试模式统一使用使用测试环境;
            String before = removeTailFirstSpecialSymbol(sinkSql, "(", true);
            stringBuilder.append(before);
            // eg: INSERT INTO index_table1(SELECT name, age, count, id FROM tmp_table);
            // 其中 id 是主键，需要进行位置调整，放到首位；
            // 由于前端生成的 sinkSql 主键放在了后面的位置，因此需要进行调整,主键需要在首位;
            String[] fieldAndTable = removeBeforeFirstSpecialSymbol(sinkSql, "(", true)
                    .replaceAll("from", "FROM").split("FROM", 2);
            if (!SourceType.MYSQL_CDC.equals(properties.getProperty(ParameterName.SOURCE_TYPE))){
                // 获取字段，取出主键
                String indexField = removeTailLastSpecialSymbol(fieldAndTable[0].trim().split("\\s+", 2)[1], ",", false);
                // 重新拼接
                stringBuilder
                        .append("(")
                        .append("SELECT ")
                        .append(properties.getProperty(ParameterName.SOURCE_PRIMARY_KEY))
                        .append(", ")
                        .append(indexField).append(" FROM ")
                        .append(fieldAndTable[1]);
            } else {
                stringBuilder
                        .append("(")
                        .append(fieldAndTable[0])
                        .append(", ")
                        .append(ParameterValue.CDC_TYPE)
                        .append(" FROM ")
                        .append(fieldAndTable[1]);
            }

            properties.setProperty(ParameterName.SINK_SQL, stringBuilder.toString());

        }
    }

    /**
     * @param sql 创建表语句；
     * @param name 配置属性名；
     * @return 获得配置属性值，并返回；
     */
    public static String getSingMeta(String sql, String name) {
        String sqlDeal = removeTailLastSpecialSymbol(sql, ")", true);
        return sqlDeal
                .split("'" + name + "'", 2)[1]
                .replaceFirst("=", "")
                .split("','", 2)[0]
                .replaceAll("'", "").trim();
    }

    /**
     * @desc 用以获取数据源类型
     * @param sourceSet 数据源表创建语句
     * @return 返回数据源的类型，目前仅仅支持4种，分别为 KAFKA_TWO_SOURCE_JOIN，
     * MYSQL_CDC，KAFKA_SINGLE_SOURCE，ORACLE_CDC，其中 ORACLE_CDC 占时不在这进一步处理；
     */
    private static String getSourceType(String[] sourceSet){
        // 有两个创建表语句，为双流JOIN
        if (sourceSet.length == 2){
            return SourceType.TWO_STREAM_JOIN;
        // 仅有一个创建表语句，且连接类型为 mysql-cdc，数据源为 MySQL 的 CDC 同步操作
        } else if (sourceSet.length == 1
                // CDC 创建表语句，包含了类型(eg: CREATE TABLE tableName(...) WITH (...)|U,I,D)
                && getSingMeta(sourceSet[0].split("[|]")[0], ParameterConfigName.SOURCE_CONNECTOR).equals(ParameterConfigValue.MYSQL_CDC)){
            return SourceType.MYSQL_CDC;
        } else {
            return SourceType.ONE_STREAM;
        }
    }

    /**
     * @param sql kafka 的数据源创建表语句；
     * @param name 配置参数名字
     * @param value 配置参数值
     * @return  对 kafka 数据源原表的配置参数进行添加, 并返回；
     */
    private static String addKafkaSourceConfig(String sql, String name, String value){
        // 去掉创建表语句中 CREATE TABLE tb_(...) WITH (...) 尾括号(")")
        // 当存在错误格式数据时，是否忽略
        return removeTailLastSpecialSymbol(sql, ")", true)
                + ","
                + connectionParameterFormat(name, value)
                + ")";
    }

    /**
     * @desc 删除字符串中指定符号(最后一位)的结尾部分
     * @param str 目标字符串
     * @param symblo 指定符号
     * @param isSpecialSymbol 是否为特殊符号
     * @return
     */
    public static String removeTailLastSpecialSymbol(String str, String symblo, boolean isSpecialSymbol){
        // 先反转
        String reverse = StringUtils.reverse(str);
        // 指定分割符号
        String separator = isSpecialSymbol ? String.format("\\%s", symblo) : symblo;
        // 获取结果
        String result = reverse.split(separator, 2)[1];
        return StringUtils.reverse(result);
    }


    /**
     * @desc 对链接配置名，以及配置值进行格式化
     * @param name 配置名
     * @param value 配置值
     * @return
     */
    private  static String connectionParameterFormat(String name, String value){
        return "'"
                + name
                + "'"
                + " = "
                + "'"
                + value
                + "'";
    }


    /**
     *
     * @param sql kafka 的数据源创建表语句；
     * @param parameter 测试时，将使用测试表，为了避免表重复，需要对每个表名需要进行特殊处理
     * @param properties 参数配置
     * @return 对 kafka 数据源原表的配置参数进行修改, 并返回；
     * @throws Exception
     */
    private static String changeKafkaSourceConfig(String sql, Properties properties, int parameter) throws Exception {
        // 非测试模式
        if (properties.getProperty(ParameterName.RUM_MODE).equals(RunMode.START_MODE)){
            // 如果是从 savepoint 进行恢复操作
            // 需要对 kafka 的消费模式进行更改，如果变量包存在添加指标的操作，新增的指标就从指定位置进行消费数据，
            // 因为多个指标，每一个指标存在属于自己的消费偏移量记录，新增加的指标将采用的所有指标中的最大的消费偏移量；
            if (properties.getProperty(ParameterName.SAVEPOINT_PATH) != null){
                // 获取 topic 的名字
                String topic =  getSingMeta(sql, ParameterConfigName.TOPIC);
                // 获取 MySQL 连接
                Connection connection = MySqlUtils.getConnection(
                        properties.getProperty(ParameterName.SAVEPOINT_URL),
                        properties.getProperty(ParameterName.TEST_USER_NAME),
                        properties.getProperty(ParameterName.TEST_PASSWORD),
                        properties.getProperty(ParameterName.TEST_DRIVER)
                );
                // 执行查询获取分区和偏移量
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(
                        "SELECT offset FROM t_offset_record WHERE savepointpath = '" + properties.getProperty(ParameterName.SAVEPOINT_PATH) + "_" + topic + "'"
                );
                while (resultSet.next()){
                    // partition:0,offset:1241
                    String offset = resultSet.getString(1);
                    // 获取消费模式
                    String scanMode = getSingMeta(sql, ParameterConfigName.SCAN_STARTUP_MODE);
                    // 更改消费模式，从指定为位置进行消费
                    sql.replace("'" + scanMode + "'", "'" + ParameterConfigValue.SPECIFIC_OFFSETS + "'");
                    // 指定消费位置
                    sql =  addKafkaSourceConfig(sql,ParameterConfigName.SCAN_STARTUP_SPECIFIC_OFFSETS, offset.toLowerCase());
                }
                // 关闭 MySql 的连接
                MySqlUtils.closeConnection(connection);
            }
            // 测试模式
        } else if (properties.getProperty(ParameterName.RUM_MODE).equals(RunMode.TEST_MODE)){
            // 指定测试 topic ，用以替换原有的 topic
            String testTopic = "source_" + properties.getProperty(ParameterName.TEST_TOPIC_NAME) + parameter;
            // 数据源表，表名
            String testSourceName = properties.getProperty(ParameterName.TEST_SOUCE_TABLE_NAME, "");
            properties.put(ParameterName.TEST_SOUCE_TABLE_NAME, (testSourceName + "\t" + testTopic).trim());
            // 获取测试环境的 kafka 连接 broker 信息
            String testBrokeList = properties.getProperty(ParameterName.TEST_BROKER_LIST);
            // 获取 topic 的名字
            String topic =  getSingMeta(sql, ParameterConfigName.TOPIC);
            // 获取消费模式
            String scanMode = getSingMeta(sql, ParameterConfigName.SCAN_STARTUP_MODE);
            // 获取 kafka 的 broker 信息
            String brokerList = getSingMeta(sql,  ParameterConfigName.PROPERTIES_BOOTSTRAP_SERVERS);
            sql = sql
                    // 替换成测试 topic
                    .replace("'" + topic + "'", "'" + testTopic +"'")
                    // 从最开始的位置进行消费
                    .replace("'" + scanMode + "'", "'" + ParameterConfigValue.EARLIEST_OFFSET +"'")
                    // 替换 broker 信息
                    .replace("'" + brokerList + "'", "'" + testBrokeList +"'");
        }
        return sql;
    }


    /**
     * @desc  对数据原表创建语句进行处理
     * @param properties 参数属性
     */
    private static void  sourceTableSqlDeal(Properties properties) throws Exception {
        StringBuilder strbuid = new StringBuilder();
        // 数据源表创建语句,多个创建表语句之间使用分号(;)进行拼接
        String source = properties.getProperty(ParameterName.SOURCE_TABLE_SQL);
        String[] sourceSet = source.split(";");
        // 数据源的类型
        String sourceType = getSourceType(sourceSet);
        if (sourceType.equals(SourceType.TWO_STREAM_JOIN) || sourceType.equals(SourceType.ONE_STREAM)){
            for (int i = 0; i < sourceSet.length; i++) {
                // 添加 kafka 的配置参数, 是否忽略错误的 JSON 格式
                String createSql = addKafkaSourceConfig(sourceSet[i], ParameterConfigName.JSON_IGNORE_PARSE_ERRORS,properties.getProperty(ParameterName.JSON_IGNORE_PARSE_ERRORS, "false"));
                // 修改 kafka 的配置参数
                createSql = changeKafkaSourceConfig(createSql, properties, i);
                strbuid.append(createSql).append(";");
            }
        } else if (sourceType.equals(SourceType.MYSQL_CDC)){
            String[] sourcePara = dealMysqlCdc(properties, sourceSet[0]);
            strbuid.append(sourcePara[0]).append(";");
        }
        String sourceTableSql = strbuid.toString();
        properties.put(ParameterName.SOURCE_TYPE, sourceType);
        properties.put(ParameterName.SOURCE_TABLE_SQL, sourceTableSql.substring(0, sourceTableSql.length() - 1));
    }

    /**
     * @desc 处理 cdc 参数，返回创建表语句
     * @param properties
     * @param sql
     * @return
     */
    private static String[] dealMysqlCdc(Properties properties, String sql) {
        String[] sourcePara = sql.split("[|]", 2);
        String sourceName = removeTailFirstSpecialSymbol(sourcePara[0], "(", true).split("\\s+")[2];
        properties.put(ParameterName.CDC_SOURCE_TABLE_NAME, sourceName);
        properties.put(ParameterName.CDC_ROW_KIND, sourcePara[1]);
        return sourcePara;
    }

    /**
     * @desc 删除字符串中指定符号(首位位)的结尾部分
     * @param str 目标字符串
     * @param symblo 指定符号
     * @param isSpecialSymbol 是否为特殊符号
     * @return
     */
    public static String removeTailFirstSpecialSymbol(String str, String symblo, boolean isSpecialSymbol){
        // 指定分割符号
        String separator = isSpecialSymbol ? String.format("\\%s", symblo) : symblo;
        return str.split(separator, 2)[0];
    }


    /**
     * @desc 删除字符串中指定符号(首位)的前面部分
     * @param str 目标字符串
     * @param symblo 指定符号
     * @param isSpecialSymbol 是否为特殊符号
     * @return
     */
    public static String removeBeforeFirstSpecialSymbol(String str, String symblo, boolean isSpecialSymbol){
        // 指定分割符号
        String separator = isSpecialSymbol ? String.format("\\%s", symblo) : symblo;
        return str.split(separator, 2)[1];
    }


    /**
     * @desc 对于包含计算指标的变量包，需要对这些计算结果再此注册成一张表，用以后续的计算，
     *        对于测试模式与非测试模式，中间表的表名有所不同；
     * @param parameterProperties 参数属性
     */
    private static void getMidTableName(Properties parameterProperties){
        // 对于变量包，包含指标计算，则这些指标拼接后会重新注册成一张中间表，用以后续的计算操作
        // 不同运行模式，中间表的表名也不同
        if (RunMode.START_MODE.equals(parameterProperties.getProperty(ParameterName.RUM_MODE))){
            // 非测试模式下，中间表的表名: tmp_ + 变量包名字
            parameterProperties.put(ParameterName.MIDDLE_TABLE_NAME,"tmp_" + parameterProperties.getProperty(ParameterName.VARIABLE_PACK_EN));
        } else if (RunMode.TEST_MODE.equals(parameterProperties.getProperty(ParameterName.RUM_MODE))) {
            // 测试模式下，中间表的表名直接为测试输出的 topic 名字
            parameterProperties.put(ParameterName.MIDDLE_TABLE_NAME,parameterProperties.getProperty(ParameterName.TEST_TOPIC_NAME));
        }
    }


    /**
     * @desc 如果是测试模式，修改维表参数
     * @return
     * @param sql
     * @param type
     * @param properties
     */
    private static String changeDimensionSql(String sql, String type, int counts, Properties properties){
            String tableNmae = getSingMeta(sql, ParameterConfigName.TABLE_NAME);
            if (DimensionType.DIM_JDBC.equals(type)){
                String url = getSingMeta(sql, ParameterConfigName.TABLE_URL);
                String username = getSingMeta(sql, ParameterConfigName.TABLE_USERNAME);
                String password = getSingMeta(sql, ParameterConfigName.TABLE_PASSWORD);
                String driver = getSingMeta(sql, ParameterConfigName.TABLE_DRIVER);
                if (driver.startsWith("oracle.jdbc")){
                    sql = sql.replace("'" + tableNmae + "'", "'" + properties.getProperty(ParameterName.TEST_TOPIC_NAME) + counts + "'")
                            .replace("'" + url + "'", "'" + properties.getProperty(ParameterName.TEST_ORACLE_DIM_URL) + "'")
                            .replace("'" + username + "'", "'" + properties.getProperty(ParameterName.TEST_ORACLE_USERNAME) + "'")
                            .replace("'" + password + "'", "'" + properties.getProperty(ParameterName.TEST_ORACLE_PASSWORD) + "'")
                            .replace("'" + driver + "'", "'" + properties.getProperty(ParameterName.TEST_ORACLE_DRIVER) + "'");
                } else {
                   sql =  sql.replace("'" + tableNmae + "'", "'" + properties.getProperty(ParameterName.TEST_TOPIC_NAME) + counts + "'")
                            .replace("'" + url + "'", "'" + properties.getProperty(ParameterName.TEST_MYSQL_DIM_URL) + "'")
                            .replace("'" + username + "'", "'" + properties.getProperty(ParameterName.TEST_USER_NAME) + "'")
                            .replace("'" + password + "'", "'" + properties.getProperty(ParameterName.TEST_PASSWORD) + "'")
                            .replace("'" + driver + "'", "'" + properties.getProperty(ParameterName.TEST_DRIVER) + "'");
                }

            } else if (DimensionType.DIM_HBASE.equals(type)){
                String testHbaseTable = ParameterValue.NAME_SPACE + ":" + properties.getProperty(ParameterName.TEST_TOPIC_NAME) + counts;
                String zkquorum = getSingMeta(sql,ParameterConfigName.ZK_QUORUM );
                sql =  sql.replace("'" + tableNmae + "'", "'" + testHbaseTable + "'")
                        .replace("'" + zkquorum + "'", "'" + properties.getProperty(ParameterName.TEST_ZK) + "'");
            }
            return sql;
    }

    /**
     * @desc 根据运行模式，对数据维表的一些列参数进行修改
     * @param properties 参数属性
     */
    private static void dimTableSqlDeal(Properties properties){
        String dimensionTable = properties.getProperty(ParameterName.DIMENSION_TABLE);
        int dimCounts = 0;
        ArrayList<String> arrayList = new ArrayList<>();
        if (dimensionTable != null){
            for (Object oj : JSONObject.parseArray(dimensionTable).toArray()) {
                HashMap hashMap = JSON.parseObject(oj.toString(), HashMap.class);
                String dimensionSql = hashMap.get(ParameterName.DIMENSION_TABLE_SQL).toString();
                String dimType = hashMap.get(ParameterName.DIM_TYPE).toString();
                if (RunMode.TEST_MODE.equals(properties.getProperty(ParameterName.RUM_MODE))){
                    dimensionSql = changeDimensionSql(dimensionSql, dimType, dimCounts, properties);
                }
                arrayList.add(Objects.requireNonNull(JSONObject.toJSON(hashMap.put(ParameterName.DIMENSION_TABLE_SQL, dimensionSql))).toString());
            }
            properties.put(ParameterName.DIMENSION_TABLE,JSONObject.toJSON(arrayList).toString());
        }
    }

    private static void changeParameterValue(Properties properties){
        if (properties.getProperty(ParameterName.RUM_MODE).equals(RunMode.TEST_MODE)){
            properties.put(ParameterName.BATH_SIZE, 1);
        }
    }

    /**
     * @param properties 待处理的参数,
     */
    public static void parameterDeal(Properties properties) throws Exception {
        // 对数据源表创建语句进行处理
        sourceTableSqlDeal(properties);
        // 对数据维表创建语句进一步处理
        dimTableSqlDeal(properties);
        // 获取 Flink 的全局参数
        getGlobParameter(properties);
        // 根据运行模式修改参数值
        changeParameterValue(properties);
        // 获取中间表的表名
        getMidTableName(properties);
        // 对数据维表创建语句进行处理
        // 如果存在输出到指定的外部存储系统，需要对输出的语句进一步处理，包括指标顺序，以及测试模式下的的一些参数值进行更改;
        sinkSqlDeal(properties);
    }


    /**
     * @desc 解析 josn 串
     * @param str
     * @return
     */
    public static HashMap parseStrToProperties(String str) {
        return JSONObject.parseObject(str, HashMap.class);
    }

    /**
     * @desc 获取需要的全局变量
     */
    private static void getGlobParameter(Properties properties){
        HashMap<String, String> gloabParameterMap = new HashMap<>();
        // 数据源闲置时间，当某一个分区在一段时间类没有产生新的数据，那么就会触发新的 waterMark，
        // 当所有的数据源都闲置的时候，此时就会发出最大的 waterMark，用以触发所有的计算
        gloabParameterMap.put(ParameterName.IDLE_TIME_OUT, properties.getProperty(ParameterName.IDLE_TIME_OUT, -1 +""));
        // 此时间为双流join时，注册延迟触发时间
        if (properties.getProperty(ParameterName.TWO_STREAM_JOIN_PARAMETER) != null){
        // join的sql语句|注册表名(左右表名以下划线拼接_)|[-3,5](左边必须小于右边,数值必须带有单位符号)
            String twoStreamJoinSqls = properties.getProperty(ParameterName.TWO_STREAM_JOIN_PARAMETER);
            String[] split = twoStreamJoinSqls.split("[|]");
            String[] joinInterval = split[2]
                    .replaceFirst("\\[", "")
                    .replaceFirst("]", "")
                    .split(",", 2);
            int left = Integer.parseInt(joinInterval[0].trim());
            int right = Integer.parseInt(joinInterval[1].trim());
            // long cleanUpTime = rowTime + leftRelativeSize + minCleanUpInterval + allowedLateness + 1;
            // minCleanUpInterval = (leftRelativeSize + rightRelativeSize) / 2;
            // delayTime = leftRelativeSize + (leftRelativeSize + rightRelativeSize) / 2 + 1
            long delayTime = -left * 1000L + (-left * 1000L + right * 1000L) / 2 + 1;
            gloabParameterMap.put(ParameterName.TWO_STREAM_JOIN_DELAY_TIME, delayTime + "");
            //  join sql 语句
            properties.put(ParameterName.TWO_STREAM_JOIN_SQL, split[0]);
            // 对于双流 join, join 后的数据需要再注册成一张新表
            properties.put(ParameterName.TWO_STREAM_JOIN_REGISTER_TABLE_NAME, split[1]);
        }
        // 属性名 gloabParameter
        properties.put(ParameterName.GLOAB_PARAMETER, JSONObject.toJSONString(gloabParameterMap));
    }

}
