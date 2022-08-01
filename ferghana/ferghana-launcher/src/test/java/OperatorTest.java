
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.junit.Test;

import java.util.*;

public class OperatorTest {

    @Test
    public void jiami() throws Exception {
        //09+11+19+28;04+05+29|04+05+09
        String mimi = "eyJzb3VyY2VUYWJsZVNxbCI6IkNSRUFURSBUQUJMRSBgRVBfT1BFTkFDQ1RfRkxPV19UQUJMRWAoYFRSQU5fTk9gIFNUUklORyxgQ1VTVF9OT2AgU1RSSU5HLGBBQ0NUX05PYCBTVFJJTkcsYExPR0lOX05PYCBTVFJJTkcsYENVU1RfTkFNRWAgU1RSSU5HLGBNT0JJTEVfTk9gIFNUUklORyxgQ0VSVF9UWVBFYCBTVFJJTkcsYENFUlRfTk9gIFNUUklORyxgQlNOX0NPREVgIFNUUklORyxgU1RBVFVTYCBTVFJJTkcsYFRFUk1fQ0hBTk5FTGAgU1RSSU5HLGBUUkFOX0RBVEVgIFRJTUVTVEFNUCxgVFJBTl9USU1FYCBUSU1FU1RBTVAscHJvY3RpbWUgQVMgUFJPQ1RJTUUoKSxXQVRFUk1BUksgRk9SIGBUUkFOX1RJTUVgIGFzIFRSQU5fVElNRSAtIElOVEVSVkFMICc1JyBTRUNPTkQpIFdJVEggKCdjb25uZWN0b3InID0gJ2thZmthLTAuMTEnICwndG9waWMnID0gJ0VQX09QRU5BQ0NUX0ZMT1dfVE9QSUMnLCdwcm9wZXJ0aWVzLmJvb3RzdHJhcC5zZXJ2ZXJzJyA9ICdtYXN0ZXI6OTA5MicsJ3Byb3BlcnRpZXMuZ3JvdXAuaWQnID0gJ3Rlc3QtZ3JvdXAnLCdzY2FuLnN0YXJ0dXAubW9kZScgPSAnbGF0ZXN0LW9mZnNldCcsJ2Zvcm1hdCcgPSAnanNvbicpIiwidHJhbnNpdGlvblNxbCI6IlNFTEVDVCAqLCByZWRpc19xdWVyeSgnc2lzbWVtYmVyJywndmlydHVhbF9tb2JpbGVfcGhvbmVfbnVtYmVyJyxTVUJTVFJJTkcoTU9CSUxFX05PLDAsMykpICBBUyBraGZxel92YXIxIEZST00gdG1wX3RhYmxlO1NFTEVDVCAqLHRpbWVfaW50ZXJ2YWwoVFJBTl9USU1FLCdbMDE6MDAsMDI6MDBdJykgQVMga2hmcXpfdmFyMyBGUk9NIHRtcF90YWJsZTtzZWxlY3QgKiwgaWYoIEJTTl9DT0RFID0gJzAxJyBBTkQgIFNUQVRVUyA9ICcwMCcgQU5EICBDVVNUX1NUUyA9ICcwJyBBTkQgIEFERFIgSVMgTlVMTCwxLDApIEFTIGtoZnF6X3ZhcjQgZnJvbSB0bXBfdGFibGU7Iiwic2lua1NxbCI6IiBpbnNlcnQgaW50byBFUF9PUEVOQUNDVF9EQVRBX1JFU1VMVCAoc2VsZWN0IFRSQU5fTk8sQ1VTVF9OTyxBQ0NUX05PLExPR0lOX05PLENVU1RfTkFNRSxNT0JJTEVfTk8sQ0VSVF9UWVBFLENFUlRfTk8sQlNOX0NPREUsU1RBVFVTLFRFUk1fQ0hBTk5FTCxUUkFOX0RBVEUsVFJBTl9USU1FLEFDQ1RfTk9fVFdPLEJJTkRfQUNDVF9OTyxWRVJJRllfRkxHLEJJTkRfQkFOS19OQU1FLENVU1RfTk8sQUREUixDVVNUX1NUUyxNT0JJTEVfTk8sTE9HSU5fREFURSxraGZxel92YXIxLGtoZnF6X3ZhcjIsa2hmcXpfdmFyMyxraGZxel92YXI0LGtoZnF6X3ZhcjUgRlJPTSB0bXBfa2hmcXpfdmFyX3BhY2thZ2UpIiwiY29ubmVjdG9yVHlwZSI6IjAxIiwiYWdnTm9QYXJ0aXRpb25TcWwiOiJTRUxFQ1QgKiwgY291bnQoQklORF9CQU5LX05BTUVfUkUpICBvdmVyKCBPUkRFUiBCWSBUUkFOX1RJTUUgUkFOR0UgQkVUV0VFTiBJTlRFUlZBTCAnMzAnIE1JTlVURSBwcmVjZWRpbmcgQU5EIENVUlJFTlQgUk9XKSAgQVMga2hmcXpfdmFyNV8wMSBGUk9NKFNFTEVDVCAqLElGKCBCU05fQ09ERT0nMDEnIGFuZCAgU1RBVFVTPScwMCcgYW5kICBWRVJJRllfRkxHPScxJyxCSU5EX0JBTktfTkFNRSxDQVNUKGlmRmFsc2VTZXROdWxsKCkgQVMgU1RSSU5HKSkgQVMgQklORF9CQU5LX05BTUVfUkUgRlJPTSB0bXBfdGFibGUpIEFTIHRtcDtTRUxFQ1QgKiwgY291bnQoQUNDVF9OT19SRSkgIG92ZXIoIE9SREVSIEJZIFRSQU5fVElNRSBSQU5HRSBCRVRXRUVOIElOVEVSVkFMICczMCcgTUlOVVRFIHByZWNlZGluZyBBTkQgQ1VSUkVOVCBST1cpICBBUyBraGZxel92YXI1XzAyIEZST00oU0VMRUNUICosSUYoIEJTTl9DT0RFPScwMScgYW5kICBTVEFUVVM9JzAwJyBhbmQgIFZFUklGWV9GTEc9JzEnLEFDQ1RfTk8sQ0FTVChpZkZhbHNlU2V0TnVsbCgpIEFTIFNUUklORykpIEFTIEFDQ1RfTk9fUkUgRlJPTSB0bXBfdGFibGUpIEFTIHRtcDsiLCJrYWZrYVRvcGljIjoiRVBfT1BFTkFDQ1RfREFUQV9SRVNVTFQiLCJhZ2dQYXJ0aXRpb25TcWwiOiJTRUxFQ1QgKiwgY291bnQoVFJBTl9OT19SRSkgIG92ZXIoIFBBUlRJVElPTiBCWSBDVVNUX05BTUUgT1JERVIgQlkgVFJBTl9USU1FIFJBTkdFIEJFVFdFRU4gSU5URVJWQUwgJzMwJyBNSU5VVEUgcHJlY2VkaW5nIEFORCBDVVJSRU5UIFJPVykgIEFTIGtoZnF6X3ZhcjIgRlJPTShTRUxFQ1QgKixJRiggQlNOX0NPREU9JzAxJyBvciAgQlNOX0NPREU9JzAyJyBhbmQgIFNUQVRVUz0nMDAnLFRSQU5fTk8sQ0FTVChpZkZhbHNlU2V0TnVsbCgpIEFTIFNUUklORykpIEFTIFRSQU5fTk9fUkUgRlJPTSB0bXBfdGFibGUpIEFTIHRtcDsiLCJrYWZrYVpLIjoibWFzdGVyOjIxODEsc2xhdmU6MjE4MSIsImthZmthQWRkcmVzcyI6Im1hc3Rlcjo5MDkyLHNsYXZlOjkwOTIiLCJvcmlnaW5hbFZhcmlhYmxlU3FsIjoic2VsZWN0IFRSQU5fTk8sQ1VTVF9OTyxBQ0NUX05PLExPR0lOX05PLENVU1RfTkFNRSxNT0JJTEVfTk8sQ0VSVF9UWVBFLENFUlRfTk8sQlNOX0NPREUsU1RBVFVTLFRFUk1fQ0hBTk5FTCxUUkFOX0RBVEUsVFJBTl9USU1FLEFDQ1RfTk9fVFdPLEJJTkRfQUNDVF9OTyxWRVJJRllfRkxHLEJJTkRfQkFOS19OQU1FLENVU1RfTk8sQUREUixDVVNUX1NUUyxNT0JJTEVfTk8sTE9HSU5fREFURSBGUk9NIEVQX09QRU5BQ0NUX0ZMT1dfVEFCTEVfam9pbl9FUF9DVVNUX0lORl9qb2luX0VQX0JJTkRfQUNDVCIsInJ1bk1vZGUiOiIwMiIsImZpZWxkT3V0TnVtIjo4LCJ2YXJpYWJsZVBhY2tFbiI6ImtoZnF6X3Zhcl9wYWNrYWdlIiwiZGltZW5zaW9uVGFibGUiOlt7ImRpbWVuc2lvblRhYmxlU3FsIjoiIENSRUFURSBUQUJMRSBgRVBfQklORF9BQ0NUYCAoIGBBQ0NUX05PX1RXT2AgU1RSSU5HLGBCSU5EX0FDQ1RfTk9gIFNUUklORyxgVkVSSUZZX0ZMR2AgU1RSSU5HLGBCSU5EX0JBTktfTkFNRWAgU1RSSU5HICxQUklNQVJZIEtFWSAoYEFDQ1RfTk9fVFdPYCkgTk9UIEVORk9SQ0VEKSBXSVRIICAoJ2Nvbm5lY3RvcicgPSAnamRiYycsICd1cmwnID0gJ2pkYmM6bXlzcWw6Ly9tYXN0ZXI6MzMwNi90ZXN0P2NoYXJhY3RlckVuY29kaW5nPVVURi04JywnZHJpdmVyJyA9ICdjb20ubXlzcWwuY2ouamRiYy5Ecml2ZXInLCd1c2VybmFtZScgPSAnZmVyZ2hhbmEnLCdwYXNzd29yZCcgPSAnRmVyZ2hhbmFAMTIzNCcsJ3RhYmxlLW5hbWUnID0gJ0VQX0JJTkRfQUNDVCcpIiwidGVzdERpbVR5cGUiOiIwNiJ9LHsiZGltZW5zaW9uVGFibGVTcWwiOiIgQ1JFQVRFIFRBQkxFIGBFUF9DVVNUX0lORmAgKCBgQ1VTVF9OT2AgU1RSSU5HLGBBRERSYCBTVFJJTkcsYENVU1RfU1RTYCBTVFJJTkcsYE1PQklMRV9OT2AgU1RSSU5HLGBMT0dJTl9EQVRFYCBEQVRFICxQUklNQVJZIEtFWSAoYENVU1RfTk9gKSBOT1QgRU5GT1JDRUQpIFdJVEggICgnY29ubmVjdG9yJyA9ICdqZGJjJywgJ3VybCcgPSAnamRiYzpteXNxbDovL21hc3RlcjozMzA2L3Rlc3Q/Y2hhcmFjdGVyRW5jb2Rpbmc9VVRGLTgnLCdkcml2ZXInID0gJ2NvbS5teXNxbC5jai5qZGJjLkRyaXZlcicsJ3VzZXJuYW1lJyA9ICdmZXJnaGFuYScsJ3Bhc3N3b3JkJyA9ICdGZXJnaGFuYUAxMjM0JywndGFibGUtbmFtZScgPSAnRVBfQ1VTVF9JTkYnKSIsInRlc3REaW1UeXBlIjoiMDYifV0sImRlcml2ZVNxbCI6IlNFTEVDVCAqLGtoZnF6X3ZhcjVfMDEgPCA1IGFuZCBraGZxel92YXI1XzAyID4gMTAgYXMga2hmcXpfdmFyNSBGUk9NIHRtcF90YWJsZSIsIndhdGVyTWFyayI6IlRSQU5fVElNRXw1Iiwiam9pblNxbCI6ImNyZWF0ZSB0YWJsZSBgRVBfT1BFTkFDQ1RfRkxPV19UQUJMRV9qb2luX0VQX0NVU1RfSU5GX2pvaW5fRVBfQklORF9BQ0NUYCAoc2VsZWN0IHMuKixFUF9DVVNUX0lORi4qLEVQX0JJTkRfQUNDVC4qIGZyb20gRVBfT1BFTkFDQ1RfRkxPV19UQUJMRSBzICBsZWZ0IGpvaW4gRVBfQklORF9BQ0NUIEZPUiBTWVNURU1fVElNRSBBUyBPRiBzLnByb2N0aW1lIEFTIEVQX0JJTkRfQUNDVCBPTiBzLkFDQ1RfTk8gPSBFUF9CSU5EX0FDQ1QuQUNDVF9OT19UV08gbGVmdCBqb2luIEVQX0NVU1RfSU5GIEZPUiBTWVNURU1fVElNRSBBUyBPRiBzLnByb2N0aW1lIEFTIEVQX0NVU1RfSU5GIE9OIHMuQ1VTVF9OTyA9IEVQX0NVU1RfSU5GLkNVU1RfTk8pIn0=";
        String jiemi = new String(Base64.getDecoder().decode(mimi));
        System.out.println("解密后:\t" +jiemi);
        String str = "{\"sourceTableSql\":\"CREATE TABLE `EP_OPENACCT_FLOW_TABLE`(`TRAN_NO` STRING,`CUST_NO` STRING,`ACCT_NO` STRING,`LOGIN_NO` STRING,`CUST_NAME` STRING,`MOBILE_NO` STRING,`CERT_TYPE` STRING,`CERT_NO` STRING,`BSN_CODE` STRING,`STATUS` STRING,`TERM_CHANNEL` STRING,`TRAN_DATE` TIMESTAMP,`TRAN_TIME` TIMESTAMP,proctime AS PROCTIME(),WATERMARK FOR `TRAN_TIME` as TRAN_TIME - INTERVAL '5' SECOND) WITH ('connector' = 'kafka-0.11' ,'topic' = 'EP_OPENACCT_FLOW_TOPIC','properties.bootstrap.servers' = 'master:9092','properties.group.id' = 'test-group-01','scan.startup.mode' = 'latest-offset','format' = 'json')\",\"transitionSql\":\"SELECT *, redis_query('sismember','virtual_mobile_phone_number',SUBSTRING(MOBILE_NO,0,3))  AS khfqz_var1 FROM tmp_table;SELECT *,time_interval(TRAN_TIME,'[01:00,02:00]') AS khfqz_var3 FROM tmp_table;select *, if( BSN_CODE = '01' AND  STATUS = '00' AND  CUST_STS = '0' AND  ADDR IS NULL,1,0) AS khfqz_var4 from tmp_table;\",\"sinkSql\":\" insert into EP_OPENACCT_DATA_RESULT_01 (select TRAN_NO,CUST_NO,ACCT_NO,LOGIN_NO,CUST_NAME,MOBILE_NO,CERT_TYPE,CERT_NO,BSN_CODE,STATUS,TERM_CHANNEL,TRAN_DATE,TRAN_TIME,ACCT_NO_TWO,BIND_ACCT_NO,VERIFY_FLG,BIND_BANK_NAME,CUST_NO,ADDR,CUST_STS,MOBILE_NO,LOGIN_DATE,khfqz_var1,khfqz_var2,khfqz_var3,khfqz_var4,khfqz_var5 FROM tmp_khfqz_var_package)\",\"connectorType\":\"01\",\"aggNoPartitionSql\":\"SELECT *, count(BIND_BANK_NAME_RE)  over( ORDER BY TRAN_TIME RANGE BETWEEN INTERVAL '30' MINUTE preceding AND CURRENT ROW)  AS khfqz_var5_01 FROM(SELECT *,IF( BSN_CODE='01' and  STATUS='00' and  VERIFY_FLG='1',BIND_BANK_NAME,CAST(ifFalseSetNull() AS STRING)) AS BIND_BANK_NAME_RE FROM tmp_table) AS tmp;SELECT *, count(ACCT_NO_RE)  over( ORDER BY TRAN_TIME RANGE BETWEEN INTERVAL '30' MINUTE preceding AND CURRENT ROW)  AS khfqz_var5_02 FROM(SELECT *,IF( BSN_CODE='01' and  STATUS='00' and  VERIFY_FLG='1',ACCT_NO,CAST(ifFalseSetNull() AS STRING)) AS ACCT_NO_RE FROM tmp_table) AS tmp;\",\"kafkaTopic\":\"EP_OPENACCT_DATA_RESULT_01\",\"aggPartitionSql\":\"SELECT *, count(TRAN_NO_RE)  over( PARTITION BY CUST_NAME ORDER BY TRAN_TIME RANGE BETWEEN INTERVAL '30' MINUTE preceding AND CURRENT ROW)  AS khfqz_var2 FROM(SELECT *,IF( BSN_CODE='01' or  BSN_CODE='02' and  STATUS='00',TRAN_NO,CAST(ifFalseSetNull() AS STRING)) AS TRAN_NO_RE FROM tmp_table) AS tmp;\",\"kafkaZK\":\"master:2181,slave:2181\",\"kafkaAddress\":\"master:9092,slave:9092\",\"originalVariableSql\":\"select TRAN_NO,CUST_NO,ACCT_NO,LOGIN_NO,CUST_NAME,MOBILE_NO,CERT_TYPE,CERT_NO,BSN_CODE,STATUS,TERM_CHANNEL,TRAN_DATE,TRAN_TIME,ACCT_NO_TWO,BIND_ACCT_NO,VERIFY_FLG,BIND_BANK_NAME,CUST_NO,ADDR,CUST_STS,MOBILE_NO,LOGIN_DATE FROM EP_OPENACCT_FLOW_TABLE_join_EP_CUST_INF_join_EP_BIND_ACCT\",\"runMode\":\"02\",\"fieldOutNum\":8,\"variablePackEn\":\"khfqz_var_package\",\"dimensionTable\":[{\"dimensionTableSql\":\" CREATE TABLE `EP_BIND_ACCT` ( `ACCT_NO_TWO` STRING,`BIND_ACCT_NO` STRING,`VERIFY_FLG` STRING,`BIND_BANK_NAME` STRING ,PRIMARY KEY (`ACCT_NO_TWO`) NOT ENFORCED) WITH  ('connector' = 'jdbc', 'url' = 'jdbc:mysql://master:3306/test?characterEncoding=UTF-8','driver' = 'com.mysql.cj.jdbc.Driver','username' = 'ferghana','password' = 'Ferghana@1234','table-name' = 'EP_BIND_ACCT')\",\"testDimType\":\"06\"},{\"dimensionTableSql\":\" CREATE TABLE `EP_CUST_INF` ( `CUST_NO` STRING,`ADDR` STRING,`CUST_STS` STRING,`MOBILE_NO` STRING,`LOGIN_DATE` DATE ,PRIMARY KEY (`CUST_NO`) NOT ENFORCED) WITH  ('connector' = 'jdbc', 'url' = 'jdbc:mysql://master:3306/test?characterEncoding=UTF-8','driver' = 'com.mysql.cj.jdbc.Driver','username' = 'ferghana','password' = 'Ferghana@1234','table-name' = 'EP_CUST_INF')\",\"testDimType\":\"06\"}],\"deriveSql\":\"SELECT *,khfqz_var5_01 < 5 and khfqz_var5_02 > 10 as khfqz_var5 FROM tmp_table\",\"waterMark\":\"TRAN_TIME|5\",\"joinSql\":\"create table `EP_OPENACCT_FLOW_TABLE_join_EP_CUST_INF_join_EP_BIND_ACCT` (select s.*,EP_CUST_INF.*,EP_BIND_ACCT.* from EP_OPENACCT_FLOW_TABLE s  left join EP_BIND_ACCT FOR SYSTEM_TIME AS OF s.proctime AS EP_BIND_ACCT ON s.ACCT_NO = EP_BIND_ACCT.ACCT_NO_TWO left join EP_CUST_INF FOR SYSTEM_TIME AS OF s.proctime AS EP_CUST_INF ON s.CUST_NO = EP_CUST_INF.CUST_NO)\"}";
        JSONObject jsonObject = JSON.parseObject(str);
        jsonObject.entrySet().stream().forEach(x -> System.out.println(x));
        byte[] bytes = str.getBytes();
        //Base64 加密
        String encoded = Base64.getEncoder().encodeToString(bytes);
        System.out.println("Base 64 加密后：" + encoded);
        byte[] decode = Base64.getDecoder().decode(encoded);
        System.out.println(new String(decode));
        // taskManger cpu 的使用情况
        double rate = 0.0000027003768868056586;
        System.out.println(rate + "");

        double init = 0.6;
        for (int i = 0; i < 200; i++) {
            init *=1.03;
            System.out.println(init);
        }
    }

    @Test
    public void re() throws SqlParseException {
        long currentTimeMillis = System.currentTimeMillis();
        String str = "CREATE TABLE order_source_topic("
                + "orderId STRING,"
                + "productId STRING,"
                + "amount DOUBLE,"
                + "orderTime TIMESTAMP,"
                + "proctime AS PROCTIME(),"
                + "WATERMARK FOR orderTime as orderTime - INTERVAL '5' SECOND "
                + ") WITH ("
                + "'connector' = 'kafka-0.11' ,"
                + "'topic' = 'order_source_topic_1',"
                + "'properties.bootstrap.servers' = 'master:9092',"
                + "'properties.group.id' = 'test-group-2',"
                + "'scan.startup.mode' = 'specific-offsets',"
                + "'format' = 'json'"
                + ",'scan.startup.specific-offsets' = '" + "partition:0,offset:20" +"')";
        String str_2 = "CREATE TABLE order_source_topic_2("
                + "orderId STRING,"
                + "productId STRING,"
                + "amount DOUBLE,"
                + "orderTime TIMESTAMP,"
                + "proctime AS PROCTIME(),"
                + "WATERMARK FOR orderTime as orderTime - INTERVAL '5' SECOND "
                + ") WITH ("
                + "'connector' = 'kafka-0.11' ,"
                + "'topic' = 'order_source_topic_1',"
                + "'properties.bootstrap.servers' = 'master:9092',"
                + "'properties.group.id' = 'test-group-2',"
                + "'scan.startup.mode' = 'specific-offsets',"
                + "'format' = 'json'"
                + ",'scan.startup.specific-offsets' = '" + "partition:0,offset:20" +"')";
        String sql = "SELECT * FROM order_source_topic";
        String sql_2 = "SELECT " +
                "orderId , " +
                "orderTime, " +
                "SUM(wc) " +
                "OVER( PARTITION BY productId ORDER BY orderTime RANGE BETWEEN INTERVAL '99' MINUTE preceding AND CURRENT ROW) " +
                "FROM (SELECT *, amount * 10 AS wc  FROM qwer1234) AS tmp";
        EnvironmentSettings build = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        TableEnvironmentImpl tableEnvironment = TableEnvironmentImpl.create(build);
        tableEnvironment.executeSql(str);
        System.out.println(System.currentTimeMillis() - currentTimeMillis);
        tableEnvironment.sqlQuery(sql);
//        List<String> sqlList = new ArrayList<>();
//        SqlParser sqlParser = SqlParser.create(sql_2, SqlParser.configBuilder()
//                .setParserFactory(FlinkSqlParserImpl.FACTORY)
//                .setQuoting(Quoting.BACK_TICK)
//                .setUnquotedCasing(Casing.TO_LOWER)
//                .setQuotedCasing(Casing.UNCHANGED)
//                .setConformance(FlinkSqlConformance.DEFAULT)
//                .build());
//        List<SqlNode> sqlNodeList = sqlParser.parseStmtList().getList();
//        if (sqlParser != null && !sqlNodeList.isEmpty()){
//            for (SqlNode sqlNode : sqlNodeList) {
//                sqlList.add(sqlNode.toString());
//            }
//        }
        System.out.println(System.currentTimeMillis() - currentTimeMillis);
        HashMap<String, String> stringStringHashMap = new HashMap<>();
        String fe = stringStringHashMap.get("fe");
        System.out.println(fe);

    }

}
