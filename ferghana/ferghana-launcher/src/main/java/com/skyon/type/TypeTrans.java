package com.skyon.type;


import org.apache.hadoop.hbase.util.Bytes;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.*;
import java.util.regex.Pattern;

public class TypeTrans {

    private static final List<String> stringList = TypeTrans.strArr();

    /**
     * Special symbols to aid in type extraction
     * @return
     */
    public static HashMap<String, String> sysArr() {
        HashMap<String, String> symble = new HashMap<>();
        symble.put("(", ")");
        symble.put("<", ">");
        symble.put("[", "]");
        symble.put("{", "}");
        return symble;
    }

    /**
     * Alphabet set
     * @return
     */
    public static List<String> strArr() {
        String[] str = {"A",  "B",  "C",  "D",  "E",  "F",  "G",  "H",  "I",  "J",  "K",  "L",  "M",  "N",  "O",  "P",  "Q",  "R",  "S",  "T",  "U",  "V",  "W",  "X",  "Y",  "Z",  "a",  "b",  "c",  "d",  "e",  "f",  "g",  "h",  "i",  "j",  "k",  "l",  "m",  "n",  "o",  "p",  "q",  "r",  "s",  "t",  "u",  "v",  "w",  "x",  "y",  "z"};
        return Arrays.asList(str);
    }

    /**
     * Converts to the corresponding byte according to the type
     * @param type
     * @param value
     * @return
     */
    public static byte[] hbaseByte(String type, String value){
        byte[] rs = null;
        switch (type){
            case "CHAR":    rs = Bytes.toBytes(value); break;
            case "VARCHAR":	    rs = Bytes.toBytes(value); break;
            case "STRING":      rs = Bytes.toBytes(value); break;
            case "BOOLEAN":     rs = Bytes.toBytes(Boolean.parseBoolean(value)); break;
            case "SMALLINT":    rs = Bytes.toBytes(Short.parseShort(value)); break;
            case "INT":     rs = Bytes.toBytes(Integer.parseInt(value)); break;
            case "BIGINT":  rs = Bytes.toBytes(Long.parseLong(value)); break;
            case "FLOAT":   rs = Bytes.toBytes(Float.parseFloat(value)); break;
            case "DOUBLE":  rs = Bytes.toBytes(Double.parseDouble(value)); break;
        }
        return rs;
    }

    /**
     * @desc 类型转换
     * @return
     */
    public static HashMap<String, String> typeAsMySql() {
        HashMap<String, String> msMap = new HashMap<>();
        msMap.put("INT", "INT");
        msMap.put("BIGINT", "BIGINT");
        msMap.put("FLOAT", "FLOAT");
        msMap.put("DOUBLE", "DOUBLE");
        msMap.put("BOOLEAN", "BOOLEAN");
        msMap.put("DATE", "DATE");
        msMap.put("TIMESTAMP", "DATETIME");
        msMap.put("STRING", "VARCHAR(255)");
        return msMap;
    }

    /**
     * @desc 类型转换
     * @return
     */
    public static HashMap<String, String> typeAsOracle() {
        HashMap<String, String> msMap = new HashMap<>();
        msMap.put("INT", "DECIMAL");
        msMap.put("BIGINT", "DECIMAL");
        msMap.put("FLOAT", "DECIMAL(38,5)");
        msMap.put("DOUBLE", "DECIMAL(38,5)");
        msMap.put("DECIMAL", "DECIMAL");
        msMap.put("DATE", "DATE");
        msMap.put("TIMESTAMP", "TIMESTAMP");
        msMap.put("STRING", "VARCHAR2(255)");
        return msMap;
    }


    public static PreparedStatement oraclePs(PreparedStatement ps, String type,int index, String values,int columnNum) throws SQLException {
        if (!"null".equals(values)) {
            switch (type) {
                case "INT":
                    ps.setBigDecimal(index, new BigDecimal(values));
                    if (columnNum != 0) ps.setBigDecimal(index + columnNum, new BigDecimal(values));
                    break;
                case "BIGINT":
                    ps.setBigDecimal(index, new BigDecimal(values));
                    if (columnNum != 0) ps.setBigDecimal(index + columnNum, new BigDecimal(values));
                    break;
                case "FLOAT":
                    ps.setBigDecimal(index, new BigDecimal(values));
                    if (columnNum != 0) ps.setBigDecimal(index + columnNum, new BigDecimal(values));
                    break;
                case "DOUBLE":
                    ps.setBigDecimal(index, new BigDecimal(values));
                    if (columnNum != 0) ps.setBigDecimal(index + columnNum, new BigDecimal(values));
                    break;
                case "DECIMAL":
                    ps.setBigDecimal(index, new BigDecimal(values));
                    if (columnNum != 0) ps.setBigDecimal(index + columnNum, new BigDecimal(values));
                case "DATE":
                    ps.setDate(index, java.sql.Date.valueOf(values));
                    if (columnNum != 0) ps.setDate(index + columnNum, java.sql.Date.valueOf(values));
                    break;
                case "TIMESTAMP":
                    values = values.replaceAll("T", " ");
                    ps.setTimestamp(index, Timestamp.valueOf(values));
                    if (columnNum != 0) ps.setTimestamp(index + columnNum, Timestamp.valueOf(values));
                    break;
                case "STRING":
                    ps.setString(index, values);
                    if (columnNum != 0) ps.setString(index + columnNum, values);
                    break;
            }
        } else {
            switch (type) {
                case "INT":
                    ps.setNull(index, Types.DECIMAL);
                    if (columnNum != 0) ps.setNull(index + columnNum, Types.DECIMAL);
                    break;
                case "BIGINT":
                    ps.setNull(index, Types.DECIMAL);
                    if (columnNum != 0) ps.setNull(index + columnNum, Types.DECIMAL);
                    break;
                case "FLOAT":
                    ps.setNull(index, Types.DECIMAL);
                    if (columnNum != 0) ps.setNull(index + columnNum, Types.DECIMAL);
                    break;
                case "DOUBLE":
                    ps.setNull(index, Types.DECIMAL);
                    if (columnNum != 0) ps.setNull(index + columnNum, Types.DECIMAL);
                    break;
                case "DECIMAL":
                    ps.setNull(index, Types.DECIMAL);
                    if (columnNum != 0) ps.setNull(index + columnNum, Types.DECIMAL);
                case "DATE":
                    ps.setNull(index, Types.DATE);
                    if (columnNum != 0) ps.setNull(index + columnNum, Types.DATE);
                    break;
                case "TIMESTAMP":
                    ps.setNull(index, Types.TIMESTAMP);
                    if (columnNum != 0) ps.setNull(index + columnNum, Types.TIMESTAMP);
                    break;
                case "STRING":
                    ps.setNull(index, Types.VARCHAR);
                    if (columnNum != 0) ps.setNull(index + columnNum, Types.VARCHAR);
                    break;
            }
        }
        return ps;

    }


    public static PreparedStatement mysqlPs(PreparedStatement ps, String type,int index, String values,int columnNum) throws SQLException {
        if (!"null".equals(values)) {
            switch (type) {
                case "INT":
                    ps.setInt(index, Integer.parseInt(values));
                    ps.setInt(index + columnNum, Integer.parseInt(values));
                    break;
                case "BIGINT":
                    ps.setLong(index, Long.parseLong(values));
                    ps.setLong(index + columnNum, Long.parseLong(values));
                    break;
                case "FLOAT":
                    ps.setFloat(index, Float.parseFloat(values));
                    ps.setFloat(index + columnNum, Float.parseFloat(values));
                    break;
                case "DOUBLE":
                    ps.setDouble(index, Double.parseDouble(values));
                    ps.setDouble(index + columnNum, Double.parseDouble(values));
                    break;
                case "BOOLEAN":
                    ps.setBoolean(index, Boolean.parseBoolean(values));
                    ps.setBoolean(index + columnNum, Boolean.parseBoolean(values));
                    break;
                case "DATE":
                    ps.setDate(index, java.sql.Date.valueOf(values));
                    ps.setDate(index + columnNum, java.sql.Date.valueOf(values));
                    break;
                case "TIMESTAMP":
                    values = values.replaceFirst("T", " ");
                    ps.setTimestamp(index, Timestamp.valueOf(values));
                    ps.setTimestamp(index + columnNum, Timestamp.valueOf(values));
                    break;
                case "STRING":
                    ps.setString(index, values);
                    ps.setString(index + columnNum, values);
                    break;
            }
        } else {
            switch (type) {
                case "INT":
                    ps.setNull(index, Types.INTEGER);
                    ps.setNull(index + columnNum, Types.INTEGER);
                    break;
                case "BIGINT":
                    ps.setNull(index, Types.BIGINT);
                    ps.setNull(index + columnNum, Types.BIGINT);
                    break;
                case "FLOAT":
                    ps.setNull(index, Types.FLOAT);
                    ps.setNull(index + columnNum, Types.FLOAT);
                    break;
                case "DOUBLE":
                    ps.setNull(index, Types.DOUBLE);
                    ps.setNull(index + columnNum, Types.DOUBLE);
                    break;
                case "BOOLEAN":
                    ps.setNull(index, Types.BOOLEAN);
                    ps.setNull(index + columnNum, Types.BOOLEAN);
                    break;
                case "DATE":
                    ps.setNull(index, Types.DATE);
                    ps.setNull(index + columnNum, Types.DATE);
                    break;
                case "TIMESTAMP":
                    ps.setNull(index, Types.TIMESTAMP);
                    ps.setNull(index + columnNum, Types.TIMESTAMP);
                    break;
                case "STRING":
                    ps.setNull(index, Types.VARCHAR);
                    ps.setNull(index + columnNum, Types.VARCHAR);
                    break;
            }
        }
        return ps;

    }

    /**
     * Flink AS ES
     */
    public static HashMap<String, String> typeAsEs(){
        HashMap<String, String> esMap = new HashMap<>();
        esMap.put("CHAR", "\"type\":\"text\",\"index\":\"true\",\"analyzer\":\"ik_max_word\"");
        esMap.put("VARCHAR", "\"type\":\"text\",\"index\":\"true\",\"analyzer\":\"ik_max_word\"");
        esMap.put("STRING", "\"type\":\"text\",\"index\":\"true\",\"analyzer\":\"ik_max_word\"");
        esMap.put("BOOLEAN", "\"type\":\"boolean\",\"index\":\"false\"");
        esMap.put("INT", "\"type\":\"integer\",\"index\":\"false\"");
        esMap.put("BIGINT", "\"type\":\"long\",\"index\":\"false\"");
        esMap.put("FLOAT", "\"type\":\"float\",\"index\":\"false\"");
        esMap.put("DOUBLE", "\"type\":\"double\",\"index\":\"false\"");
        esMap.put("DATE", "\"type\":\"date\",\"index\":\"false\"");
        esMap.put("TIMESTAMP", "\"type\":\"date\",\"index\":\"false\"");
        return esMap;
    }

    /**
     * @desc 后去 flink 内置类型，去掉特殊符号
     * @param type
     * @return
     */
    public static String getTranKey(String type){
        String msKey = "";
        for (String cha : type.split("")) {
            if (stringList.contains(cha)){
                msKey = msKey + cha;
            } else {
                break;
            }
        }
        return msKey;
    }

    /**
     * @desc 处理 flink 内置的类型
     * @param unsolType
     * @return
     */
    public static String getType(String unsolType){
        List<String> strArr = strArr();
        HashMap<String, String> symbolHash = sysArr();
        String fieldType = "";
        String symbol = null;
        String symbolReverse = null;
        int count = 0;
        for (String s : unsolType.split("")) {
            if (strArr.contains(s)){
                fieldType = fieldType + s;
            } else if ((Pattern.matches("\\s+", s) || s.equals(",")) && count == 0){
                break;
            } else {
                fieldType = fieldType + s;
                if (symbol == null){
                    symbol = s;
                    symbolReverse = symbolHash.get(symbol);
                    count++;
                } else if (symbol.equals(s)){
                    count++;
                } else if (symbolReverse.equals(s)){
                    count--;
                }
            }
        }
        return fieldType;
    }

    public static String getUid(String input) {
        String[] split = input.replaceAll("\\s+", "").trim().split("");
        String uid = "";
        for (String s : split) {
            switch (s){
                case "0": uid = uid + "a";break;
                case "1": uid = uid + "b";break;
                case "2": uid = uid + "c";break;
                case "3": uid = uid + "d";break;
                case "4": uid = uid + "e";break;
                case "5": uid = uid + "f";break;
                case "6": uid = uid + "g";break;
                case "7": uid = uid + "h";break;
                case "8": uid = uid + "i";break;
                case "9": uid = uid + "j";break;
                default: uid = uid + s;
            }
        }
        if (uid.length() >= 33){
            uid = uid.substring(0, 33);
        }
        return uid;
    }


}
