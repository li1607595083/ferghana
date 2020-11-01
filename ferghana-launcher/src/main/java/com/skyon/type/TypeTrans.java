package com.skyon.type;

import org.apache.hadoop.hbase.util.Bytes;

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
     * Flink as MySql
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
     * Used to get the Flink Type
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
     * Further processing of the type
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


}
