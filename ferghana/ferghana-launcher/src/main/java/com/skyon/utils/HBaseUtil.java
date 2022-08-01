package com.skyon.utils;

import com.skyon.bean.ParameterConfigName;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.util.List;

public class HBaseUtil {

        /**
         * @param zkQuorum zookeeper地址，多个要用逗号分隔
         * @param port     zookeeper端口号
         * @return
         */
        public static Connection getHbaseConnection(String zkQuorum, int port) throws Exception{
            Configuration conf = HBaseConfiguration.create();
            conf.set(ParameterConfigName.HBASE_ZK_QUORUM, zkQuorum);
            conf.set(ParameterConfigName.ZK_CLIENT_PORT, port + "");
            Connection connection = ConnectionFactory.createConnection(conf);
            return connection;
        }

    /**
     * @desc 根据连接获取 admin
     * @param connection
     * @return
     * @throws IOException
     */
    public static Admin getHbaseAdmin(Connection connection) throws IOException {
            return  connection.getAdmin();
        }

    /**
     * @desc 创建 namespace
     * @param namespace
     * @param admin
     * @throws IOException
     */
    public static void createNamespace(String namespace, Admin admin) throws IOException {
            boolean flag = true;
            for (NamespaceDescriptor descriptor : admin.listNamespaceDescriptors()) {
                if (descriptor.getName().equals(namespace)){
                    flag = false;
                }
            }
            if (flag){
                NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();
                admin.createNamespace(namespaceDescriptor);
            }
    }

    /**
     * @desc 创建 habase 表
     * @param admin
     * @param tableName
     * @param family
     * @throws IOException
     */
    public static void createTable(Admin admin, TableName tableName, List<String> family) throws IOException {
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
        for (String s : family) {
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(s);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }
        admin.createTable(hTableDescriptor);
    }

    /**
     * @desc 更新 hbase 表
     * @param admin
     * @param tableName
     * @param family
     * @throws IOException
     */
    public static void updateTable(Admin admin, TableName tableName, List<String> family) throws IOException {
        HTableDescriptor tableDescriptor = admin.getTableDescriptor(tableName);
        for(HColumnDescriptor fdescriptor : tableDescriptor.getColumnFamilies()){
            family.remove(fdescriptor.getNameAsString());
        }
        if (family.size() > 0){
            admin.disableTable(tableName);
            for (String fam : family) {
                HColumnDescriptor hColumnDescriptor=new HColumnDescriptor(fam);
                tableDescriptor.addFamily(hColumnDescriptor);
                admin.modifyTable(tableName, tableDescriptor);
            }
            admin.enableTable(tableName);
        }
    }

    public static void closeAdmin(Admin admin) throws IOException {
        if (admin != null){
            admin.close();
        }
    }

    public static void closeConn(Connection connection) throws IOException {
        if (connection != null){
            connection.close();
        }
    }

}
