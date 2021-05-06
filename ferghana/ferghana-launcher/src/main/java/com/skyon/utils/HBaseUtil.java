package com.skyon.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class HBaseUtil {

        /**
         * @param zkQuorum zookeeper地址，多个要用逗号分隔
         * @param port     zookeeper端口号
         * @return
         */
        public static Connection getHbaseConnection(String zkQuorum, int port) throws Exception{
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", zkQuorum);
            conf.set("hbase.zookeeper.property.clientPort", port + "");
            Connection connection = ConnectionFactory.createConnection(conf);
            return connection;
        }

}
