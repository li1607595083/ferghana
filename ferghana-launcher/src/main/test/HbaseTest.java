import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class HbaseTest {

    Connection conn;

    @Before
    public void createHbaseConnection() throws IOException {
        Configuration hbaseConfig = HBaseConfiguration.create();
        hbaseConfig.set("hbase.zookeeper.quorum", "spark01:2181,spark02:2181,spark03:2181");
        conn = ConnectionFactory.createConnection(hbaseConfig);
    }

    @Test
    public void updateData() throws IOException {
        TableName tableName = TableName.valueOf("test:products");
        Table table = conn.getTable(tableName);
        Put put = new Put("5".getBytes());
        Double unitPrice = 888.88D;
        put.addColumn("info".getBytes(), "name".getBytes(), "cf".getBytes());
        put.addColumn("info".getBytes(), "unitPrice".getBytes(), Bytes.toBytes(unitPrice));
        table.put(put);
        if (table != null){
            table.close();
        }
    }

    @Test
    public void deleteTable() throws IOException {
        TableName tableName = TableName.valueOf("TEST:mytable");
        Admin admin = conn.getAdmin();
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
        if (admin != null){
            admin.close();
        }
    }

    @Test
    public void deleteNameSpace() throws Exception{
        Admin admin = conn.getAdmin();
        admin.deleteNamespace("TEST");
        if (admin != null){
            admin.close();
        }
    }

    @Test
    public void readerData() throws IOException {
        TableName tableName = TableName.valueOf("test:ordeproduct");
        Table table = conn.getTable(tableName);
        ResultScanner scanner = table.getScanner(new Scan());
        for (Result result : scanner) {
            String rk = Bytes.toString(result.getRow());
            // ROW(productId STRING, units INT, orderTime TIMESTAMP(3), proctime TIMESTAMP(3), name STRING, unitPrice DOUBLE)
            Iterator<Map.Entry<byte[], byte[]>> iterator = result.getFamilyMap("info".getBytes()).entrySet().iterator();
            String productId = Bytes.toString(result.getValue("info".getBytes(), "productId".getBytes()));
            String units = Bytes.toString(result.getValue("info".getBytes(), "units".getBytes()));
            String orderTime = Bytes.toString(result.getValue("info".getBytes(), "orderTime".getBytes()));
            String proctime = Bytes.toString(result.getValue("info".getBytes(), "proctime".getBytes()));
            String name = Bytes.toString(result.getValue("info2".getBytes(), "name".getBytes()));
            String unitPrice = Bytes.toString(result.getValue("info2".getBytes(), "unitPrice".getBytes()));
            System.out.println(rk + "\tproductId: " + productId + "\tunits: " + units + "\torderTime: " + orderTime + "\tproctime: " + proctime + "\tname: " + name + "\tunitPrice: " + unitPrice);
        }
        if (table != null){
            table.close();
        }
    }

    @Test
    public void writeData() throws Exception {
        TableName tableName = TableName.valueOf("TEST:mytable");
        Table table = conn.getTable(tableName);
        Put put = new Put("1234".getBytes());
    }

    @After
    public void closeHbaseConnection() throws IOException {
        if (conn != null){
            conn.close();
        }
    }

}
