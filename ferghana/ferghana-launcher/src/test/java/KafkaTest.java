import com.alibaba.fastjson.JSON;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZkUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.security.JaasUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.collection.Iterator;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaTest {

    ZkUtils zkUtils;

    // spark01:2181
    @Before
    public void getZkUtils() {
        zkUtils = ZkUtils.apply("192.168.4.95:2181", 30000, 30000, JaasUtils.isZkSecurityEnabled());
    }

    @After
    public void clostZkUtils() {
        if (zkUtils != null){
            zkUtils.close();
        }
    }

    @Test
    public void createKafkaTopic() {
        AdminUtils.createTopic(zkUtils, "EP_OPENACCT_FLOW_TOPIC", 2, 1, new Properties(), RackAwareMode.Enforced$.MODULE$);
    }

    //kafka_sink_topic
    @Test
    public void deleteKafkaTopic(){
        AdminUtils.deleteTopic(zkUtils, "EP_OPENACCT_FLOW_TOPIC");
    }

    @Test
    public void listKafkaTopic(){
        Iterator<String> iter = zkUtils.getAllTopics().iterator();
        while (iter.hasNext()){
            String topic = iter.next();
            if ((topic.startsWith("source_topic") && topic.length() == 25) || (topic.length() == 18 && topic.startsWith("topic"))){
                AdminUtils.deleteTopic(zkUtils, topic);
                System.out.println(topic);
            }
            System.out.println(topic);
        }
    }


    @Test
    public void addDate() throws InterruptedException {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "192.168.4.95:9092");
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("acks", "all");
        KafkaProducer producer  = new KafkaProducer<String, String>(props);
        int counts = 0;
        int stma = 0;
        while (true){
            Map<String,String> map = new HashMap<String,String>();
            map.put("TRAN_NO", UtilData.getTranNo());
            map.put("CUST_NO",UtilData.getCustNo());
            map.put("CUST_NAME",UtilData.custName());
            map.put("STATUS","00");
            map.put("BSN_CODE","01");
            map.put("ACCT_NO",UtilData.getAcctNo());
            map.put("MOBILE_NO",UtilData.getTel());
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            map.put("TRAN_TIME",sdf.format(new Date(System.currentTimeMillis() - 3 * 60 * 1000)));
            String s = JSON.toJSONString(map);
            producer.send(new ProducerRecord<String, String>("EP_OPENACCT_FLOW_TOPIC", s));
            counts ++;
            Thread.sleep(2);
            stma++;
            if (counts >= 1){
                counts = 0;
                System.out.println("当前发送条数:\t" + stma);
                Thread.sleep(1000000);
            }
        }

    }

}
