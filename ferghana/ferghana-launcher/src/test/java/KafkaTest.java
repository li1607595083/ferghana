import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZkUtils;
import org.apache.kafka.common.security.JaasUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.collection.Iterator;
import java.util.Properties;

public class KafkaTest {

    ZkUtils zkUtils;

    // spark01:2181
    @Before
    public void getZkUtils() {
        zkUtils = ZkUtils.apply("master:2181", 30000, 30000, JaasUtils.isZkSecurityEnabled());
    }

    @After
    public void clostZkUtils() {
        if (zkUtils != null){
            zkUtils.close();
        }
    }

    @Test
    public void createKafkaTopic() {
        AdminUtils.createTopic(zkUtils, "order_main_table_detail", 2, 1, new Properties(), RackAwareMode.Enforced$.MODULE$);
    }

    //kafka_sink_topic
    @Test
    public void deleteKafkaTopic(){
        AdminUtils.deleteTopic(zkUtils, "order_main_table_detail");
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

}
