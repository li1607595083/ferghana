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

    // spark01:2181,spark02:2181,spark03:2181
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
        AdminUtils.createTopic(zkUtils, "test_trade_info_topic", 1, 1, new Properties(), RackAwareMode.Enforced$.MODULE$);
    }

    @Test
    public void deleteKafkaTopic(){
        AdminUtils.deleteTopic(zkUtils, "kfk_test_topic");
    }

    @Test
    public void listKafkaTopic(){
        Iterator<String> iter = zkUtils.getAllTopics().iterator();
        while (iter.hasNext()){
            System.out.println(iter.next());
        }
    }

}
