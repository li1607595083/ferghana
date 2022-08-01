import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

public class RedisTest {

    Jedis jedis;

    @Before
   public void getJedis(){
        jedis = new Jedis("192.168.30.33", 6379, 5000);
   }

    /**
     * String
     */
   @Test
   public void addDataStr(){
        while (!jedis.isConnected()){
            jedis.connect();
        }
       Transaction multi = jedis.multi();
       multi.set("str", "StringTest");
       multi.hset("001", "1", "one");
       multi.hset("001", "2", "one1");
       multi.hset("002", "1", "two");
       multi.hset("002", "2", "two2");
       multi.hset("003", "1", "three");
       multi.hset("003", "2", "three2");
       multi.sadd("set", "set1", "set2", "set3", "set4");
       multi.sadd("set", "set5", "set0", "set4", "set6", "set7", "set80");
       multi.zadd("", 0.0, "");
       multi.exec();
   }

   @After
   public void closeJedis(){
        if (jedis != null){
            jedis.close();
        }
   }

}
