package redis;

import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by Administrator on 2018-5-25.
 */
public class RedisClusterTest {

    @Test
    public void testCluster() {
        Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
        //Jedis Cluster will attempt to discover cluster nodes automatically
        jedisClusterNodes.add(new HostAndPort("192.168.3.5", 7000));
        JedisCluster jc = new JedisCluster(jedisClusterNodes);
        jc.set("foo2", "bar22");
        String value = jc.get("foo2");
        System.out.println(value);
    }
}
