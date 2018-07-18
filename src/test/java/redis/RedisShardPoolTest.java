package redis;

import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

import java.util.ArrayList;
import java.util.List;

public class RedisShardPoolTest {
    
    static ShardedJedisPool pool;

    static{
        JedisPoolConfig config =new JedisPoolConfig();//Jedis池配置
//        config.setMaxActive(500);//最大活动的对象个数
        config.setMaxIdle(1000 * 60);//对象最大空闲时间
//        config.setMaxWait(1000 * 10);//获取对象时最大等待时间
        config.setTestOnBorrow(true);
        String hostA = "192.168.2.3";
        int portA = 6379;
        String hostB = "192.168.2.5";
        int portB = 6379;
        List<JedisShardInfo> jdsInfoList =new ArrayList<JedisShardInfo>(2);
        JedisShardInfo infoA = new JedisShardInfo(hostA, portA);
//        infoA.setPassword("admin");
        JedisShardInfo infoB = new JedisShardInfo(hostB, portB);
//        infoB.setPassword("admin");
        jdsInfoList.add(infoA);
        jdsInfoList.add(infoB);
        pool =new ShardedJedisPool(config, jdsInfoList);
        System.out.println("================== init ========================");
     }

       

    /**
    
     * @param args
    
     */
    
    public static void main(String[] args) {
       /* for(int i=0; i<10; i++){
            String key = generateKey();
            ShardedJedis jds = null;
            try {
                jds = pool.getResource();
                System.out.println(key+":"+jds.getShard(key).getClient().getHost());
                System.out.println(jds.set(key,Math.random()+""));
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                pool.returnResource(jds);
            }
        }
        
        System.out.println( "================================== ");
        index = 1;
        for(int i=0; i<10; i++){
            String key = generateKey();
            ShardedJedis jds = null;
            try {
                jds = pool.getResource();
                System.out.println("new key is : "+key);
                System.out.println( jds.get(key)); 
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                pool.returnResource(jds);
            }
        }*/
    	
    	//测试获取排期值
    	ShardedJedis jds = null;
        try {
            jds = pool.getResource();

            jds.set("dev_test_20180507","1001");

            System.out.println( jds.get("dev_test_20180507"));

  /*          System.out.println( jds.get("108-20170620"));
            System.out.println( jds.get("108-ABCD5ffd91b8")); 
            System.out.println( jds.get("108-ABCD5ffd91b8-20170620")); */
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            pool.returnResource(jds);
        }
    }
    
    private static int index = 1;
    public static String generateKey(){
        return String.valueOf(Thread.currentThread().getId())+"_"+(index++);
    }
}
