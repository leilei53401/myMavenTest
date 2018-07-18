package redis;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.zookeeper.proto.SetACLRequest;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class RedisShardPoolTestV2 {
    
    static ShardedJedisPool pool;


    @Before
    public void init(){

            JedisPoolConfig config =new JedisPoolConfig();//Jedis池配置
//            config.setMaxActive(500);//最大活动的对象个数
            config.setMaxIdle(1000 * 60);//对象最大空闲时间
//            config.setMaxWait(1000 * 10);//获取对象时最大等待时间
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
//            jdsInfoList.add(infoA);
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
    /*	ShardedJedis jds = null;
        try {
            jds = pool.getResource();
            System.out.println( jds.get("108")); 
            System.out.println( jds.get("108-20170620")); 
            System.out.println( jds.get("108-ABCD5ffd91b8")); 
            System.out.println( jds.get("108-ABCD5ffd91b8-20170620")); 
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            pool.returnResource(jds);
        }*/


    }


    @Test
    public void testBase(){
        ShardedJedis jds = null;
        try {
            jds = pool.getResource();

            jds.set("dev_test_20180507","2001");

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

    @Test
    public void testHkeys(){
        System.out.println("======testHkeys()=====");
        ShardedJedis jds = null;
        try {
            jds = pool.getResource();

            Set<String> expdaykeysSet =  jds.hkeys("expday*");

            if(null!=expdaykeysSet && expdaykeysSet.size()>0){

                for(String key: expdaykeysSet){
                    System.out.println("key : "+key);
                }

            }else{
                System.out.println("未获取到expdaykeysSet信息!");
            }



        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            pool.returnResource(jds);
        }
    }



    @Test
    public void testRedisMd5Set(){


        long startSet = System.currentTimeMillis();


        Set md5Set = new HashSet<String>();

        md5Set.add("40b48b1e0f26a9d94a611a0556257304");
        md5Set.add("29c33fba179d2b851ac9570cd16add6b");
        md5Set.add("03f1de3d7a53b7beae76e356aeead85a");
        md5Set.add("90971f5a8685aad243dfc8fe596f40c1");
        md5Set.add("2440f971e0a05887f6f8b6ba7701b1e5");
        md5Set.add("3acd0ebbae7b3ded7a1213fe79779059");
        md5Set.add("762de2f5bbffb67770e0c70b2969a238");
        md5Set.add("583cf2710d21bab2ad859de054f79545");
        md5Set.add("cff3487abbcf8b269ec45197b1bf77cd");
        md5Set.add("492842ac4cc7bc21ecbd4c02f05fc3e9");


        long endSet = System.currentTimeMillis();

        System.out.println("add  mac to set 耗时： "+(endSet- startSet)+"毫秒!");

//        Object[] strMd5Macs = md5Set.toArray();
        String[] strMd5Macs = (String[]) md5Set.toArray(new String[md5Set.size()]);

        long endConvert = System.currentTimeMillis();
        System.out.println("convert to String[] 耗时： "+(endConvert-endSet)+"毫秒!");

        ShardedJedis jds = null;
        try {
            jds = pool.getResource();

            jds.sadd("devday_20180507",strMd5Macs);


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            pool.returnResource(jds);
        }
        long endToRedis = System.currentTimeMillis();
        System.out.println("put to redis 耗时： "+(endToRedis-endConvert)+"毫秒!");

        System.out.println(jds.scard("expdev_20180507"));

        System.out.println("总耗时： "+(endToRedis-startSet)+"毫秒!");


    }



    @Test
    public void testReadFileMd5BySet(){

        long startSet = System.currentTimeMillis();

        Set md5Set = new HashSet<String>(5000);

        long count = 0l;
        File theFile = new File("/opt/data/konkamacnew/001A34/top100w_md5mac.txt");
        try {
            LineIterator it = FileUtils.lineIterator(theFile, "UTF-8");
            try {
                while (it.hasNext()) {
                    String line = it.nextLine();
                    md5Set.add(line);

                    count++;

                    if(count%5000==0){
                        //每1000条处理一批
                        System.out.println("java set size = "+md5Set.size());
                        String[] strMd5Macs = (String[]) md5Set.toArray(new String[md5Set.size()]);
                        addToRedis(strMd5Macs);
                        md5Set.clear();
                    }
                }

                if(md5Set.size()>0){
                    //将结尾数据存入redis
                    System.out.println("java last set size = "+md5Set.size());
                    String[] strMd5Macs = (String[]) md5Set.toArray(new String[md5Set.size()]);
                    addToRedis(strMd5Macs);
                    md5Set.clear();
                }

            } finally {
                LineIterator.closeQuietly(it);
            }
            System.out.println("共处理数据["+count+"]条!");


            //########### 最后获取数据大小查看 ################

            ShardedJedis jds = null;
            try {
                jds = pool.getResource();
                System.out.println("再次获取 redis set大小为:"+jds.scard("devday_20180507_2"));
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                pool.returnResource(jds);
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        long endSet = System.currentTimeMillis();

        System.out.println("总耗时： "+(endSet-startSet)+"毫秒!");

    }


    private void addToRedis(String... members){

        long startToRedis = System.currentTimeMillis();

        ShardedJedis jds = null;
        try {
            jds = pool.getResource();


            System.out.println(members.length+" 个数据开始入 redis !");

            jds.sadd("devday_20180507_2",members);


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            pool.returnResource(jds);
        }

        long endToRedis = System.currentTimeMillis();

        System.out.println("put to redis 耗时： "+(endToRedis-startToRedis)+"毫秒!");


        System.out.println("当前set大小为:"+jds.scard("devday_20180507_2"));



    }

    
    private static int index = 1;
    public static String generateKey(){
        return String.valueOf(Thread.currentThread().getId())+"_"+(index++);
    }
}
