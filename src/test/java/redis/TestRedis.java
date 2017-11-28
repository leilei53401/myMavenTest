package redis;

import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.*;
import java.util.Map.Entry;

public class TestRedis {

//	private static final String SERVER_ADDRESS = "172.16.42.21";	//服务器地址
	private static final String SERVER_ADDRESS = "192.168.2.3";	//服务器地址
	private static final Integer SERVER_PORT = 6379 ;	//端口
	
	private Jedis jedis ;
	
	@Before
	public void init(){
		jedis = new Jedis(SERVER_ADDRESS, SERVER_PORT); 
	}
	
	
	
	/**
	 * 操作普通键值对
	 */
	@Test
	public void test01(){
		jedis.set("name", "redistest2");//存入1个key=name value=zhoufeng的键值对
		String value = jedis.get("name");  	//获取key=name的值
		System.out.println(value);
	}
	
	/**
	 * 操作List
	 */
	@Test
	public void test02(){
		
		//将zhoufeng 加入students数组的结尾,如果该元素是第一个元素，那么会自动创建students数组
		jedis.rpush("students", "zhoufeng");	
		
		//将zhangsan加入到students的末尾
		jedis.lpush("students", "zhangsan");
		
		//移除students的第一个元素
		jedis.lpop("students");
		
		//移除students的最后一个元素
		jedis.rpop("students");
		
		//移除制定的元素,第二个参数表示要移除的个数，因为list中是允许出现重复元素的
		jedis.lrem("student", 1, "zhangsan") ;
		//获取students数组的所有元素
		List <String> students = jedis.lrange("students", 0, -1) ;
		
		System.out.println(students);
	}
	
	/**
	 * 操作Set
	 */
	@Test
	public void test03(){
		//添加元素
		jedis.sadd("teachers", "zhangsan");
		jedis.sadd("teachers", "lisi");
		jedis.sadd("teachers", "wangwu");
		
		//判断Set是否包含制定元素
		Boolean b1 = jedis.sismember("teachers", "wangwu");
		Boolean b2 = jedis.sismember("teachers", "xxxxx");
		System.out.println(b1 + "   " + b2);  
		
		//获取Set内所有的元素
		Set<String> members =  jedis.smembers("teachers");
		Iterator<String> it = members.iterator() ;
		while(it.hasNext()){
			System.out.println(it.next());
		}
		
	//	jedis.sunion(keys...) 可以将多个Set合并成1个并返回合并后的Set
		
	}
	
	/**
	 * 操作带排序功能的Set
	 */
	@Test
	public void test04(){
		//添加元素，会根据第二个参数排序
		jedis.zadd("emps", 5 , "aaa") ;
		jedis.zadd("emps", 1 , "bbbb") ;
		jedis.zadd("emps", 3 , "ccc") ;
		jedis.zadd("emps", 2 , "ddd") ;
		
		//获取所有元素
		Set<String> emps = jedis.zrange("emps", 0, -1) ;
		Iterator<String> it = emps.iterator() ;
		while(it.hasNext()){
			System.out.println(it.next());
		}
	}
	
	/**
	 * 存入对象,使用Map作为对象
	 */
	@Test
	public void test05(){
		Map<String , String > car = new HashMap<String , String >() ;
		car.put("COLOR", "red") ;
		car.put("SIZE", "2T");
		car.put("NO", "京8888");
		//存入对象，使用car:01当作key，是为了方便和其他car区分。比如car:02
		jedis.hmset("car:01", car);
		
		//获取整个对象
		Map<String, String> result = jedis.hgetAll("car:01");	
		Iterator<Entry<String, String>>  it = result.entrySet().iterator();
		while(it.hasNext()){
			Entry<String, String> entry = it.next() ;  
			System.out.println("key:" + entry.getKey() + " value:" + entry.getValue());
		}
		
		//也可以获取制定的属性
		String no = jedis.hget("car:01", "NO") ;
		System.out.println("NO:" + no);
	}
	
}
