package memcachetest;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Date;

import org.apache.commons.lang.StringUtils;

import junit.framework.TestCase;

public class MemClientTest extends TestCase{ 
	
	 public static void testTread() {
		   
	}
	
	public static void testGetAdstatMemValue20161205() {
		 
		    System.out.println("============开始getAdstatMemValue20161205============");
			//目录
			String path = "E:\\tmp\\getMemValue\\";
			File f = new File(path);
			String[] files = f.list(new FilenameFilter() {
				public boolean accept(File dir, String name) {
					if (name.endsWith(".txt")) {
						return true;
					}
					return false;
				}
			});
			
			 System.out.println("=======files.length==["+files.length+"]============");
			//循环处理
			for (String processFile : files) {
				System.out.println("============开始处理文件["+processFile+"]============");
				//开始处理文件
				//"E:\\tmp\\ip\\2016101101_900101_20161112.txt"
				File input = new File(path+processFile);
				BufferedReader br = null;
				File f1 = null;
				FileWriter fw1 = null;
				BufferedWriter bw1 = null;
				//数据样例：
				//<2016-12-01 14:38:01,330>  INFO (PlayLogThresholdService.java:129) [TaskExePool-2-TaskProcessor3] (com.voole.ad.service.impl.PlayLogThresholdService) - pln_day = [1], set addOrIncr key : 1010000801-f20220189da8-20161201

				try {
					br = new BufferedReader(new FileReader(input));
					String line = null;
					// 写文件
					String outFile =  processFile.replaceAll(".txt", ".out");
					//"E:\\tmp\\ip\\2016101101_900101_20161112.out"
					f1 = new File(path+outFile);
					fw1 = new FileWriter(f1, true);
					bw1 = new BufferedWriter(fw1);
					int count=0;
					int belowTenCount=0;
					int overTenCount=0;
					// 读
					while ((line = br.readLine()) != null) {
						count++;
						String key = StringUtils.trim(line);
						int value = 0;
						if(null==MemcachedUtils.get(key)){
							System.err.println(key+" value is null!");
						}else{
						  value = Integer.valueOf(MemcachedUtils.get(key).toString());
						  if(value<10){
//							  System.out.println(key+":"+value);
							  belowTenCount++;
							  continue;
						  }else{
							  overTenCount++;
						  }
						}
						// 写
						String newline = key+":"+value;
						bw1.write(newline);
						bw1.newLine();
					}
					
					System.out.println("文件["+processFile+"]处理完成，处理数据["+count+"]条!,belowTenCount=["+belowTenCount+"],overTenCount=["+overTenCount+"]");
					
				} catch (IOException ex) {
					ex.printStackTrace();
				} finally {
					try {
						if (input != null && input.exists()) {
							input.delete();
						}
						if (br != null)
							br.close();
		
						if (bw1 != null) {
							bw1.flush();
							bw1.close();
						}
		
					} catch (IOException e) {
						e.printStackTrace();
					}
		
				}
			
			}
			
			System.out.println("done!");

	 }
	
	/**
	 * 删除memcache中key值
	 */
	public static void testDelAdstatMemKey() {
		 
	    System.out.println("============开始删除memcache中key值============");
		//目录
		String path = "E:\\tmp\\delMemKey\\";
		File f = new File(path);
		String[] files = f.list(new FilenameFilter() {
			public boolean accept(File dir, String name) {
				if (name.endsWith(".txt")) {
					return true;
				}
				return false;
			}
		});
		
		 System.out.println("=======files.length==["+files.length+"]============");
		//循环处理
		for (String processFile : files) {
			System.out.println("============开始处理文件["+processFile+"]============");
			//开始处理文件
			//"E:\\tmp\\ip\\2016101101_900101_20161112.txt"
			File input = new File(path+processFile);
			BufferedReader br = null;
			File f1 = null;
			FileWriter fw1 = null;
			BufferedWriter bw1 = null;
			//数据样例：
			//<2016-12-01 14:38:01,330>  INFO (PlayLogThresholdService.java:129) [TaskExePool-2-TaskProcessor3] (com.voole.ad.service.impl.PlayLogThresholdService) - pln_day = [1], set addOrIncr key : 1010000801-f20220189da8-20161201

			try {
				br = new BufferedReader(new FileReader(input));
				String line = null;
				// 写文件
				String outFile =  processFile.replaceAll(".txt", ".out");
				//"E:\\tmp\\ip\\2016101101_900101_20161112.out"
				f1 = new File(path+outFile);
				fw1 = new FileWriter(f1, true);
				bw1 = new BufferedWriter(fw1);
				int count=0;
				int okCount=0;
				int errCount=0;
				// 读
				while ((line = br.readLine()) != null) {
					count++;
					String key = StringUtils.trim(line);
					int value = 0;
					if(MemcachedUtils.delete(key)){
						okCount++;
					}else{
						errCount++;
					}
					// 写
					String newline = key+":"+value;
					bw1.write(newline);
					bw1.newLine();
				}
				
				System.out.println("文件["+processFile+"]处理完成，处理数据["+count+"]条!,okCount=["+okCount+"],errCount=["+errCount+"]");
				
			} catch (IOException ex) {
				ex.printStackTrace();
			} finally {
				try {
					if (input != null && input.exists()) {
						input.delete();
					}
					if (br != null)
						br.close();
	
					if (bw1 != null) {
						bw1.flush();
						bw1.close();
					}
	
				} catch (IOException e) {
					e.printStackTrace();
				}
	
			}
		
		}
		
		System.out.println("done!");

 }
	 
	 
	/**
   * 测试MemcachedUtils类的set方法。
   * 
   * @author GaoHuanjie
   */
 public static void testSet1() {
    MemcachedUtils.set("set1Description", "调用MemcachedUtils类的set方法，没有设置键值对的存在时长");
    System.out.println(MemcachedUtils.get("set1Description").toString());
  }

  /**
   * 测试MemcachedUtils类的set方法。
   * 
   * @author GaoHuanjie
   */ 
   public static void testSet2() {
    MemcachedUtils.set("set2Description", "调用MemcachedUtils类的set方法，设置了键值对的存在时长——存在60秒", new Date(1000*60));
    System.out.println(MemcachedUtils.get("set2Description").toString());
  }
   //获取以上值查看，或者直接telnet 到本机 get值查看
   public static void testGetSet2() {
//	    MemcachedUtils.set("set2Description", "调用MemcachedUtils类的set方法，设置了键值对的存在时长——存在60秒", new Date(1000*60));
	    System.out.println((null==MemcachedUtils.get("set2Description"))?"":MemcachedUtils.get("set2Description").toString());
	    System.out.println("=======end=========");
	}
   
   
   //
   public static void testIncr() {
//	    MemcachedUtils.set("set2Description", "调用MemcachedUtils类的set方法，设置了键值对的存在时长——存在60秒", new Date(1000*60));
	    Date expDayTime = new Date(1000*60*60);
	 // 排期+hid+day(日频次控制)
		long pln_day = MemcachedUtils.incr("1010000735-1ca770d25229-20161129");
		
		System.out.println("pln_day is  "+pln_day);
		// logger.info("pln_hid_localyearDay==============="+playlog.getPlanid()
		// + "-" + hid + "-" + localyearDay);
		if (pln_day < 0) {
			MemcachedUtils.set("1010000735-1ca770d25229-20161129", "1",expDayTime);
		}
		
		Date date = new Date();
		 System.out.println("date="+date);
	    System.out.println("=======end=========");
	}
   
   public static void testGetIncr() {
//	    MemcachedUtils.set("set2Description", "调用MemcachedUtils类的set方法，设置了键值对的存在时长——存在60秒", new Date(1000*60));
	   String key = "1010000735-1ca770d25229-20161129";
	    System.out.println((null==MemcachedUtils.get(key))?"":MemcachedUtils.get(key).toString());
		Date date = new Date();
		 System.out.println("date="+date);
	    System.out.println("=======end=========");
	}
   
   
	

  /**
   * 测试MemcachedUtils类的add方法。
   * 
   * @author GaoHuanjie
   */  public static void testAdd1() {
    MemcachedUtils.add("add1Description", "调用MemcachedUtils类的add方法，没有设置键值对的存在时长");
    System.out.println(MemcachedUtils.get("add1Description").toString());
  }

  /**
   * 测试MemcachedUtils类的add方法。
   * 
   * @author GaoHuanjie
   */  public static void testAdd2() {
    MemcachedUtils.add("add2Description", "调用MemcachedUtils类的add方法，设置了键值对的存在时长——存在60秒", new Date(1000*60));
    System.out.println(MemcachedUtils.get("add2Description").toString());
  }

  /**
   * 测试MemcachedUtils类的replace方法。
   * 
   * @author GaoHuanjie
   */  public static void testReplace1() {
    MemcachedUtils.add("replace1Description", "调用MemcachedUtils类的replace方法，没有设置键值对的存在时长");
    MemcachedUtils.replace("replace1Description", "值改变了！！！");
    System.out.println(MemcachedUtils.get("replace1Description").toString());
  }

  /**
   * 测试MemcachedUtils类的replace方法。
   * 
   * @author GaoHuanjie
   */  public static void testReplace2() {
    MemcachedUtils.add("replace2Description", "调用MemcachedUtils类的replace方法，设置了键值对的存在时长——存在60秒", new Date(1000*60));
    MemcachedUtils.replace("replace2Description", "值改变了！！！", new Date(1000*60));
    System.out.println(MemcachedUtils.get("replace2Description").toString());
  }

  /**
   * 测试MemcachedUtils类的get方法。
   * 
   * @author GaoHuanjie
   */  public static void testGet() {
    MemcachedUtils.add("getDescription", "调用MemcachedUtils类的get方法，没有设置键值对的存在时长");
    System.out.println(MemcachedUtils.get("getDescription").toString());
  }

  /**
   * 测试MemcachedUtils类的delete方法。
   * 
   * @author GaoHuanjie
   */  public static void testDelete1() {
    MemcachedUtils.add("delete1Description", "调用MemcachedUtils类的delete方法，没有设置键值对的逾期时长");
    MemcachedUtils.delete("delete1Description");
    assertEquals(null, MemcachedUtils.get("delete1Description"));
  }

  /**
   * 测试MemcachedUtils类的delete方法。
   * 
   * @author GaoHuanjie
   */  public static void testDelete2() {
    MemcachedUtils.set("delete2Description1", "调用MemcachedUtils类的delete方法，设置键值对的逾期时长", new Date(600*1000));
    MemcachedUtils.delete("delete2Description1", new Date(1000*600));
    assertEquals(null, MemcachedUtils.get("delete2Description1"));
  }

  /**
   * 测试MemcachedUtils类的flashAll方法。
   * 
   * @author GaoHuanjie
   */  public static void testFlashAll() {
    MemcachedUtils.add("flashAllDescription", "调用MemcachedUtils类的delete方法，没有设置键值对的预期时长");
    MemcachedUtils.flashAll();
    assertEquals(null, MemcachedUtils.get("flashAllDescription"));
  }
}