package hive;

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 涉及到广协地址库相关处理
 * @author shaoyl
 *
 */
public class AreaTest {
	
	Logger logger = LoggerFactory.getLogger(AreaTest.class);
	
	
	private JdbcTemplate hiveJt;// 连接池
	private JdbcTemplate mysqlJt;// 连接池
	
	Map<String,String> proMap = null;
	Map<String,String> cityMap = null;
	Map<String,String> proCityMap = null;
	
	
	Map<String,String>  oemMap = null; 
	Map<String,String>  posMap = null;
	
	Map<String,String>  vendorMap = null;
	 Map<String,String>  amidMap = null;
	
	
	 Map<String,String>  oemidEpgMap = null;
	 Map<String,String>  epgDicMap = null;
	 Map<String,String>  adMap = null;
	 Map<String,Integer>  adLenMap = null;
	 //频道
	 Map<String,String>  channelMap = null;
	 
	 static RangeMap<Double, String[]> rangeMap = TreeRangeMap.create();

	public AreaTest(){
		BasicDataSource dataSource1 = new BasicDataSource();
		dataSource1.setDriverClassName("org.apache.hive.jdbc.HiveDriver");
		dataSource1.setUrl("jdbc:hive2://192.168.2.3:10000/default");
//		dataSource1.setUrl("jdbc:hive2://172.16.10.95:10000/default");
//		dataSource1.setUrl("jdbc:hive2://172.16.30.25:10000/default");
//		dataSource1.setUrl("jdbc:hive2://172.16.42.17:10000/superssp_adstat");
	
//		dataSource1.setUsername("statuser");
//		dataSource1.setPassword("statuser*()");
		dataSource1.setUsername("root");
		dataSource1.setPassword("root123");
		this.hiveJt = new JdbcTemplate();
		this.hiveJt.setDataSource(dataSource1);
		
		
		//初始化MySql连接配置
		BasicDataSource mysqlDS = new BasicDataSource();
		mysqlDS.setDriverClassName("com.mysql.jdbc.Driver");
//		mysqlDS.setUrl("jdbc:mysql://localhost:3306/ad_guide?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull");
//		mysqlDS.setUsername("root");
//		mysqlDS.setPassword("root123");	
		
		mysqlDS.setUrl("jdbc:mysql://192.168.2.2:3306/super_ssp?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull");
		mysqlDS.setUsername("root");
		mysqlDS.setPassword("mysql*()");		
		this.mysqlJt = new JdbcTemplate();
		this.mysqlJt.setDataSource(mysqlDS);
		
//		initDict();
		//初始化广协ip地址库字典表
//		initIpdatabaseDict();

//        initIpdatabaseDictNew();

        initAreaNameDict();
        initAreaInfo();
	}

    public void initAreaNameDict() {
        //初始化各个字典表
        System.out.println("----------初始化区域名称字典表开始-------------");

        //加载mysql字典表
        //省份
        proMap = new HashMap<String, String>();
        String provinceSql = "SELECT area_code,area_name FROM `area` t  WHERE t.`area_type` = 1";
        List provinceResult = mysqlJt.queryForList(provinceSql);
        for (int i = 0; i < provinceResult.size(); i++) {
            Map map = (Map) provinceResult.get(i);
            String name = (String) map.get("area_name");
            String code = (String) map.get("area_code");
            proMap.put(code, name);
        }


        System.out.println("省份字典表:"+proMap.toString());

        //地市
        cityMap = new HashMap<String,String>();
        String citySql = "SELECT area_code,area_name FROM `area` t  WHERE t.`area_type` = 2";
        List cityResult =  mysqlJt.queryForList(citySql);
        for (int i = 0; i < cityResult.size(); i++) {
            Map map = (Map)cityResult.get(i);
            String name  = (String) map.get("area_name");
            String code  = (String) map.get("area_code");
            cityMap.put(code,name);
        }

    }

    public void initDict(){
		//初始化各个字典表
		System.out.println("----------初始化字典表开始-------------");
		
		//加载mysql字典表
				//省份
			 proMap = new HashMap<String,String>();
				String provinceSql = "SELECT t.Name,t.Code FROM  ad_ip_province t ";
				List provinceResult =  mysqlJt.queryForList(provinceSql);
				 for (int i = 0; i < provinceResult.size(); i++) {
				      Map map = (Map)provinceResult.get(i);
				      String name  = (String) map.get("name");
				      String code  = (String) map.get("code");
				      proMap.put(code,name);
				 }
				 
				 System.out.println("省份字典表:"+proMap.toString());
				
				 //地市
			    cityMap = new HashMap<String,String>();
				String citySql = "SELECT t.Name,t.Code FROM  ad_ip_area t ";
				List cityResult =  mysqlJt.queryForList(citySql);
				 for (int i = 0; i < cityResult.size(); i++) {
				      Map map = (Map)cityResult.get(i);
				      String name  = (String) map.get("name");
				      String code  = (String) map.get("code");
				      cityMap.put(code,name);
				 }
			//省份地市关系字典表	 
			 proCityMap = new HashMap<String,String>();
					String proCitySql = "SELECT c.code ccode,c.`Name` cname,p.code pcode,p.`Name` pname FROM  ad_ip_area c,ad_ip_province p " +
							" WHERE c.`ProvinceID`=p.`ProvinceID`  ";
					List proCityResult =  mysqlJt.queryForList(proCitySql);
					 for (int i = 0; i < proCityResult.size(); i++) {
					      Map map = (Map)proCityResult.get(i);
					      String ccode  = (String) map.get("ccode");
					      String pcode  = (String) map.get("pcode");
					      proCityMap.put(ccode,pcode);
					 }
				 
				 //厂家 
					 vendorMap = new HashMap<String,String>();
					String vendorSql = "SELECT t.`ven_no`,t.`ven_name` FROM `adap_vendor` t ";
					List vendorResult =  mysqlJt.queryForList(vendorSql);
					 for (int i = 0; i < vendorResult.size(); i++) {
					      Map map = (Map)vendorResult.get(i);
					      String ven_no  = (Integer)map.get("ven_no")+"";
					      String ven_name  = (String) map.get("ven_name");
					      vendorMap.put(ven_no,ven_name);
					 }		
					
					 
				//节目 (注:广告联盟节目)
				amidMap = new HashMap<String,String>();
				String amidSql = "SELECT t.`amid`,t.`adname` FROM `adap_movie_info` t  ";
				List amidResult =  mysqlJt.queryForList(amidSql);
				 for (int i = 0; i < amidResult.size(); i++) {
				      Map map = (Map)amidResult.get(i);
				      String amid  = (Integer) map.get("amid")+"";
				      String adname  = (String) map.get("adname");
				      amidMap.put(amid,adname);
				 }
				 
				//广告位
				posMap = new HashMap<String,String>();
				String posSql = "SELECT guide_detailname NAME,ad_location_code CODE FROM ad_guide_detail_loc";
				List posResult =  mysqlJt.queryForList(posSql);
				 for (int i = 0; i < posResult.size(); i++) {
				      Map map = (Map)posResult.get(i);
				      String name  = (String) map.get("name");
				      String code  = (Integer) map.get("code")+"";
				      posMap.put(code,name);
				 }
				 	 
			
				 //频道字典表
				 channelMap = new HashMap<String,String>();
				String channelSql = "SELECT t.adChannelCode,t.adChannelName FROM ad_channel t WHERE adstatus =1";
				List channelResult =  mysqlJt.queryForList(channelSql);
				 for (int i = 0; i < channelResult.size(); i++) {
				      Map map = (Map)channelResult.get(i);
				      String name  = (String) map.get("adChannelName");
				      String code  = (Integer) map.get("adChannelCode")+"";
				      channelMap.put(code,name);
				 }
				 
				 //oemid
			    oemMap = new HashMap<String,String>();
					String oemSql = "SELECT oemid,oem_name FROM ad_oemid";
					List oemResult =  mysqlJt.queryForList(oemSql);
					 for (int i = 0; i < oemResult.size(); i++) {
					      Map map = (Map)oemResult.get(i);
					      String oemid  = (String) map.get("oemid");
					      String oem_name  = map.get("oem_name")==null?"":map.get("oem_name").toString();
					      oemMap.put(oemid,oem_name);
					 }
				 
				 
				 //查询出OEMID和EPG关系。
/*				 oemidEpgMap = new HashMap<String,String>();
					String oemidEpgSql = "SELECT t.`oemid`,t.`epgid` FROM oem_epg_dic t  ";
					List oemidEpgResult =  mysqlJt.queryForList(oemidEpgSql);
					 for (int i = 0; i < oemidEpgResult.size(); i++) {
					      Map map = (Map)oemidEpgResult.get(i);
					      String oemid  = (Integer) map.get("oemid")+"";
					      String epgid  = (Integer) map.get("epgid")+"";
					      oemidEpgMap.put(oemid,epgid);
					 }
					 
					 System.out.println("oemidEpgMap  = :" + oemidEpgMap.toString());*/
					 
				 //查询 EPG编号和名称字典。
			/*		 epgDicMap = new HashMap<String,String>();
					String epgSql = "SELECT t.`epgid`,t.portalname FROM oem_epg_dic t  ";
					List epgResult =  mysqlJt.queryForList(epgSql);
					 for (int i = 0; i < epgResult.size(); i++) {
					      Map map = (Map)epgResult.get(i);
					      String epgid  = (Integer) map.get("epgid")+"";
					      String portalname  = (String) map.get("portalname");
					      portalname = portalname==null?"":portalname.trim();
					      epgDicMap.put(epgid,portalname);
					}
					 
					 System.out.println("epgDicMap  = :" + epgDicMap.toString());*/
				
				//广告节目字典表
					 /* adMap = new HashMap<String,String>();
						String adSql = "SELECT t.amid,t.adname FROM `guide_movieinfo` t  ";
						List adResult =  mysqlJt.queryForList(adSql);
						 for (int i = 0; i < adResult.size(); i++) {
						      Map map = (Map)adResult.get(i);
						      String amid  = (Integer) map.get("amid")+"";
						      String adname  = (String) map.get("adname");
						      adMap.put(amid,adname);
						}
						 
					 System.out.println("adMap  = :" + adMap.toString());*/
				//广告节目时长字典表
					/*	adLenMap = new HashMap<String,Integer>();
							String adLenSql = "SELECT t.amid,t.length FROM `guide_movieinfo` t where TYPE=1 ";
							List adLenResult =  mysqlJt.queryForList(adLenSql);
							 for (int i = 0; i < adLenResult.size(); i++) {
							      Map map = (Map)adLenResult.get(i);
							      String amid  = (Integer) map.get("amid")+"";
							      Integer length  = (Integer) map.get("length");
							      adLenMap.put(amid,length);
							}
					 System.out.println("adLenMap  = :" + adLenMap.toString());*/
				 
		
		System.out.println("----------初始化字典表结束-------------");
	}
	



    /**
     * 缓存广协ip地址库数据
     */


    public void initAreaInfo() {
        logger.info("AreaInfoReCache doRefresh start....");
//        logger.debug("where is Range.class : "+ClassLocationUtils.where(Range.class));

        final AtomicLong at=new AtomicLong(0l);


        String mysql_sql = "select a.`parent_id` provinceid,a.`area_code` cityid,p.netseg1dec,p.netseg2dec\n" +
                " from  ad_ip_topology p,area a\n" +
                " where a.`area_type`=2 and p.adcode = a.`area_code`\t";
        try {
            List<Map<String, Object>> areaMapperList = mysqlJt.queryForList(mysql_sql);

            rangeMap.clear();

            for (Map<String, Object> map : areaMapperList) {

//				logger.debug("mapdata is : "+map);

                String[] data = { map.get("provinceid")+"",
                        map.get("cityid")+"" };
                double netseg1dec =  (Double)map.get("netseg1dec");
                double netseg2dec = (Double)map.get("netseg2dec");
                if (netseg1dec <= netseg2dec) {
                    rangeMap.put(Range.closed(netseg1dec, netseg2dec), data);
                }

                at.getAndIncrement();
            }

            logger.info("加载广协IP地址库【"+at.get()+"】条!");
        } catch (Exception e) {
            logger.error("加载ip区域异常", e);
            return;
        }


        logger.info("AreaInfoReCache doRefresh end....");

    }


    /**
     * 根据厂家和IP获取地域Id
     * @param ip
     * @return Integer[] data, data[0] : 省份code， data[1]:地市code
     */
    @Test
    public void testGetAreaInfo() {
        String[] areaData = {"0","0"};
       /* if(org.apache.commons.lang.StringUtils.isBlank(ip)){
            logger.warn("IP IS NULL");
            return areaData;
        }*/
        String ip="49.220.221.208";
        double longIp = Double.valueOf(ipToDecimal(ip)+"");

        try {
            areaData = rangeMap.get(longIp);
        } catch (Exception e) {
            logger.warn("get ip area error:", e);
        }

        if(null==areaData){
            areaData = new String[2];
            areaData[0]="0";
            areaData[1]="0";
        }
        String proName = proMap.get(areaData[0]);
        String cityName = cityMap.get(areaData[1]);
        System.out.println("解析IP["+ip+"]的区域信息为【"+areaData[0]+" "+proName+","+areaData[1]+" "+cityName+"】");

    }



	



	
	/**
	 * 将Result数据写到本地文件
	 * @param result
	 */
	private void writeResultNoGetKey(List result){
		System.out.println("===========================开始写文件==========================");
		//写文件
		File file = new File("e:\\work\\tmp\\anli_1.txt");
		FileWriter fw = null;
		BufferedWriter bw = null;
		try {
			fw = new FileWriter(file,true);//true 追加写 
			bw = new BufferedWriter (fw);  
			 StringBuilder sbTitle =  new StringBuilder();;
			for (int j = 0; j < result.size(); j++) {
			      Map map = (Map)result.get(j);
			     //System.out.println(map.toString());
			      String line = map.toString();
				//处理数据			      
			  	Pattern p = Pattern.compile("\\w+\\.(\\w+)=(.*?)[,|}]"); 
		        Matcher m = p.matcher(line);
		        StringBuilder sbLine = new StringBuilder();
		        while (m.find()) {
		        	if(j==0){
		        		String s1 = m.group(1);
		        		 sbTitle.append(s1).append(",");
		        	}
		        	
		            String s2 = m.group(2);
//		            System.out.println(s0);
		            sbLine.append(s2).append(",");
		        }
//		        System.out.println(sbLine.toString()); 
		        if(j==0){
		        	System.out.println(line);
		        	System.out.println(sbLine.toString());
		        	//标题头
		        	bw.write(sbTitle.toString());
					bw.newLine();
		        }
		        
			      //写入一条数据
				bw.write(sbLine.toString());
				bw.newLine();
			 }
			
			try {
				bw.flush();
				bw.close();
				fw.close();
			} catch (IOException e1) {
				System.err.println("写文件出错："+e1);
			}
		} catch (IOException e) {
			System.err.println("写文件出错2："+e);
			try {
	        	
	        	if(bw!=null){
					bw.close();
	        		bw = null;
	        	}
				if(fw!=null){
					fw.close();
					fw = null;
				}
    	
	    	} catch (IOException ex) {
				System.err.println("关闭流出错！"+ex);
			}
		}
		
		System.out.println("===========================写文件结束==========================");
	}
	
	
	/**
	 * 将Result数据写到本地文件
	 * @param result
	 */
	private void writeResultAndKey(List result){
		System.out.println("===========================开始写文件==========================");
		//写文件
		File file = new File("e:\\work\\tmp\\anli_7.txt");
		FileWriter fw = null;
		BufferedWriter bw = null;
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
		try {
			fw = new FileWriter(file);//true 追加写 
			bw = new BufferedWriter (fw);  
			for (int j = 0; j < result.size(); j++) {
			    Map map = (Map)result.get(j);
			    
			    //tmp 增加时间处理
			    String ts = (Long) map.get("ts")+"";
			      long time = Long.valueOf(ts+"000");
			     String stime = format.format(new Date(time));
			    
				bw.write(map.toString()+",stime="+stime);
				bw.newLine();
			 }			
			try {
				bw.flush();
				bw.close();
				fw.close();
			} catch (IOException e1) {
				System.err.println("写文件出错："+e1);
			}
		} catch (IOException e) {
			System.err.println("写文件出错2："+e);
			try {
	        	
	        	if(bw!=null){
					bw.close();
	        		bw = null;
	        	}
				if(fw!=null){
					fw.close();
					fw = null;
				}
    	
	    	} catch (IOException ex) {
				System.err.println("关闭流出错！"+ex);
			}
		}
		
		System.out.println("===========================写文件结束==========================");
	}

	
	/**
	 * 将Result数据写到本地文件
	 * @param result
	 */
	private void writeResult(List result){
		System.out.println("===========================开始写文件==========================");
		//定义写入字段顺序
		String [] outPutTitle = {"day","areaparentid","exp1_3","exp4"};
//		day,areaparentid,sum(case when allnum<=3 then allnum else 0 end) exp1_3, sum(case when allnum>=4 then allnum else 0 end) exp4
		//写文件
		File file = new File("e:\\work\\tmp\\anlikaiji.txt");
		FileWriter fw = null;
		BufferedWriter bw = null;
		try {
			fw = new FileWriter(file,true);//true 追加写 
			bw = new BufferedWriter (fw);  
			for (int j = 0; j < result.size(); j++) {
			      Map map = (Map)result.get(j);
			      
			      System.out.println(map.toString());
			      
			      String line ="";
					//按指定表头写入文件
					for(String oneTitle:outPutTitle){
						String  str="";
						try {
							str = String.valueOf(map.get(oneTitle));
							if(null==str||"null".equals(str)){
								str="";
							}
						} catch (Exception exp) {
							str="";
						}
						//省份特殊处理
						if(oneTitle.equals("areaparentid")){
							str = proMap.get(str);
						}
						line+=str+",";
					}
					line=line.substring(0,line.length()-1);
					//写入一条数据
					bw.write(line);
					bw.newLine();
			 }
			
			try {
				bw.flush();
				bw.close();
				fw.close();
			} catch (IOException e1) {
				System.err.println("写文件出错："+e1);
			}
		} catch (IOException e) {
			System.err.println("写文件出错2："+e);
			try {
	        	
	        	if(bw!=null){
					bw.close();
	        		bw = null;
	        	}
				if(fw!=null){
					fw.close();
					fw = null;
				}
    	
	    	} catch (IOException ex) {
				System.err.println("关闭流出错！"+ex);
			}
		}
		
		System.out.println("===========================写文件结束==========================");
	}
	
	
	
	/**
	 * 解析文本数据
	 */
	 
	@Test
	public void readFile() {
		
		HashMap<String,HashMap<String,Long>> fw4Map =  new HashMap<String,HashMap<String,Long>>();
		HashMap<String,HashMap<String,Long>> miaozhenMap =  new HashMap<String,HashMap<String,Long>>();
		
		File theFile = new File("/opt/data/adstat/file/lianmeng.txt");
		try {
			LineIterator it = FileUtils.lineIterator(theFile, "UTF-8");
			try {
			    while (it.hasNext()) {
			    	String line = it.nextLine();
			    	//数据样例
			    	//1722 ./fw4.co/1050000442/20161205.txt
					try {
						if(line.startsWith("wc")){
				    		continue;
				    	}
						
						String num = StringUtils.trim(StringUtils.substringBefore(line, "./"));
						
						String str = StringUtils.substringAfter(line, "./");
						String [] array = str.split("/");
						if(array.length<3){
							continue;
						}
						String venName = array[0];
						String amid = array[1];
						String day = array[2].replaceAll(".txt", "");
						
						if("fw4.co".equals(venName)){
							if(fw4Map.get(amid)!=null){
								HashMap<String,Long> amidMap = fw4Map.get(amid);
								if(amidMap.get(day)!=null){
									Long value = amidMap.get(day);
									value = value+Long.valueOf(num);
									amidMap.put(day, value);
								}else{
									amidMap.put(day, Long.valueOf(num));
								}
							}else{
								HashMap<String,Long> amidMap = new HashMap<String,Long>();
								amidMap.put(day, Long.valueOf(num));
								fw4Map.put(amid, amidMap);
							}
						}else if("g.dtv.cn.miaozhen".equals(venName)){
							if(miaozhenMap.get(amid)!=null){
								HashMap<String,Long> amidMap = miaozhenMap.get(amid);
								if(amidMap.get(day)!=null){
									Long value = amidMap.get(day);
									value = value+Long.valueOf(num);
									amidMap.put(day, value);
								}else{
									amidMap.put(day, Long.valueOf(num));
								}
							}else{
								HashMap<String,Long> amidMap = new HashMap<String,Long>();
								amidMap.put(day, Long.valueOf(num));
								miaozhenMap.put(amid, amidMap);
							}
						}
											
					} catch (Exception e) {
						System.err.println(e);
					
					}
					
			    }
	    
				} finally {
				    LineIterator.closeQuietly(it);
				}
		//打印Map数据
			Iterator<String> fwit = fw4Map.keySet().iterator();
			while(fwit.hasNext()){
				String amid = fwit.next();
				HashMap<String,Long> dayMap = fw4Map.get(amid);
				//循环日期
				Iterator<String> dayIt = dayMap.keySet().iterator();
				while(dayIt.hasNext()){
					String day = (String) dayIt.next();
					
					long value = dayMap.get(day);
					
					String admidName = amidMap.get(amid);
					
					String outLine = "admaster,"+admidName+","+amid+","+day+","+value;
					
					System.out.println(outLine);
				}
			}
			
			//miaozhen
			Iterator<String> miaozhenIt = miaozhenMap.keySet().iterator();
			while(miaozhenIt.hasNext()){
				String amid = miaozhenIt.next();
				HashMap<String,Long> dayMap = miaozhenMap.get(amid);
				//循环日期
				Iterator<String> dayIt = dayMap.keySet().iterator();
				while(dayIt.hasNext()){
					String day = (String) dayIt.next();
					
					long value = dayMap.get(day);
					
					String admidName = amidMap.get(amid);
					
					String outLine = "miaozhen,"+admidName+","+amid+","+day+","+value;
					
					System.out.println(outLine);
				}
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	/**
	 * 将IP转化为十进制
	 * @param ip
	 * @return
	 */
	private  long ipToDecimal(String ip) {
		long ipDec = 0;
		if (ip != null) {
			String[] ipArr = ip.split("\\.");
			if (ipArr != null && ipArr.length == 4) {
				for (int i = 3; i >= 0; i--) {
					ipDec += (Long.valueOf(ipArr[i].trim()) * Math.pow(256, 3 - i));
				}
			}
		}
		return ipDec;
	}
	 
	
}

