package hive;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

/**
 * 奥运期间广告联盟查询测试
 * @author shaoyl
 *
 */
public class AdapOlympicTest {
	
	private JdbcTemplate hiveJt;// 连接池
	private JdbcTemplate mysqlJt;// 连接池
	
	Map<String,String> proMap = null;
	Map<String,String> cityMap = null;
	Map<String,String>  oemMap = null; 
	Map<String,String>  posMap = null;
	
	 Map<String,String>  oemidEpgMap = null;
	 Map<String,String>  epgDicMap = null;
	 Map<String,String>  adMap = null;
	 Map<String,Integer>  adLenMap = null;

	public AdapOlympicTest(){
		BasicDataSource dataSource1 = new BasicDataSource();
		dataSource1.setDriverClassName("org.apache.hive.jdbc.HiveDriver");
		dataSource1.setUrl("jdbc:hive2://60.29.252.4:10000/default");
//		dataSource1.setUrl("jdbc:hive2://172.16.10.95:10000/default");
		dataSource1.setUsername("statuser");
		dataSource1.setPassword("statuser*()");		
		this.hiveJt = new JdbcTemplate();
		this.hiveJt.setDataSource(dataSource1);
		
		
		//初始化MySql连接配置
		BasicDataSource mysqlDS = new BasicDataSource();
		mysqlDS.setDriverClassName("com.mysql.jdbc.Driver");
		mysqlDS.setUrl("jdbc:mysql://localhost:3306/ad_guide?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull");
		mysqlDS.setUsername("root");
		mysqlDS.setPassword("root123");		
		this.mysqlJt = new JdbcTemplate();
		this.mysqlJt.setDataSource(mysqlDS);
		
		initDict();
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
				 
				 //厂家 
					/*Map<String,String>  vendorMap = new HashMap<String,String>();
					String vendorSql = "SELECT t.`ven_no`,t.`ven_name` FROM `adap_vendor` t ";
					List vendorResult =  mysqlJt.queryForList(vendorSql);
					 for (int i = 0; i < vendorResult.size(); i++) {
					      Map map = (Map)vendorResult.get(i);
					      String ven_no  = (Integer)map.get("ven_no")+"";
					      String ven_name  = (String) map.get("ven_name");
					      vendorMap.put(ven_no,ven_name);
					 }		*/
					
					 
				//节目 (注:广告联盟节目)
				/* Map<String,String>  amidMap = new HashMap<String,String>();
				String amidSql = "SELECT t.`amid`,t.`adname` FROM `adap_movie_info` t  ";
				List amidResult =  mysqlJt.queryForList(amidSql);
				 for (int i = 0; i < amidResult.size(); i++) {
				      Map map = (Map)amidResult.get(i);
				      String amid  = (Integer) map.get("amid")+"";
				      String adname  = (String) map.get("adname");
				      amidMap.put(amid,adname);
				 }*/
				 
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
				/*Map<String,String>  channelMap = new HashMap<String,String>();
				String channelSql = "SELECT t.adChannelCode,t.adChannelName FROM ad_channel t WHERE adstatus =1";
				List channelResult =  mysqlJt.queryForList(channelSql);
				 for (int i = 0; i < channelResult.size(); i++) {
				      Map map = (Map)channelResult.get(i);
				      String name  = (String) map.get("adChannelName");
				      String code  = (Integer) map.get("adChannelCode")+"";
				      channelMap.put(code,name);
				 }
				 */
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
	 * 按厂家节目id分日查看数据
	 */
	@Test
	public void aggreVenAmidDay(){
		String sql = "SELECT   DAY,  k_record.ven,  k_record.amid, k_record.pos,  SUM(1),  COUNT(DISTINCT (k_record.hid)) usernum" +
				" FROM  kafka.kafka_adap_adst_0 WHERE DAY = '2016-06-14' and k_record.te=0" +
				" GROUP BY DAY,  k_record.amid,  k_record.ven,k_record.pos  " +
				" order by DAY,k_record.ven,k_record.amid ,k_record.pos ";
		this.hiveJt.query(sql, new RowMapper<Object>(){
			@Override
			public Object mapRow(ResultSet rs, int rownum) throws SQLException {
				System.out.println(rs.getString(1)+","+rs.getString(2)+","+rs.getString(3)+","+rs.getString(4)+","+rs.getString(5)+","+rs.getString(6));
				return null;
			}
		});
	}
	
	/**
	 * 分节目，分区域汇总(不分pos)
	 */
	@Test
	public void aggreVenAmidAreaDay(){
		String sql = "SELECT   DAY,  ven,  amid, areaid," +
				"  SUM(1),  COUNT(DISTINCT (hid)) usernum" +
				" from (select day,  k_record.ven,  k_record.amid, k_record.pos ,getcityid_01(k_record.ip) areaid,k_record.hid" +
				" FROM  kafka.kafka_adap_adst_0 WHERE " +
				" k_record.ven=900108 and k_record.te=0" +
				" ) tt" +
				" GROUP BY DAY,  k_record.amid,  k_record.ven,areaid  " +
				" order by DAY,k_record.ven,k_record.amid,areaid";
		this.hiveJt.query(sql, new RowMapper<Object>(){
			@Override
			public Object mapRow(ResultSet rs, int rownum) throws SQLException {
				System.out.println(rs.getString(1)+","+rs.getString(2)+","+rs.getString(3)+","+rs.getString(4)+","+rs.getString(5)+","+rs.getString(6));
				return null;
			}
		});
	}
	
	/**
	 * 20160628 分区域总用户数汇总(不分pos)
	 */
	@Test
	public void aggreAmidAreaDay(){
		String sql = "SELECT  amid, areaid," +
				"  SUM(1),  COUNT(DISTINCT (hid)) usernum" +
				" from (select day,  k_record.ven,  k_record.amid, k_record.pos ,getcityid_01(k_record.ip) areaid,k_record.hid" +
				" FROM  kafka.kafka_adap_adst_0 WHERE " +
				" k_record.ven=900108 and k_record.te=0 " +
				" ) tt" +
				" GROUP  by tt.amid, areaid  " +
				" order by tt.amid,areaid";
		this.hiveJt.query(sql, new RowMapper<Object>(){
			@Override
			public Object mapRow(ResultSet rs, int rownum) throws SQLException {
				System.out.println(rs.getString(1)+","+rs.getString(2)+","+rs.getString(3)+","+rs.getString(4)+","+rs.getString(5)+","+rs.getString(6));
				return null;
			}
		});
	}
	
	
	@Test
	public void aggreVenAmidAreaDayNoGroupby(){
		String sql = "SELECT   DAY,  k_record.ven,  k_record.amid, k_record.pos,getcityid_01(k_record.ip) areaid,k_record.hid" +
				" FROM  kafka.kafka_adap_adst_0 WHERE DAY = '2016-06-14' and k_record.ven=900108 and k_record.te=0 limit 50";
		this.hiveJt.query(sql, new RowMapper<Object>(){
			@Override
			public Object mapRow(ResultSet rs, int rownum) throws SQLException {
				System.out.println(rs.getString(1)+","+rs.getString(2)+","+rs.getString(3)+","+rs.getString(4)+","+rs.getString(5)+","+rs.getString(6)+","+rs.getString(7));
				return null;
			}
		});
		System.out.println("done");
	}
	/**
	 * 输出打印共耗时（1214000
		1215000
		done, 耗时343117毫秒!）
	 * @throws Exception
	 */
	@Test
	public void aggreVenAmidAreaDayNoGroupbyWriteFile() throws Exception{
		final AtomicLong at=new AtomicLong(0l);
		File f = new File("e:\\tmp\\e.txt");
		FileWriter fw = new FileWriter(f, true);
		final BufferedWriter bw = new BufferedWriter(fw);
		String sql = "SELECT   DAY,  k_record.ven,  k_record.amid, k_record.pos,getcityid_01(k_record.ip) areaid,k_record.hid" +
				" FROM  kafka.kafka_adap_adst_0 WHERE DAY = '2016-06-14' and k_record.te=0";
		long start = System.currentTimeMillis();
		this.hiveJt.query(sql, new RowMapper<Object>(){
			@Override
			public Object mapRow(ResultSet rs, int rownum) throws SQLException {
				String day = rs.getString(1);
				String ven = rs.getString(2);
				String amid = rs.getString(3);
				String pos = rs.getString(4);
				String areaid = rs.getString(5);
				String hid = rs.getString(6);
				try {
					bw.write(day + "|" + ven + "|" + amid+"|"+pos+"|"+areaid+"|"+hid);
					bw.newLine();
					
					if(at.incrementAndGet()%1000==0){
						System.out.println(at.get());
						bw.flush();
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
				return null;
			}
		});
		bw.flush();
		bw.close();
		long end = System.currentTimeMillis();
		System.out.println("done, 耗时"+(end-start)+"毫秒!");
		
	}
	
	
	
	/**
	 * 输出打印共耗时（1214000
		1215000
		done, 耗时343117毫秒!）
	 * @throws Exception
	 */
	@Test
	public void aggreMangGuoProvince0613_0619() throws Exception{
		final AtomicLong at=new AtomicLong(0l);
		File f = new File("e:\\tmp\\aggreMangGuoProvince0613_0619.txt");
		FileWriter fw = new FileWriter(f, true);
		final BufferedWriter bw = new BufferedWriter(fw);
		String sql = "SELECT   DAY,  k_record.ven, k_record.amid, k_record.pos,getcityid_01(k_record.ip) areaid,k_record.hid" +
				" FROM  kafka.kafka_adap_adst_0 WHERE DAY >= '2016-06-13' and day<='2016-06-19' and k_record.ven=900108 and k_record.te=0";
		long start = System.currentTimeMillis();
		this.hiveJt.query(sql, new RowMapper<Object>(){
			@Override
			public Object mapRow(ResultSet rs, int rownum) throws SQLException {
				String day = rs.getString(1);
				String ven = rs.getString(2);
				String amid = rs.getString(3);
				String pos = rs.getString(4);
				String areaid = rs.getString(5);
				String hid = rs.getString(6);
				try {
					bw.write(day + "|" + ven + "|" + amid+"|"+pos+"|"+areaid+"|"+hid);
					bw.newLine();
					
					if(at.incrementAndGet()%100000==0){
						System.out.println(at.get());
						bw.flush();
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
				return null;
			}
		});
		bw.flush();
		bw.close();
		long end = System.currentTimeMillis();
		System.out.println("done, 耗时"+(end-start)+"毫秒!");
		
	}
	
	
	/**************************2016-07-12**********************
	 * 
	 * 
	 */
	
	/**
	 * 分厂家分节目查询奥运广告联盟
	 */
	public void aggreVenAmid(){
		String sql = "select ven,amid,count(1) allnum,count(distinct hid) usernum " +
				" from kafka_adap_adst_0  group by ven,amid" +
				" order by ven,amid ";
		
		long start = System.currentTimeMillis();
		List result =  this.hiveJt.queryForList(sql);		 
		 long end = System.currentTimeMillis();
		 System.out.println("查询耗时["+(end-start)+"]毫秒");
		 for (int j = 0; j < result.size(); j++) {
		      Map map = (Map)result.get(j);
		      System.out.println(map.toString());
		 }

		System.out.println("done");
	}
	
	
	
	/********************查询春节广告播放****************************/
	
	@Test
	public void getVenAmidDay(){
		String sql = "select sum(1) allsum,count(distinct(hid)) as usernum,ven,amid " +
				" from  fact_adst_history where day='2016-06-22' and ven=0 and amid = 2016041201";
		this.hiveJt.query(sql, new RowMapper<Object>(){
			@Override
			public Object mapRow(ResultSet rs, int rownum) throws SQLException {
				System.out.println(rs.getString(1)+","+rs.getString(2)+","+rs.getString(3)+","+rs.getString(4));
				return null;
			}
		});
	}
	
	
	@Test
	public void getHaixinJilieDay(){
		String sql = "select day,hour,count(1) allsum,count(distinct(hid)) as usernum" +
				" from  fact_adst_history where day>='2016-06-22' and day<='2016-06-24' " +
				" and ven=900101 and amid=2016060801" + 
				" group by day,hour";
		this.hiveJt.query(sql, new RowMapper<Object>(){
			@Override
			public Object mapRow(ResultSet rs, int rownum) throws SQLException {
				System.out.println(rs.getString(1)+","+rs.getString(2)+","+rs.getString(3)+","+rs.getString(4));
				return null;
			}
		});
		
		System.out.println("----done----");
	}

	@Test
	public void getTest(){
		String sql = "select getcityid_01('1.0.8.0') ";
		this.hiveJt.query(sql, new RowMapper<Object>(){
			@Override
			public Object mapRow(ResultSet rs, int rownum) throws SQLException {
				System.out.println(rs.getString(1));
				return null;
			}
		});
		System.out.println("done");
	}
	
	
	
	/**************************2016-07-12**********************
	 * 广告联盟
	 * 
	 */
	
	/**
	 * 查询媒体资源方开机广告测试分地市数据
	 */
	@Test
	public void getMedieCityData() {

		long start = System.currentTimeMillis();

		String sql = "select amid,areaparentid,areaid,count(1) allnum, COUNT(DISTINCT (hid)) usernum" +
				" from fact_adap_adst_history t" +
				" where DAY = '2016-08-31' and t.ven = 900110 and t.amid in (2016082901,2016082902) group by amid,areaparentid,areaid";

		System.out.println("hivesql is : " + sql);

		List result = hiveJt.queryForList(sql);
		
		long end = System.currentTimeMillis();
		System.out.println("查询耗时[" + (end - start) + "]毫秒");
		
//		printResult(result);	
		
		System.out.println("result.size()=="+result.size());
		
		for (int j = 0; j < result.size(); j++) {
		      Map map = (Map)result.get(j);
		      
		      try {
				String amid = (Long) map.get("amid")+"";
				  String areaparentid = (Long) map.get("areaparentid")+"";
				  String allnum =  map.get("allnum")==null?"":(Long)map.get("allnum")+"";
				  String usernum = map.get("usernum")==null?"":(Long)map.get("usernum")+"";
				  String proName = (null==proMap.get(areaparentid))?"":proMap.get(areaparentid);
				  
				  //分地市
				  String areaid =  (Long) map.get("areaid")+"";
				  String areaName = cityMap.get(areaid)==null?cityMap.get(areaid):cityMap.get(areaid);
				  
				  System.out.println(amid+","+proName+","+areaName+","+allnum+","+usernum);
			} catch (Exception e) {
				System.err.println("错误信息:"+map.toString());
				e.printStackTrace();
			}
		      
		}

		System.out.println("done");
	}

	/**
	 * 查询江苏itv没有落在江苏的IP地址
	 */
	@Test
	public void getIpNoJiangsu() {

		long start = System.currentTimeMillis();

		String sql = "select distinct ip from fact_adap_adst_history t where t.ven = 900109 and t.areaparentid=0";

		System.out.println("hivesql is : " + sql);

		List result = hiveJt.queryForList(sql);
		
		long end = System.currentTimeMillis();
		System.out.println("查询耗时[" + (end - start) + "]毫秒");
		
		printResult(result);		

		System.out.println("done");
	}
	/**
	 * 终端统计
	 */
	@Test
	public void oemid() {

		long start = System.currentTimeMillis();

		String sql = "select oemid,sum(expsum) as exposure,sum(case when usersum >=1 then 1 else 0 end) as touchone" +
				"  from  (select hid,oemid,count(distinct sessionid) usersum,sum(expsum) expsum" +
				" from (select hid,oemid,adposid,sessionid,1 as expsum" +
				" from adstat where type=0 and uv=0 and logdate >= '20160701' and logdate <= '20160731' and adposid in (15101010)  and oemid in (40005,40006,10008,49006)  and amid != 0 and pv = 1  group by hid,oemid,adposid,sessionid) as tmp" +
				" group by hid,oemid) as temp1  group by oemid";

		System.out.println("hivesql is : " + sql);

		List result = hiveJt.queryForList(sql);
		
		long end = System.currentTimeMillis();
		System.out.println("查询耗时[" + (end - start) + "]毫秒");
		
		printResult(result);		

		System.out.println("done");
	}
	
	
	/**
	 * 定制表表汇总有节目
	 */
	@Test
	public void customSumAmid() {

		long start = System.currentTimeMillis();

		String sql = "select amid,sum(exposure) as exposure,	 sum(case when exposure >=1 then 1 else 0 end) as touchone,	" +
				"  from 	 (select amid,hid,count(distinct sessionid) as exposure 	" +
				"	from adstat where type=0 		and logdate = 20160805	and uv = 0 and amid = 1050000020" +
				"	group by hid,amid) as temp1 group by amid";

		System.out.println("hivesql is : " + sql);

		List result = null;
		try {
			result = hiveJt.queryForList(sql);
		} catch (DataAccessException e) {
			e.printStackTrace();
		}
		
		long end = System.currentTimeMillis();
		System.out.println("查询耗时[" + (end - start) + "]毫秒");
		
		printResult(result);		

		System.out.println("done");
	}
	
	
	@Test
	public void customAreaAdpos() {

		long start = System.currentTimeMillis();

		String sql = " SELECT hid,sessionid,COUNT(1) AS  exposure " +
				"	FROM ( " +
				" SELECT provinceid,hid,sessionid,1 AS exposure" +
				" FROM adstat " +
				" WHERE TYPE=0 " +
				" AND pv=1 " +
				" AND logdate = 20160801" +
				" AND adposid = 17101110 " + 
				" AND uv = 0  " +
				" AND oemid IN (126,158,176,177,178,180,222,226,227,229,273,276,285,296,297,309,321,323,329,332,337,342,374,380,384,397,414,415,416,425,440,445,446,541,544,546,547,559,566,593,597,598,599,602,610,615,616,617,620,627,636,637,638,639,640,641,644,646,655,658,665,667,669,670,673,674,676,677,680,682,686,690,691,694,698,699,705,706,708,726,729,737,738,739,741,745,746,747,750,757,760,763,764,768,769,772,773,777,778,779,780,784,785,786,788,789,791,792,796,799,807,808,809,814,816,818,819,821,822,828,829,831,833,834,838,839,841,842,843,848,849,851,852,853,854,861,868,869,870,875,877,878,880,881,882,883,889,890,891,892,893,894,895,896,898,100002,100005,100006,100007,100008,100009,100010,100011,100012,100014,100015,100018,100019,100020,100021,100022,100023,100024,100025,100026,100027,100028,100029,100030,100031,100032,100033,100034,100035,100036,100037,100038,100039,100040,100042,100043,100044,100045,100046,100047,100048,100051,100052,100053,100054,100055,100056,100057,100058,100060,100062,100063,100064,100065,100066,100067,100068,100069,100070,100071,100072,100073,100075,100076,100077,100078,100079,100080,100082,100083,100085,100086,100088,100089,100090,100092,100095,100096,100097,100098,100099,100100,100102,100104,100108,100109,100114,100116,100117,100118,100119,100120,100121,100122,100123,100124,100125,100126,100127,100128,100129,100130,100131,100132,100136,100137,100138,100139,100140,100141,100142,100144,100146,100147,100148,100149,100150,100152,100153,100154,100155,100156,100157,100158,100160,100161,100162,100163,100164,100165,100166,100167,100168,100169,100170,100171,100172,100173,100174,100175,100176,100177,100178,100179,100180,100181,100182,100183,100184,100185,100186,100187,100188,100189,100190,100191,100192,100193,100194,100195,100196,100197,100198,100200,100203,100204,100206,100207,100208,100210,100211,100212,100213,100214,100215,100216,100217,100218,100219,100221,100222,150001,150002,150003,150004,150005,150006,150007,150008,150009,160001,160002,160003,160004,160005,160006,160007,160008,160009,160010,170001,170002,170003,170004,170005,170006,170007,180001,180002,180003,180004,180005,180006,180007,180008,200005,200006,200007,200008,200011,200012,200013,200015,200017,200018,200019,200023,200025,200027,200029,200030,200031,200032,200033,200035,200042,200043,200045,200046,200052,200053,200056,200061,200062,250001,300004,300062,400001,401000,500001,500002,500004,500005,500214,600001,725,300025,300136,300003,797,368,225,545,100199,100202,100223,100224,100225,100227,100233,100234,100237,100240,100241,100243,134,135,136,142,143,145,148,153,155,156,157,170008,160013,172,181,182,183,200002,200003,200004,200009,200010,200014,200020,200021,200022,200024,200026,200028,200034,200036,200040,200041,200055,200044,200048,200049,200050,200057,200058,200059,200060,200063,200064,200065,200068,200100,205,208,209,211,212,217,223,258,268,275,278,293,369,303,305,325,335,345,355,367,371,379,386,395,406,409,433,410,411,412,441,442,443,444,447,448,449,532,534,538,565,548,555,606,612,619,622,625,650,654,656,661,679,696,702,727,740,742,781,800002,800008,804,805,815,837,823,847,856,858,859,860,862,886,100074,840,100059,100061,100103,100115,100134,100151,531,681,776,100049)" +  
				" AND provinceid IN (23,12,11,10,24,25,26,27,40,41,42,43,44,45,60,61,62,63,64,65,80,81,82,83,84,95,96,97,98,99,46,01,00,66,67,04,06,05)" +
				"  GROUP BY provinceid,hid,sessionid" +
				"  ) tmp " +
				"  GROUP BY hid,sessionid" +
				"  HAVING COUNT(1)>1" ;

		System.out.println("hivesql is : " + sql);

		List result = null;
		try {
			result = hiveJt.queryForList(sql);
		} catch (DataAccessException e) {
			e.printStackTrace();
		}
		
		long end = System.currentTimeMillis();
		System.out.println("查询耗时[" + (end - start) + "]毫秒");
		
		printResult(result);		

		System.out.println("done");
	}
	/**
	 * 导流位查询所有，月查不出来问题
	 */
	@Test
	public void adposMonthByAdposid() {

		long start = System.currentTimeMillis();

		String sql = " SELECT    adposid,   SUM(exposure) AS exposure,  " +
				" SUM(     CASE       WHEN exposure >= 1        THEN 1        ELSE 0      END   ) AS touchone " +
				" FROM   (SELECT      hid,     adposid,     COUNT(DISTINCT sessionid) AS exposure" +
				"    FROM     adstat    WHERE TYPE = 0      AND uv = 0      AND logdate >= 20160701     AND logdate <= 20160731     AND pv = 1   " +
				" GROUP BY hid,     adposid) AS temp1  GROUP BY adposid  " ;

		System.out.println("hivesql is : " + sql);

		List result = null;
		try {
			result = hiveJt.queryForList(sql);
		} catch (DataAccessException e) {
			e.printStackTrace();
		}
		
		long end = System.currentTimeMillis();
		System.out.println("查询耗时[" + (end - start) + "]毫秒");
		
		printResult(result);		

		System.out.println("done");
	}
	
	
	
	@Test
	public void adposMonthSum() {

		long start = System.currentTimeMillis();

		String sql = " SELECT  SUM(    CASE    WHEN exposure >= 1       THEN 1       ELSE 0     END  ) AS touchone" +
				"  FROM  (SELECT     hid,    COUNT(DISTINCT sessionid) AS exposure   FROM    adstat   WHERE TYPE = 0   AND uv = 0   " +
				"  AND logdate = 20160701 " +
				"  AND adposid IN (15101010)     AND pv = 1   GROUP BY hid) tmp " ;

		System.out.println("hivesql is : " + sql);

		List result = null;
		try {
			result = hiveJt.queryForList(sql);
		} catch (DataAccessException e) {
			e.printStackTrace();
		}
		
		long end = System.currentTimeMillis();
		System.out.println("查询耗时[" + (end - start) + "]毫秒");
		
		printResult(result);		

		System.out.println("done");
	}
	
	
	@Test
	public void adposMonthSumNew() {

		long start = System.currentTimeMillis();

		String sql = " SELECT   COUNT(DISTINCT hid) AS exposure   FROM    adstat   WHERE TYPE = 0   AND uv = 0   " +
				"  AND logdate = 20160701 " +
				"  AND adposid IN (15101010)     AND pv = 1" ;

		System.out.println("hivesql is : " + sql);

		List result = null;
		try {
			result = hiveJt.queryForList(sql);
		} catch (DataAccessException e) {
			e.printStackTrace();
		}
		
		long end = System.currentTimeMillis();
		System.out.println("查询耗时[" + (end - start) + "]毫秒");
		
		printResult(result);		

		System.out.println("done");
	}
	
	/**
	 * 天籁前贴
	 */
	@Test
	public void aggreTianlaiQT() {

		long start = System.currentTimeMillis();

		String sql = " SELECT    amid,   SUM(exposure) AS exposure,   SUM(     CASE    " +
				"   WHEN exposure >= 1        THEN 1        ELSE 0      END   ) AS touchone" +
				" FROM   (SELECT      amid,     hid,     COUNT(DISTINCT sessionid) AS exposure" +
				"  FROM     adstat    WHERE  logdate = 20160805  AND amid = 1050000020    GROUP BY hid,     amid) AS temp1 " +
				" GROUP BY amid ";
		System.out.println("hivesql is : " + sql);

		List result = null;
		try {
			result = hiveJt.queryForList(sql);
		} catch (DataAccessException e) {
			e.printStackTrace();
		}
		
		long end = System.currentTimeMillis();
		System.out.println("查询耗时[" + (end - start) + "]毫秒");
		
		printResult(result);		

		System.out.println("done");
	}
	
	@Test
	public void countNotTianlai() {

		long start = System.currentTimeMillis();

		String sql = " select distinct hid	from adstat	where logdate = 20160807 and adposid = 17101410" +
				" 	and provinceid = 23	and amid not in (1050000021,0)	limit 200	" ;

		System.out.println("hivesql is : " + sql);

		List result = null;
		try {
			result = hiveJt.queryForList(sql);
		} catch (DataAccessException e) {
			e.printStackTrace();
		}
		
		long end = System.currentTimeMillis();
		System.out.println("查询耗时[" + (end - start) + "]毫秒");
		
		printResult(result);		

		System.out.println("done");
	}
	
	
	
	
	/**
	 * 多维度查询测试
	 */
			@Test
			public void multiDimTest() {

				long start = System.currentTimeMillis();

				/*String sql = "select logdate,timequantum,adposid,amid,channelid,provinceid,cityid,oemid," +
						" count(1) exp, count(distinct sessionid) touch" +
							" from adstat where logdate>= 20160801 and logdate<=20160801" +
							" group by logdate,timequantum,adposid,amid,channelid,provinceid,cityid,oemid limit 100" ;*/
				
	/*			String sql="select sum(expsum) expsum from ( select hid,adposid,sessionid,1 as expsum " +
						" from adstat where  logdate >= 20160701 and logdate <= 20160731" +
						" and adposid = 15101010 group by hid,adposid,sessionid ) tmp";*/

		/*		String sql = "select sum(expsum) expsum" +
						" from ( select hid,adposid,amid,count(distinct sessionid) as expsum " +
						" from adstat where  logdate >= 20160701 and logdate <= 20160731" +
						" and adposid = 15101010 group by hid,adposid,amid ) tmp";*/
				
				//触达效率比较
			/*	String sql = "select adposid,sum(expsum) expsum," +
						"sum(case when expsum>=1 then 1 else 0 end) touchone," +
						"sum(case when expsum>=2 then 1 else 0 end) touchtwo" +
						" from ( " +
						" select hid,adposid,count(distinct sessionid) as expsum " +
						" from adstat where  logdate >= 20160801 and logdate <= 20160807" +
						" and adposid in (15101010,17101410)" +
						 " group by hid,adposid" +
						" ) tmp" +
						" group by adposid";		*/		
				
				
				/*String sql = "select adposid,sum(expsum) expsum," +
						"count(case when expsum>=1 then 1 else null end) touchone," +
						"count(case when expsum>=2 then 1 else null end) touchtwo" +
						" from ( " +
						" select hid,adposid,count(distinct sessionid) as expsum " +
						" from adstat where  logdate >= 20160801 and logdate <= 20160807" +
						" and adposid in (15101010,17101410)" +
						 " group by hid,adposid" +
						" ) tmp" +
						" group by adposid";*/
				
				String sql = "select logdate,oemid,sum(expsum) expsum" +
						" from (" +
						" select logdate,hid,oemid,adposid,amid,count(distinct sessionid) as expsum " +
						" from adstat where  logdate >= 20160701 and logdate <= 20160821" +
						" and type=0 and uv=0" +
						" and amid<>0" +
						" and oemid in (0,1,3,4,5,25,26,27,28,29,30,100,101,102,103,104,105,106,108,109,111,112,114,116,118,119,120,121,122,123,124,125,126,130,131,132,133,134,135,136,138,140,141,142,143,145,146,147,148,149,150,153,154,155,156,157,158,160,165,167,169,170,172,176,177,178,179,180,181,182,183,200,201,202,205,207,208,209,210,211,212,215,216,217,218,220,221,222,223,225,226,227,229,230,231,232,235,236,237,239,240,241,242,243,244,245,246,249,250,251,256,257,258,260,261,262,264,268,270,271,272,273,274,275,276,278,280,281,282,283,285,288,289,290,291,292,293,294,296,297,299,300,301,302,303,304,305,307,309,310,312,313,314,315,316,318,319,320,321,322,323,324,325,326,327,328,329,330,331,332,333,334,335,336,337,338,339,340,341,342,343,344,345,346,347,348,350,351,352,355,356,357,358,359,360,361,365,367,368,369,370,371,372,374,379,380,384,386,393,394,395,396,397,400,401,402,403,404,405,406,407,409,410,411,412,414,415,416,417,418,419,420,421,422,425,427,428,429,430,431,432,433,434,440,441,442,443,444,445,446,447,448,449,450,451,452,453,454,455,456,457,458,459,460,461,462,463,464,465,469,470,471,472,477,489,490,491,492,493,494,495,496,497,498,500,501,502,503,504,505,506,507,508,509,510,511,512,513,514,515,516,517,519,520,521,522,523,531,532,534,535,537,538,539,540,541,542,543,544,545,546,547,548,549,550,551,552,553,554,555,556,557,558,559,565,566,567,568,588,589,590,592,593,594,595,597,598,599,601,602,605,606,607,609,610,612,613,614,615,616,617,618,619,620,622,625,626,627,628,629,630,631,634,635,636,637,638,639,640,641,642,643,644,645,646,647,649,650,654,655,656,657,658,659,661,664,665,666,667,668,669,670,671,672,673,674,675,676,677,678,679,680,681,682,684,686,687,688,689,690,691,694,695,696,697,698,699,700,701,702,703,705,706,707,708,709,711,712,713,714,715,716,717,718,719,720,721,722,723,724,725,726,727,728,729,730,731,732,733,734,735,736,737,738,739,740,741,742,743,744,745,746,747,748,749,750,751,752,753,754,755,756,757,758,759,760,762,763,764,765,766,767,768,769,770,771,772,773,774,776,777,778,779,780,781,782,783,784,785,786,787,788,789,791,792,793,794,795,796,797,798,799,800,801,802,803,804,805,807,808,809,810,811,812,814,815,816,817,818,819,820,821,822,823,824,825,826,827,828,829,830,831,832,833,834,835,836,837,838,839,840,841,842,843,844,845,846,847,848,849,850,851,852,853,854,855,856,857,858,859,860,861,862,863,864,865,866,867,868,869,870,871,872,873,874,875,876,877,878,879,880,881,882,883,884,885,886,887,888,889,890,891,892,893,894,895,896,897,898,899,920,998,999,1001,1111,1222,2000,3000,8801,8802,8803,8882,10000,10001,10002,10003,10004,10005,10006,10007,10008,10009,10010,10011,10012,10013,10014,10015,10016,10017,10018,10019,10020,10021,10022,10023,10024,10025,10026,10027,10028,10029,10030,10031,10032,10033,10034,10035,10036,10037,10038,10039,10040,10041,10042,10043,10044,10045,10046,10047,10048,10049,10050,10051,10052,10053,10054,10055,10056,10057,10058,10059,10060,10061,10062,10063,10064,10065,10066,10067,10068,10069,11111,12345,12522,15000,15001,15002,17000,20000,20001,20002,20003,20004,20005,20006,20007,20008,20009,20010,20011,30019,30020,30021,30022,30023,30024,30025,30026,30027,30028,30029,30030,30031,30032,30033,30034,30035,30036,30037,30038,30039,30040,30041,30042,30043,30045,30046,30047,30048,30049,30050,30051,30052,30053,30054,30055,30058,30059,30060,30061,30062,30063,30064,30065,40000,40001,40002,40003,40004,40005,40006,40007,40008,40009,40010,45000,45001,45002,45003,45004,45005,45006,46000,49000,49001,49002,49003,49004,49005,49006,49007,100001,100002,100005,100006,100007,100008,100009,100010,100011,100012,100013,100014,100015,100016,100017,100018,100019,100020,100021,100022,100023,100024,100025,100026,100027,100028,100029,100030,100031,100032,100033,100034,100035,100036,100037,100038,100039,100040,100041,100042,100043,100044,100045,100046,100047,100048,100049,100051,100052,100053,100054,100055,100056,100057,100058,100059,100060,100061,100062,100063,100064,100065,100066,100067,100068,100069,100070,100071,100072,100073,100074,100075,100076,100077,100078,100079,100080,100081,100082,100083,100084,100085,100086,100087,100088,100089,100090,100091,100092,100093,100094,100095,100096,100097,100098,100099,100100,100101,100102,100103,100104,100105,100106,100107,100108,100109,100110,100111,100112,100113,100114,100115,100116,100117,100118,100119,100120,100121,100122,100123,100124,100125,100126,100127,100128,100129,100130,100131,100132,100133,100134,100136,100137,100138,100139,100140,100141,100142,100144,100145,100146,100147,100148,100149,100150,100151,100152,100153,100154,100155,100156,100157,100158,100160,100161,100162,100163,100164,100165,100166,100167,100168,100169,100170,100171,100172,100173,100174,100175,100176,100177,100178,100179,100180,100181,100182,100183,100184,100185,100186,100187,100188,100189,100190,100191,100192,100193,100194,100195,100196,100197,100198,100199,100200,100201,100202,100203,100204,100206,100207,100208,100209,100210,100211,100212,100213,100214,100215,100216,100217,100218,100219,100220,100221,100222,100223,100224,100225,100226,100227,100230,100231,100232,100233,100234,100235,100236,100237,100238,100239,100240,100241,100242,100243,100244,100245,100246,100247,100248,100249,100250,100251,100252,100253,100254,100255,100256,100257,100258,100259,100260,100261,100262,100762,100817,110263,110264,150001,150002,150003,150004,150005,150006,150007,150008,150009,150010,150011,150012,150013,150014,160001,160002,160003,160004,160005,160006,160007,160008,160009,160010,160013,170001,170002,170003,170004,170005,170006,170007,170008,170009,170010,180001,180002,180003,180004,180005,180006,180007,180008,200002,200003,200004,200005,200006,200007,200008,200009,200010,200011,200012,200013,200014,200015,200016,200017,200018,200019,200020,200021,200022,200023,200024,200025,200026,200027,200028,200029,200030,200031,200032,200033,200034,200035,200036,200037,200038,200039,200040,200041,200042,200043,200044,200045,200046,200047,200048,200049,200050,200051,200052,200053,200054,200055,200056,200057,200058,200060,200061,200062,200063,200064,200065,200066,200067,200068,200069,200100,220070,220071,250001,250002,300001,300002,300003,300004,300005,300006,300007,300008,300009,300010,300011,300012,300013,300014,300015,300016,300017,300018,300019,300020,300021,300022,300023,300024,300025,300026,300027,300028,300029,300030,300031,300032,300033,300034,300035,300036,300037,300038,300039,300040,300041,300042,300043,300044,300045,300046,300047,300048,300049,300050,300051,300052,300053,300054,300055,300056,300057,300058,300060,300061,300062,300063,300064,300065,300066,300067,300068,300069,300070,300071,300072,300073,300074,300075,300076,300077,300078,300079,300080,300081,300082,300083,300084,300085,300086,300087,300088,300089,300090,300091,300092,300093,300094,300095,300096,300097,300098,300099,300101,300102,300103,300104,300105,300106,300107,300108,300109,300110,300111,300112,300113,300114,300115,300116,300117,300119,300120,300121,300122,300123,300124,300125,300126,300127,300128,300129,300130,300131,300132,300133,300134,300135,300136,300137,300138,300139,300140,300141,300142,300143,300144,300145,300146,300147,300148,300149,300150,300151,300152,300153,300154,300155,300156,300157,300158,300159,300160,300161,300162,300163,300164,300165,300166,300167,300169,300170,300171,300172,300173,300174,300175,300176,300177,300178,300179,300180,300181,300182,300183,300184,300185,300186,300187,300188,300189,300190,300191,300192,300193,300194,300195,300196,300197,300198,330199,400001,400002,401000,500001,500002,500003,500004,500005,500214,600001,800002,800006,800008,800014,800017,800024,800035,800044,800047,800055,800099,800801,800802,800803,909090,999999,1234567,20150819,123456789)" +
						" group by logdate,hid,oemid,adposid,amid" +
						" ) tmp" +
						" group by logdate,oemid";
				
				System.out.println("hivesql is : " + sql);

				List result = null;
				try {
					result = hiveJt.queryForList(sql);
				} catch (DataAccessException e) {
					e.printStackTrace();
				}
				
				long end = System.currentTimeMillis();
				System.out.println("查询耗时[" + (end - start) + "]毫秒");
				
				printResult(result);		

				System.out.println("done");
			}
			
			
			
			/**
			 * 华扬对数 分日分小时
			 */
					@Test
					public void huayangDayAndHour() {

						long start = System.currentTimeMillis();

					String sql = "select day,count(1) allnum,count(distinct hid) usernum from fact_adap_adst_history " +
								" where ven = 900100  and" +
								//" amid in  (1050000033,1050000034,1050000035,1050000036,1050000037,1050000038,1050000039,1050000040,1050000041,1050000042,1050000043,1050000044,1050000020)" +
								" amid = 2016072901" +
								" and te=0 and day='2016-08-16' group by day";
						
					/*	String sql = "select day,amid,count(1) allnum,count(distinct hid) usernum from fact_adap_adst_history " +
								" where ven = 900100  and amid in  (1050000033,1050000034,1050000035)" +
								" and te=0 and day>='2016-08-10' and day<='2016-08-12' group by day,amid  order by day,amid";*/
						
					/*	String sql = "select day,hour,count(1) allnum,count(distinct hid) usernum from fact_adap_adst_history " +
								" where ven = 900100  and amid in  (1050000033,1050000034,1050000035,1050000036,1050000037,1050000038,1050000039,1050000040,1050000041,1050000042,1050000043,1050000044,1050000020)" +
								" and te=0 and day>='2016-08-12' and day<='2016-08-14' group by day,hour" +
								" order by day,hour";*/
						
/*						String sql = "select * from fact_adap_adst_history " +
								" where ven = 900100  and amid in  (1050000033,1050000034,1050000035,1050000036,1050000037,1050000038,1050000039,1050000040,1050000041,1050000042,1050000043,1050000044,1050000020)" +
								" and te=0 and day='2016-08-11' limit 100";*/
						
						
						System.out.println("hivesql is : " + sql);

						List result = null;
						try {
							result = hiveJt.queryForList(sql);
						} catch (DataAccessException e) {
							e.printStackTrace();
						}
						
						long end = System.currentTimeMillis();
						System.out.println("查询耗时[" + (end - start) + "]毫秒");
						
						printResult(result);		

						System.out.println("done");
					}
					
					
					

	
	@Test
	public void heiLongJiang() {

		long start = System.currentTimeMillis();

		
		String sql = "SELECT * FROM adstat WHERE logdate=20160806 and oemid=300126 limit 10";
		
		System.out.println("hivesql is : " + sql);

		List result = null;
		try {
			result = hiveJt.queryForList(sql);
		} catch (DataAccessException e) {
			e.printStackTrace();
		}
		
		long end = System.currentTimeMillis();
		System.out.println("查询耗时[" + (end - start) + "]毫秒");
		
		printResult(result);		

		System.out.println("done");
	}
	
	@Test
	public void testRegx() {
		String str="{adstat.starttime=20160825174503, adstat.sessionid=14721183032096989550, adstat.uv=0, adstat.type=0, adstat.hid=a089e4448248, adstat.oemid=817, adstat.uid=135182187, adstat.provinceid=23, adstat.cityid=2300, adstat.adposid=17101610, adstat.amid=16088201, adstat.channelid=0, adstat.planid=4671, adstat.admt=null, adstat.pv=1, adstat.vv=1, adstat.distributeid=, adstat.ip=103.16.127.89, adstat.timequantum=17, adstat.logdate=20160825, adstat.bigtype=8}";
		Pattern p = Pattern.compile("\\w+\\.\\w+=(.*?)[,|}]"); 
        Matcher m = p.matcher(str);
        StringBuilder sbLine = new StringBuilder();
        while (m.find()) {
            String s0 = m.group(1);
//            System.out.println(s0);
            sbLine.append(s0).append(",");
        }
        System.out.println(sbLine.toString());  
        System.out.println("done!");       
	}
	
	/**
	 * 8月25日点播APK启动和首页EPG浮层的数据明细，包含HID和OEMID。
	 */
	@Test
	public void getApkStartAndEpgDetail() {

		long start = System.currentTimeMillis();

		
//		String sql = "SELECT * FROM adstat WHERE logdate=20160825 and adposid in (11101010,17101610) and uv=0 and amid<>0 and type=0";
//		String sql = "SELECT * FROM adstat WHERE logdate=20160825 and adposid in (11101010,17101610) and uv=0 and type=0";
		
	/*	String sql ="SELECT * FROM adstat WHERE logdate=20160825 " +
				" and adposid in (11101010)  "+
			"	and uv=0"+
			"	and type=0"+
			"	and hid not in ("+
			"		SELECT hid FROM adstat stat WHERE logdate=20160825"+
			"		and adposid in (17101610)"+
			"		and uv=0"+
			"		and type=0"+
			"	)";*/
		
		String sql =" select stat1.* from 				 "
			+" (SELECT * FROM adstat WHERE logdate=20160825 	 "
			+" and adposid in (11101010)			 "
			+" and uv=0					 "
			+" and type=0) stat1 left outer join 		 "
			+" (SELECT * FROM adstat WHERE logdate=20160825 	 "
			+" and adposid in (17101610)			 "
			+" and uv=0					 "
			+" and type=0) stat2				 "
			+" on stat1.hid=stat2.hid where stat2.hid is null ";

		
		System.out.println("hivesql is : " + sql);

		List result = null;
		try {
			result = hiveJt.queryForList(sql);
		} catch (DataAccessException e) {
			e.printStackTrace();
		}
		
		long end = System.currentTimeMillis();
		System.out.println("查询耗时[" + (end - start) + "]毫秒");
		
//		printResult(result);	
		
		writeResultNoGetKey(result);
		
		System.out.println("done");
	}
	
	/**
	 用于客户需求麻烦从广告联盟计算以下数据：
	对象：安利开机广告（8月14-22日） amid=2016072901
	维度：分区域分日
	目的：按分区域分日计算 1次-3次曝光数  和 4+以上曝光数
	 */
	@Test
	public void getAnliStart() {

		long start = System.currentTimeMillis();
				
		String sql = " select day,areaparentid,sum(case when allnum<=3 then allnum else 0 end) exp1_3, sum(case when allnum>=4 then allnum else 0 end) exp4"
				+" from (				       "
				+" select day,areaparentid,hid,count(1) allnum "
				+" from fact_adap_adst_history		       "
				+" where day>='2016-08-14'		       "
				+" and day<='2016-08-22'		       "
				+" and amid=2016072901			       "
				+" and te=0				       "
				+" group by day,areaparentid,hid	       "
				+" ) tmp				       "
				+" group by day,areaparentid		       "
				+" order by day,areaparentid		       ";

		
		System.out.println("hivesql is : " + sql);

		List result = null;
		try {
			result = hiveJt.queryForList(sql);
		} catch (DataAccessException e) {
			e.printStackTrace();
		}
		
		long end = System.currentTimeMillis();
		System.out.println("查询耗时[" + (end - start) + "]毫秒");
		
//		printResult(result);	
		
//		writeResultNoGetKey(result);
		writeResult(result);
		
		System.out.println("done");
	}
	
	/**
		终端统计导流位库存分日报表
	 */
	@Test
	public void getOemidByDay() {

		long start = System.currentTimeMillis();
				
		String sql = " select logdate,oemid,sum(expsum) as exposure,count(distinct hid) as touchone"
				+" from (								       "
				+" select logdate,hid,oemid,adposid,count(distinct sessionid) as expsum        "
				+" from adstat where type=0 and uv=0  and pv = 1  			       "
				+" and logdate >= '20160801' and logdate <= '20160831' 			       "
				+" group by logdate,hid,oemid,adposid) as tmp				       "
				+" group by logdate,oemid						       "
				+" order by logdate,oemid						       ";
		
		System.out.println("hivesql is : " + sql);

		List result = null;
		try {
			result = hiveJt.queryForList(sql);
		} catch (DataAccessException e) {
			e.printStackTrace();
		}
		
		long end = System.currentTimeMillis();
		System.out.println("查询耗时[" + (end - start) + "]毫秒");
//		printResult(result);
		
//{logdate=20160801, oemid=10000, exposure=1369, touchone=789}
		for (int j = 0; j < result.size(); j++) {
		      Map map = (Map)result.get(j);
		
		      String day =  (String) map.get("logdate");
		      String oemid = (String) map.get("oemid")+"";
		      
		      String oemname = oemMap.get(oemid);
		      
		      String exposure =  (Long) map.get("exposure")+"";
		      String touchone = (Long) map.get("touchone")+"";
		      
		      System.out.println(day+","+oemid+","+oemname+","+exposure+","+touchone);
		}
		
		System.out.println("done");
	}
	
	/**
	 * 导流位分日统计
	 */
	@Test
	public void getAdposByDay() {

		long start = System.currentTimeMillis();
				
		String sql = " SELECT  logdate,adposid,SUM(exposure) AS exposure,  SUM(    CASE      WHEN exposure >= 1       THEN 1       ELSE 0    END  ) AS touchone "
				+" FROM										     "
				+"   (SELECT    logdate, hid,    adposid,    COUNT(DISTINCT sessionid) AS exposure   "
				+"      FROM    adstat   WHERE TYPE = 0     AND uv = 0 				     "
				+"     AND logdate >= 20160801 							     "
				+"     AND logdate <= 20160831							     "
				+"     AND pv = 1 								     "
				+"   GROUP BY logdate,hid, adposid) AS temp1 					     "
				+" GROUP BY logdate,adposid 							     "
				+" order by logdate,adposid 							     ";

		
		System.out.println("hivesql is : " + sql);

		List result = null;
		try {
			result = hiveJt.queryForList(sql);
		} catch (DataAccessException e) {
			e.printStackTrace();
		}
		
		long end = System.currentTimeMillis();
		System.out.println("查询耗时[" + (end - start) + "]毫秒");
//		printResult(result);
		
//{logdate=20160801, oemid=10000, exposure=1369, touchone=789}
		for (int j = 0; j < result.size(); j++) {
		      Map map = (Map)result.get(j);
		
		      String day =  (String) map.get("logdate");
		      String adposid = (Integer) map.get("adposid")+"";
		      
		      String adposname = posMap.get(adposid);
		      
		      String exposure =  (Long) map.get("exposure")+"";
		      String touchone = (Long) map.get("touchone")+"";
		      
		      System.out.println(day+","+adposid+","+adposname+","+exposure+","+touchone);
		}
		
		System.out.println("done");
	}
	
	
	/**
	 * 广告联盟分节目，分区域统计。
	 */
	@Test
	public void getChuangweiDay() {

		long start = System.currentTimeMillis();
				
		String sql = " select amid,areaparentid,sum(allnum) allnum,sum(usernum) usernum " +
				" from (select amid,areaparentid,areaid,count(1) allnum, COUNT(DISTINCT (hid)) usernum " +
				" from fact_adap_adst_history t where DAY = '2016-09-08' and t.ven = 900102 and t.te=0" +
				"  and t.amid in (2016090101,2016090801)" +
				" group by amid,areaparentid,areaid) tmp" +
				" group by amid,areaparentid" +
				" order by amid,areaparentid";


		
		System.out.println("hivesql is : " + sql);

		List result = null;
		try {
			result = hiveJt.queryForList(sql);
		} catch (DataAccessException e) {
			e.printStackTrace();
		}
		
		long end = System.currentTimeMillis();
		System.out.println("查询耗时[" + (end - start) + "]毫秒");
//		printResult(result);
		
//{logdate=20160801, oemid=10000, exposure=1369, touchone=789}
		for (int j = 0; j < result.size(); j++) {
		      Map map = (Map)result.get(j);
		
		      String amid =  (Long) map.get("amid")+"";
		      String areaparentid =  (Long) map.get("areaparentid")+"";
		      
		      String proName = proMap.get(areaparentid);
		      
		      String exposure =  (Long) map.get("allnum")+"";
		      String touchone = (Long) map.get("usernum")+"";
		      
		      System.out.println(amid+","+proName+","+exposure+","+touchone);
		}
		
		System.out.println("done");
	}
	
	
	/**
	 * 导流位分日统计
	 */
	@Test
	public void test() {

		long start = System.currentTimeMillis();
				
//		String sql = "select * from adstat where logdate=20160904 and bigtype=6 limit 10		     ";
		String sql = " show create table  adstat";
		
		System.out.println("hivesql is : " + sql);

		List result = null;
		try {
			result = hiveJt.queryForList(sql);
		} catch (DataAccessException e) {
			e.printStackTrace();
		}
		
		long end = System.currentTimeMillis();
		System.out.println("查询耗时[" + (end - start) + "]毫秒");
//		printResult(result);
		
		for (int j = 0; j < result.size(); j++) {
		      Map map = (Map)result.get(j);
		System.out.println(map.toString());
		}
		
		System.out.println("done");
	}
	
	
	
	
	
	/**
	 * 将数据打印到控制台
	 * @param result
	 */
	private void printResult(List result){
		 for (int j = 0; j < result.size(); j++) {
		      Map map = (Map)result.get(j);    
		      System.out.println(map.toString());
		 }	
	}
	
	/**
	 * 将数据打印到控制台
	 * @param result
	 */
	private void printResultNoKey(List result){
		 for (int j = 0; j < result.size(); j++) {
		      Map map = (Map)result.get(j);   
		      String line = map.toString();
		      line =  StringUtils.substringBetween(line, "{","}") ;
		      
		      String[] array = line.split(",");
		      
		 }	
	}
	/**
	 * 获取结果打印数据，通过计算打印数据
	 * @param result
	 */
	private void printResultByKey(List result){
	
			for (int j = 0; j < result.size(); j++) {
			      Map map = (Map)result.get(j);
			
			      String day =  (String) map.get("day");
		//	      String areaparentid =  (Long) map.get("areaparentid")+"";
			      String channelid =  (Integer) map.get("channelid")+"";
			      String amid = (Long) map.get("amid")+"";
			      String allnum =  (Long) map.get("allnum")+"";
			      String usernum =  (Long) map.get("usernum")+"";
			      
		//	      String venName = vendorMap.get(ven);
			      //节目
//			      String amName = amidMap.get(amid);
			      
			      //地市
//			      String proName = proMap.get(areaparentid);
//		
//			      proName = proName==null?"国外":proName;
			      
		
			      //分区域(注，华扬的其他为国外)
		//	       System.out.println(day+","+proName+","+allnum+","+usernum);
			      //分地市
			      String areaid =  (Long) map.get("areaid")+"";
			      String areaName = cityMap.get(areaid);
//			      if(areaName==null||areaName.equals("其它")){
//			    	  areaName = proName;
//			      }
			      // System.out.println(day+","+proName+","+areaName+","+allnum+","+usernum);
			      
			      
		//	      分频道
//			      String channelName = channelMap.get(channelid);
			      
//			      System.out.println(day+","+channelid+","+amName+","+channelName+","+allnum+","+usernum);
			      
			 }
	}
	
	
	/**
	 * 将Result数据写到本地文件
	 * @param result
	 */
	private void writeResultNoGetKey(List result){
		System.out.println("===========================开始写文件==========================");
		//写文件
		File file = new File("e:\\work\\tmp\\apkandepg0825_7.txt");
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
	 
	 
	
}

