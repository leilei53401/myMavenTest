package kafkaecode;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import adap.AdapAreaInfo;
import adstat.AdposidStatGrid;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;


@Repository
public class Client {
	
	
	 Map<String,String>  oemidEpgMap = null;
	 Map<String,String>  epgDicMap = null;
	 Map<String,String>  adMap = null;
	 Map<String,Integer>  adLenMap = null;

	@Autowired
	public JdbcTemplate hiveKafkaJt;
	@Autowired
	public JdbcTemplate mysqlJt;
	
	List<AdapAreaInfo> areaInfoList = new ArrayList<AdapAreaInfo>();
	
	static DecimalFormat df = new DecimalFormat("#.00");  
	//测试环境
//	String [] outPutTitle = {"adstat.sessionid","adstat.hid","adstat.oemid","adstat.adposid"};
	//生产环境
//	String [] outPutTitle = {"sessionid","hid","oemid","adposid"};
	
	//广告联盟春节投放
	String [] outPutTitle = {"username","ven","ip","longip","areaid"};

	private List<ErrorCode> transformErrorCode(String datareportings) {
		List<ErrorCode> errorcodes = new ArrayList<ErrorCode>();
		Gson gson = new Gson();
		JsonParser jsonParser = new JsonParser();
		if(datareportings==null){
			return errorcodes;
		}
		JsonElement jsonElement = jsonParser.parse(datareportings);
		JsonArray jsonArray = jsonElement.getAsJsonArray();
		Iterator<JsonElement> it = jsonArray.iterator();
		while (it.hasNext()) {
			jsonElement = (JsonElement) it.next();
			ErrorCode errorcode = gson.fromJson(jsonElement.toString(), ErrorCode.class);
			errorcodes.add(errorcode);
		}
		/*
		 * for(ErrorCode e : errorcodes){ System.out.println(e.getOeminfo()); }
		 */
		return errorcodes;
	}

	/*****************************adstat相关*************************************/
	/**
	 * 查询导流位统计
	 * @return
	 */
	public List<AdposidStatGrid> findAdposidSum() {
		String startdate = "20160426";
		String enddate = "20160427";
		
//		String adlocationcode = "15101010,17101210,17101410";
		String adlocationcode = "15101010";
		String oemid = "";
		String amid = "44582669,87978660,35660010,42790566";
//		String amid = " 52506004,95140754";
		Object[] obj = new Object[2];
		obj[0] = startdate;
		obj[1] = enddate;
		String adposid_sql = "select adposid,sum(exposure) as exposure,sum(case when exposure >=1 then 1 else 0 end) as touchone "+
						"from "+
						"(select hid,adposid,count(adposid) as exposure "+ 
						"    from adstat where logdate >= 20160426 and logdate <= 20160427 ";
		String adposid_sum_sql = "select count(distinct hid)  as hid "+ 
						    "from adstat where logdate >= 20160426 and logdate <= 20160427 ";
		if(adlocationcode != null && !"".equals(adlocationcode)){
			adposid_sql += " and adposid in (" +adlocationcode+ ") ";
			adposid_sum_sql += " and adposid in (" +adlocationcode+ ") ";
		}
		if(oemid != null && !"".equals(oemid)){
			adposid_sql += " and oemid in (" +oemid+ ") ";
			adposid_sum_sql += " and oemid in (" +oemid+ ") ";
		}
		if(amid != null && !"".equals(amid)){
			adposid_sql += " and amid in (" +amid+ ") ";
			adposid_sum_sql += " and amid in (" +amid+ ") ";
		}
		adposid_sql += "    group by hid,adposid) as temp1 "+
						" group by adposid";
		
		System.out.println("adposid_sql is : "+adposid_sql);
		
		System.out.println("adposid_sum_sql is : "+adposid_sum_sql);
		
		List result =  hiveKafkaJt.queryForList(adposid_sql);
		  
		for (int j = 0; j < result.size(); j++) {
		      Map map = (Map)result.get(j);
		      System.out.println(map.toString());
		 }
		
		
		System.out.println("=============================");
		List resultSum =  hiveKafkaJt.queryForList(adposid_sum_sql);
		  
		for (int j = 0; j < resultSum.size(); j++) {
		      Map map = (Map)resultSum.get(j);
		      System.out.println(map.toString());
		 }
		
		
		
		
		/*List<AdposidStatGrid> oemid_list = hiveKafkaJt.query(adposid_sql,obj, new RowMapper<AdposidStatGrid>() {
			@Override
			public AdposidStatGrid mapRow(ResultSet rs, int rowNum) throws SQLException {
				AdposidStatGrid r = new AdposidStatGrid();
				r.setAdposid(rs.getString(1));
				r.setExposure(rs.getInt(2));
				r.setTouchone(rs.getString(3));
				r.setFrequency(df.format(Float.valueOf(r.getExposure())/Integer.valueOf(r.getTouchone())));
				return r;
			}
		});
		if(oemid_list != null && oemid_list.size() > 0){
			List<AdposidStatGrid> finalList = new ArrayList<AdposidStatGrid>();
//			Map<String, String> initAdposidData = initAdposidData();
			AdposidStatGrid tmp = null;
			int real_exposure = 0;
			for(AdposidStatGrid s : oemid_list){
				tmp = new AdposidStatGrid();
				int exposure = s.getExposure();
//				tmp.setAdposid(initAdposidData.get(s.getAdposid()) == null ? s.getAdposid() : initAdposidData.get(s.getAdposid()));
				//System.out.println("s.getAdposid())"+s.getAdposid());
				tmp.setAdposid(s.getAdposid());
				tmp.setExposure(exposure);
				tmp.setTouchone(s.getTouchone());
				tmp.setFrequency(s.getFrequency());
				finalList.add(tmp);
				real_exposure += exposure;
				
				System.out.println("AdposidStatGrid : "+s.toString());
			}
			List<String> adposid_sum_list = hiveKafkaJt.query(adposid_sum_sql,obj, new RowMapper<String>() {
				@Override
				public String mapRow(ResultSet rs, int rowNum) throws SQLException {
					return rs.getString(1);
				}
			});
			String real_touch = adposid_sum_list.get(0);
			AdposidStatGrid s = new AdposidStatGrid();
			s.setAdposid("汇总");
			s.setExposure(real_exposure);
			s.setTouchone(real_touch + "");
			s.setFrequency(df.format(Float.valueOf(real_exposure)/Integer.valueOf(real_touch)));
//			finalList.add(s);
//			return finalList;
			
			System.out.println("汇总结果 : "+s.toString());
		}*/
		return null;
	}
	
	
	/*private Map<String, String> initAdposidData() {
		Map<String, String> map = new HashMap<String, String>();
		List<Map<String, Object>> list = mysqlJt_adguide
				.queryForList("SELECT ad_location_code as adposid,guide_detailname as adposname FROM ad_guide_detail_loc");
		for (Map<String, Object> temp : list) {
			String adposid = temp.get("adposid").toString();
			String adposname = CommonUtil.getJdbcNameObj(temp.get("adposname"));
			map.put(adposid, adposid + "_" + adposname);
		}
		return map;
	}*/
	
	public void testHIveSql() {
		
		//加载mysql字典表
		//省份
	/*	Map<String,String> proMap = new HashMap<String,String>();
		String provinceSql = "SELECT t.Name,t.Code FROM  ad_ip_province t ";
		List provinceResult =  mysqlJt.queryForList(provinceSql);
		 for (int i = 0; i < provinceResult.size(); i++) {
		      Map map = (Map)provinceResult.get(i);
		      String name  = (String) map.get("name");
		      String code  = (String) map.get("code");
		      proMap.put(code,name);
		 }
		 
		 System.out.println("省份字典表:"+proMap.toString());*/
		
		 //地市
		/*Map<String,String> cityMap = new HashMap<String,String>();
		String citySql = "SELECT t.Name,t.Code FROM  ad_ip_area t ";
		List cityResult =  mysqlJt.queryForList(citySql);
		 for (int i = 0; i < cityResult.size(); i++) {
		      Map map = (Map)cityResult.get(i);
		      String name  = (String) map.get("name");
		      String code  = (String) map.get("code");
		      cityMap.put(code,name);
		 }*/
		 /*
		//厂家 
			Map<String,String>  vendorMap = new HashMap<String,String>();
			String vendorSql = "SELECT t.`ven_no`,t.`ven_name` FROM `adap_vendor` t ";
			List vendorResult =  mysqlJt.queryForList(vendorSql);
			 for (int i = 0; i < vendorResult.size(); i++) {
			      Map map = (Map)vendorResult.get(i);
			      String ven_no  = (Integer)map.get("ven_no")+"";
			      String ven_name  = (String) map.get("ven_name");
			      vendorMap.put(ven_no,ven_name);
			 }		
			 */
			 
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
		/* Map<String,String>  posMap = new HashMap<String,String>();
		String posSql = "SELECT guide_detailname NAME,ad_location_code CODE FROM ad_guide_detail_loc WHERE STATUS=1 AND belong_adap=1";
		List posResult =  mysqlJt.queryForList(posSql);
		 for (int i = 0; i < posResult.size(); i++) {
		      Map map = (Map)posResult.get(i);
		      String name  = (String) map.get("name");
		      String code  = (Integer) map.get("code")+"";
		      posMap.put(code,name);
		 }*/
		 	 
	
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
		 
		 
		 //查询出OEMID和EPG关系。
		 oemidEpgMap = new HashMap<String,String>();
			String oemidEpgSql = "SELECT t.`oemid`,t.`epgid` FROM oem_epg_dic t  ";
			List oemidEpgResult =  mysqlJt.queryForList(oemidEpgSql);
			 for (int i = 0; i < oemidEpgResult.size(); i++) {
			      Map map = (Map)oemidEpgResult.get(i);
			      String oemid  = (Integer) map.get("oemid")+"";
			      String epgid  = (Integer) map.get("epgid")+"";
			      oemidEpgMap.put(oemid,epgid);
			 }
			 
			 System.out.println("oemidEpgMap  = :" + oemidEpgMap.toString());
			 
		 //查询 EPG编号和名称字典。
			 epgDicMap = new HashMap<String,String>();
			String epgSql = "SELECT t.`epgid`,t.portalname FROM oem_epg_dic t  ";
			List epgResult =  mysqlJt.queryForList(epgSql);
			 for (int i = 0; i < epgResult.size(); i++) {
			      Map map = (Map)epgResult.get(i);
			      String epgid  = (Integer) map.get("epgid")+"";
			      String portalname  = (String) map.get("portalname");
			      portalname = portalname==null?"":portalname.trim();
			      epgDicMap.put(epgid,portalname);
			}
			 
			 System.out.println("epgDicMap  = :" + epgDicMap.toString());
		
		//广告节目字典表
			  adMap = new HashMap<String,String>();
				String adSql = "SELECT t.amid,t.adname FROM `guide_movieinfo` t  ";
				List adResult =  mysqlJt.queryForList(adSql);
				 for (int i = 0; i < adResult.size(); i++) {
				      Map map = (Map)adResult.get(i);
				      String amid  = (Integer) map.get("amid")+"";
				      String adname  = (String) map.get("adname");
				      adMap.put(amid,adname);
				}
				 
			 System.out.println("adMap  = :" + adMap.toString());
		//广告节目时长字典表
				adLenMap = new HashMap<String,Integer>();
					String adLenSql = "SELECT t.amid,t.length FROM `guide_movieinfo` t where TYPE=1 ";
					List adLenResult =  mysqlJt.queryForList(adLenSql);
					 for (int i = 0; i < adLenResult.size(); i++) {
					      Map map = (Map)adLenResult.get(i);
					      String amid  = (Integer) map.get("amid")+"";
					      Integer length  = (Integer) map.get("length");
					      adLenMap.put(amid,length);
					}
			 System.out.println("adLenMap  = :" + adLenMap.toString());
		 
	
		
/*		String mysql_sql="SELECT a.provinceid,a.provincename,a.areaid,a.netseg1dec,a.netseg2dec,ar.name cityname,ar.code cityid FROM " +
				"	(SELECT pr.code provinceid,pr.name provincename,ip.areaid,netseg1dec,netseg2dec" +
				"	 FROM ad_ip_topology ip,ad_ip_province pr" +
				"	 WHERE ip.provinceid = pr.provinceid )" +
				" a LEFT JOIN ad_ip_area ar ON a.areaid=ar.areaid ";
		
//		List<AdapAreaInfo> result =  mysqlJt.queryForList(mysql_sql);
		
//		List<AdapAreaInfo> areaInfo = new ArrayList<AdapAreaInfo>();
		areaInfoList = mysqlJt.query(mysql_sql, new RowMapper<AdapAreaInfo>() {
			@Override
			public AdapAreaInfo mapRow(ResultSet rs, int rowNum) throws SQLException {
				AdapAreaInfo info = new AdapAreaInfo();
				info.setProid(rs.getInt("provinceid"));
				info.setProname(rs.getString("provincename"));
				info.setAreaid(rs.getInt("areaid"));
				info.setNetseg1(rs.getLong("netseg1dec"));
				info.setNetseg2(rs.getLong("netseg2dec"));
				info.setCityname(rs.getString("cityname"));
				info.setCityid(rs.getInt("cityid"));
				return info;
			}
		});
		
		System.out.println("init areaInfoList end , areaInfoList.size="+areaInfoList.size());*/
		
		//------定义写文件信息------------
		//写文件
		/*		File file = new File("e:\\work\\tmp\\lianmeng.txt");
				FileWriter fw = null;
				BufferedWriter bw = null;
				try {
					fw = new FileWriter(file,true);//true 追加写 
					bw = new BufferedWriter (fw);  
		*/

		 
		 //查询msql表
//		String hiveSql = "select count(1) from  fact_adst_history where day>='2016-02-07' and day<='2016-02-22' and amid in (2016020701,2016020702,2016011501,2016020703,2016020704) and ven in (900101,900102,900103,900104,900105)";
	/*	String hiveSql ="select count(distinct(hid)) as usernum " +
				" from  fact_adst_history where day>='2016-02-07' and day<='2016-02-22' and amid in (2016020701,2016020702,2016011501,2016020703,2016020704) " +
				" and ven in (900101,900102,900103,900104,900105)";*/
/*		String addJar = "add jar hdfs://master:9000/hive_udfs/adudf.jar";
		String createFunction = "create temporary function dim_getAreaIdByIp as 'com.xiaojin.hiveudf.GetAreaIdByIP'";
		String hiveSql = "select hid,ip,dim_getAreaIdByIp(hid) areaid from adap_count_tmp limit 10";
		hiveKafkaJt.execute(addJar);
		hiveKafkaJt.execute(createFunction);
		*/
		
		long start = System.currentTimeMillis();
		
//		hiveKafkaJt.execute("use kafka");
		//20160608 统计6月1号后所有厂家按节目，天的数据
	/*   	 String hiveSql = "SELECT   DAY,  k_record.ven,  k_record.amid, SUM(1) allnum, COUNT(DISTINCT (k_record.hid)) usernum " +
	   	 		" FROM  kafka_adap_adst_0 " +
	   	 		" WHERE DAY >= '2016-06-01' GROUP BY DAY,  k_record.amid,  k_record.ven " +
	   	 		" order by DAY,k_record.ven,k_record.amid";*/
		
	  /* 	 String hiveSql = "SELECT   DAY, HOUR, k_record.ven,  k_record.amid, SUM(1) allnum, COUNT(DISTINCT (k_record.hid)) usernum " +
		   	 		" FROM  kafka_adap_adst_0 " +
		   	 		" WHERE DAY >= '2016-06-01' GROUP BY DAY, HOUR, k_record.amid,  k_record.ven " +
		   	 		" order by DAY,k_record.ven,k_record.amid";*/
		//海尔
	   	/* String hiveSql = "SELECT   DAY,  k_record.amid,  SUM(1) allsum,   COUNT(DISTINCT (k_record.hid)) usrnum" +
	   	 		" FROM  kafka.kafka_adap_adst_0 " +
	   	 		" WHERE DAY >= '2016-05-29'   AND k_record.ven = 900106 " +
	   	 		" GROUP BY DAY,  k_record.amid  " +
	   	 		" order by day,k_record.amid ";*/
		
		/*String hiveSql = "select SUM(1) allsum,count(distinct(hid)) as usernum,areaparentid from  fact_adst_history " +
				" where day>='2016-05-31' and ven = 900106 group by areaparentid";*/
		
		/*String hiveSql = "select count(1) allsum,count(distinct(hid)) as usernum,ven,getip(ip) areaid " +
				" from  fact_adst_history " +
				" where day='2016-02-07'  and amid in (2016020701,2016020702,2016011501,2016020703,2016020704) and ven in (900101,900102,900103,900104,900105)" +
				" group by ven,getip(ip)";*/
		
		/*String hiveSql = "select ven,getip(ip) areaid " +
				" from  fact_adst_history " +
				" where day='2016-02-07'  and amid in (2016020701,2016020702,2016011501,2016020703,2016020704) and ven in (900101,900102,900103,900104,900105)" +
				" group by ven,getip(ip)";*/
		
		/*String hiveSql = "select ven,amid,newareaid,count(1) allsum,count(distinct hid) usersum from(select ven,amid,hid,ip,getip(ip) newareaid" +
				" from  fact_adst_history where day>='2016-02-07' and day<='2016-02-22') tt group by tt.ven,tt.amid,tt.newareaid";*/
		
		
		//小时
		/*String hiveSql = "SELECT   DAY, HOUR, k_record.amid,  COUNT(1) allnum,  COUNT(DISTINCT k_record.hid) alluser " +
				" FROM kafka_adap_adst_0 WHERE k_record.ven = 900107  " +
				" AND DAY = '2016-06-13' GROUP BY DAY, HOUR, k_record.amid order BY DAY, HOUR, k_record.amid";*/
		//天
/*		String hiveSql = "SELECT   DAY, k_record.amid,  COUNT(1) allnum,  COUNT(DISTINCT k_record.hid) alluser " +
				" FROM kafka_adap_adst_0 WHERE k_record.ven = 900107  " +
				" AND DAY = '2016-06-13' GROUP BY DAY, k_record.amid order BY DAY, k_record.amid";*/
		
/*		String hiveSql  = "SELECT   DAY,  k_record.ven,  k_record.amid, k_record.pos,getcityid_01(k_record.ip) areaid,k_record.hid" +
						" FROM  kafka.kafka_adap_adst_0 WHERE DAY = '2016-06-14' and k_record.ven=900108 and k_record.te=1 limit 50";*/
		
		/*String hiveSql = "SELECT  amid, areaid," +
				"  SUM(1) allnum,  COUNT(DISTINCT (hid)) usernum" +
				" from (select day,  k_record.ven,  k_record.amid, k_record.pos ,getcityid_01(k_record.ip) areaid,k_record.hid" +
				" FROM  kafka.kafka_adap_adst_0 WHERE " +
				" k_record.ven=900108 and k_record.te=0 " +
				" ) tt" +
				" GROUP by tt.amid, areaid  " +
				" order by tt.amid,areaid";*/
/*		
		String hiveSql = "SELECT  areaid," +
				"  SUM(1) allnum,  COUNT(DISTINCT (hid)) usernum" +
				" from (select day,  k_record.ven,  k_record.amid, k_record.pos ,getcityid_01(k_record.ip) areaid,k_record.hid" +
				" FROM  kafka.kafka_adap_adst_0 WHERE " +
				" k_record.ven=900108 and k_record.te=0 " +
				" ) tt" +
				" GROUP by  areaid ";*/
		
/*		String hiveSql = "select amid,day,hour,count(1) allsum,count(distinct(hid)) as usernum from  fact_adst_history " +
				" where day ='2016-06-29' and amid in (2016062202,2016062201) " +
				" group by amid,day,hour order by amid,day,hour";*/
		
	/*	String hiveSql =	" SELECT 	  DAY,		  HOUR,		  k_record.ven,		  k_record.amid,	  SUM(1)," +
				"  COUNT(DISTINCT (k_record.hid)) usernum	" +
				" FROM	 kafka.kafka_adap_adst_0		" +
				" WHERE DAY = '2016-06-30'		 and k_record.ven = 900107		 and k_record.amid = 2016051810		" +
				" GROUP BY DAY,HOUR,		  k_record.amid,	  k_record.ven		  order by DAY,HOUR,k_record.ven,k_record.amid	";
				*/
		
	/*	String hiveSql = "select sum(exposure) as exposure, sum(case when exposure >=1 then 1 else 0 end) as touchone, sum(case when exposure >=2 then 1 else 0 end) as touchtwo,  sum(case when exposure >=3 then 1 else 0 end) as touchthree,  sum(case when exposure >=4 then 1 else 0 end) as touchfour,  sum(case when exposure >=5 then 1 else 0 end) as touchfive,  sum(case when exposure >=6 then 1 else 0 end) as touchsix,  sum(case when exposure >=7 then 1 else 0 end) as touchseven,  sum(case when exposure >=8 then 1 else 0 end) as toucheight,  sum(case when exposure >=9 then 1 else 0 end) as touchnine,  sum(case when exposure >=10 then 1 else 0 end) as touchten  from  (select hid,count(distinct sessionid) as exposure from adstat" +
				" where logdate >= 20160601 and logdate <= 20160630 and adposid = 15101010 and uv = 0 and oemid in (126,158,165,176,177,178,180,222,226,227,229,273,276,285,296,297,309,321,323,329,332,337,342,374,380,384,397,414,415,416,425,440,445,446,541,544,546,547,559,566,593,597,598,599,602,610,615,616,617,620,627,636,637,638,639,640,641,644,646,655,658,665,667,669,670,673,674,676,677,680,682,686,690,691,694,698,699,705,706,708,726,729,737,738,739,741,745,746,747,750,757,760,763,764,768,769,772,773,777,778,779,780,784,785,786,788,789,791,792,796,799,807,808,809,814,816,818,819,821,822,828,829,831,833,834,838,839,841,842,843,848,849,851,852,853,854,861,868,869,870,875,877,878,880,881,882,883,889,890,891,892,893,894,895,896,898,100002,100005,100006,100007,100008,100009,100010,100011,100012,100014,100015,100018,100019,100020,100021,100022,100023,100024,100025,100026,100027,100028,100029,100030,100031,100032,100033,100034,100035,100036,100037,100038,100039,100040,100042,100043,100044,100045,100046,100047,100048,100051,100052,100053,100054,100055,100056,100057,100058,100060,100062,100063,100064,100065,100066,100067,100068,100069,100070,100071,100072,100073,100075,100076,100077,100078,100079,100080,100082,100083,100085,100086,100088,100089,100090,100092,100095,100096,100097,100098,100099,100100,100102,100104,100108,100109,100114,100116,100117,100118,100119,100120,100121,100122,100123,100124,100125,100126,100127,100128,100129,100130,100131,100132,100136,100137,100138,100139,100140,100141,100142,100144,100146,100147,100148,100149,100150,100152,100153,100154,100155,100156,100157,100158,100160,100161,100162,100163,100164,100165,100166,100167,100168,100169,100170,100171,100172,100173,100174,100175,100176,100177,100178,100179,100180,100181,100182,100183,100184,100185,100186,100187,100188,100189,100190,100191,100192,100193,100194,100195,100196,100197,100198,100200,100203,100204,100206,100207,100208,100210,100211,100212,100213,100214,100215,100216,100217,100218,100219,100221,100222,150001,150002,150003,150004,150005,150006,150007,150008,150009,160001,160002,160003,160004,160005,160006,160007,160008,160009,160010,170001,170002,170003,170004,170005,170006,170007,180001,180002,180003,180004,180005,180006,180007,180008,200005,200006,200007,200008,200011,200012,200013,200015,200017,200018,200019,200023,200025,200027,200029,200030,200031,200032,200033,200035,200042,200043,200045,200046,200052,200053,200056,200061,200062,250001,300004,300062,400001,401000,500001,500002,500004,500005,500214,600001,725,300025,300136,300003,797,368,225,545,100199,100202,100223,100224,100225,100227,100233,100234,100237,100240,100241,100243,134,135,136,142,143,145,148,153,155,156,157,170008,160013,172,181,182,183,200002,200003,200004,200009,200010,200014,200020,200021,200022,200024,200026,200028,200034,200036,200040,200041,200055,200044,200048,200049,200050,200057,200058,200059,200060,200063,200064,200065,200068,200100,205,208,209,211,212,217,223,258,268,275,278,293,369,303,305,325,335,345,355,367,371,379,386,395,406,409,433,410,411,412,441,442,443,444,447,448,449,532,534,538,565,548,555,606,612,619,622,625,650,654,656,661,679,696,702,727,740,742,781,800002,800008,804,805,815,837,823,847,856,858,859,860,862,886,100074,840,100059,100061,100103,100115,100134,100151,531,681,776,100049) " +
				" and channelid not in (100,201,202,101,102,103,204,104,205,206,105,207,107,108,0,106)  group by hid) as temp1";*/
		
//		channelid 为112 的数据
			/*String hiveSql = "select sum(exposure) as exposure, sum(case when exposure >=1 then 1 else 0 end) as touchone, sum(case when exposure >=2 then 1 else 0 end) as touchtwo,  sum(case when exposure >=3 then 1 else 0 end) as touchthree,  sum(case when exposure >=4 then 1 else 0 end) as touchfour,  sum(case when exposure >=5 then 1 else 0 end) as touchfive,  sum(case when exposure >=6 then 1 else 0 end) as touchsix,  sum(case when exposure >=7 then 1 else 0 end) as touchseven,  sum(case when exposure >=8 then 1 else 0 end) as toucheight,  sum(case when exposure >=9 then 1 else 0 end) as touchnine,  sum(case when exposure >=10 then 1 else 0 end) as touchten  from  (select hid,count(distinct sessionid) as exposure from adstat" +
		" where logdate >= 20160601 and logdate <= 20160630 and adposid = 15101010 and uv = 0 and oemid in (126,158,165,176,177,178,180,222,226,227,229,273,276,285,296,297,309,321,323,329,332,337,342,374,380,384,397,414,415,416,425,440,445,446,541,544,546,547,559,566,593,597,598,599,602,610,615,616,617,620,627,636,637,638,639,640,641,644,646,655,658,665,667,669,670,673,674,676,677,680,682,686,690,691,694,698,699,705,706,708,726,729,737,738,739,741,745,746,747,750,757,760,763,764,768,769,772,773,777,778,779,780,784,785,786,788,789,791,792,796,799,807,808,809,814,816,818,819,821,822,828,829,831,833,834,838,839,841,842,843,848,849,851,852,853,854,861,868,869,870,875,877,878,880,881,882,883,889,890,891,892,893,894,895,896,898,100002,100005,100006,100007,100008,100009,100010,100011,100012,100014,100015,100018,100019,100020,100021,100022,100023,100024,100025,100026,100027,100028,100029,100030,100031,100032,100033,100034,100035,100036,100037,100038,100039,100040,100042,100043,100044,100045,100046,100047,100048,100051,100052,100053,100054,100055,100056,100057,100058,100060,100062,100063,100064,100065,100066,100067,100068,100069,100070,100071,100072,100073,100075,100076,100077,100078,100079,100080,100082,100083,100085,100086,100088,100089,100090,100092,100095,100096,100097,100098,100099,100100,100102,100104,100108,100109,100114,100116,100117,100118,100119,100120,100121,100122,100123,100124,100125,100126,100127,100128,100129,100130,100131,100132,100136,100137,100138,100139,100140,100141,100142,100144,100146,100147,100148,100149,100150,100152,100153,100154,100155,100156,100157,100158,100160,100161,100162,100163,100164,100165,100166,100167,100168,100169,100170,100171,100172,100173,100174,100175,100176,100177,100178,100179,100180,100181,100182,100183,100184,100185,100186,100187,100188,100189,100190,100191,100192,100193,100194,100195,100196,100197,100198,100200,100203,100204,100206,100207,100208,100210,100211,100212,100213,100214,100215,100216,100217,100218,100219,100221,100222,150001,150002,150003,150004,150005,150006,150007,150008,150009,160001,160002,160003,160004,160005,160006,160007,160008,160009,160010,170001,170002,170003,170004,170005,170006,170007,180001,180002,180003,180004,180005,180006,180007,180008,200005,200006,200007,200008,200011,200012,200013,200015,200017,200018,200019,200023,200025,200027,200029,200030,200031,200032,200033,200035,200042,200043,200045,200046,200052,200053,200056,200061,200062,250001,300004,300062,400001,401000,500001,500002,500004,500005,500214,600001,725,300025,300136,300003,797,368,225,545,100199,100202,100223,100224,100225,100227,100233,100234,100237,100240,100241,100243,134,135,136,142,143,145,148,153,155,156,157,170008,160013,172,181,182,183,200002,200003,200004,200009,200010,200014,200020,200021,200022,200024,200026,200028,200034,200036,200040,200041,200055,200044,200048,200049,200050,200057,200058,200059,200060,200063,200064,200065,200068,200100,205,208,209,211,212,217,223,258,268,275,278,293,369,303,305,325,335,345,355,367,371,379,386,395,406,409,433,410,411,412,441,442,443,444,447,448,449,532,534,538,565,548,555,606,612,619,622,625,650,654,656,661,679,696,702,727,740,742,781,800002,800008,804,805,815,837,823,847,856,858,859,860,862,886,100074,840,100059,100061,100103,100115,100134,100151,531,681,776,100049) " +
		" and channelid = 112 group by hid) as temp1";*/
	/*	
			String hiveSql = "select day,hour,count(1) allsum,count(distinct(hid)) as usernum from  fact_adst_history" +
					" where day='2016-07-07' and ven = 900105 and amid=2016062202 group by day,hour order by day,hour";*/

		/*String hiveSql = "select distinct channelid from adstat" +
	" where logdate >= 20160601 and logdate <= 20160630 and adposid = 15101010 and uv = 0 and oemid in (126,158,165,176,177,178,180,222,226,227,229,273,276,285,296,297,309,321,323,329,332,337,342,374,380,384,397,414,415,416,425,440,445,446,541,544,546,547,559,566,593,597,598,599,602,610,615,616,617,620,627,636,637,638,639,640,641,644,646,655,658,665,667,669,670,673,674,676,677,680,682,686,690,691,694,698,699,705,706,708,726,729,737,738,739,741,745,746,747,750,757,760,763,764,768,769,772,773,777,778,779,780,784,785,786,788,789,791,792,796,799,807,808,809,814,816,818,819,821,822,828,829,831,833,834,838,839,841,842,843,848,849,851,852,853,854,861,868,869,870,875,877,878,880,881,882,883,889,890,891,892,893,894,895,896,898,100002,100005,100006,100007,100008,100009,100010,100011,100012,100014,100015,100018,100019,100020,100021,100022,100023,100024,100025,100026,100027,100028,100029,100030,100031,100032,100033,100034,100035,100036,100037,100038,100039,100040,100042,100043,100044,100045,100046,100047,100048,100051,100052,100053,100054,100055,100056,100057,100058,100060,100062,100063,100064,100065,100066,100067,100068,100069,100070,100071,100072,100073,100075,100076,100077,100078,100079,100080,100082,100083,100085,100086,100088,100089,100090,100092,100095,100096,100097,100098,100099,100100,100102,100104,100108,100109,100114,100116,100117,100118,100119,100120,100121,100122,100123,100124,100125,100126,100127,100128,100129,100130,100131,100132,100136,100137,100138,100139,100140,100141,100142,100144,100146,100147,100148,100149,100150,100152,100153,100154,100155,100156,100157,100158,100160,100161,100162,100163,100164,100165,100166,100167,100168,100169,100170,100171,100172,100173,100174,100175,100176,100177,100178,100179,100180,100181,100182,100183,100184,100185,100186,100187,100188,100189,100190,100191,100192,100193,100194,100195,100196,100197,100198,100200,100203,100204,100206,100207,100208,100210,100211,100212,100213,100214,100215,100216,100217,100218,100219,100221,100222,150001,150002,150003,150004,150005,150006,150007,150008,150009,160001,160002,160003,160004,160005,160006,160007,160008,160009,160010,170001,170002,170003,170004,170005,170006,170007,180001,180002,180003,180004,180005,180006,180007,180008,200005,200006,200007,200008,200011,200012,200013,200015,200017,200018,200019,200023,200025,200027,200029,200030,200031,200032,200033,200035,200042,200043,200045,200046,200052,200053,200056,200061,200062,250001,300004,300062,400001,401000,500001,500002,500004,500005,500214,600001,725,300025,300136,300003,797,368,225,545,100199,100202,100223,100224,100225,100227,100233,100234,100237,100240,100241,100243,134,135,136,142,143,145,148,153,155,156,157,170008,160013,172,181,182,183,200002,200003,200004,200009,200010,200014,200020,200021,200022,200024,200026,200028,200034,200036,200040,200041,200055,200044,200048,200049,200050,200057,200058,200059,200060,200063,200064,200065,200068,200100,205,208,209,211,212,217,223,258,268,275,278,293,369,303,305,325,335,345,355,367,371,379,386,395,406,409,433,410,411,412,441,442,443,444,447,448,449,532,534,538,565,548,555,606,612,619,622,625,650,654,656,661,679,696,702,727,740,742,781,800002,800008,804,805,815,837,823,847,856,858,859,860,862,886,100074,840,100059,100061,100103,100115,100134,100151,531,681,776,100049) " +
	" and channelid not in (100,201,202,101,102,103,204,104,205,206,105,207,107,108,0,106) ";
		*/
		
		/*String hiveSql="select hid,count(1) hnum from adstat" +
				"  where amid='18702466' and logdate>='20160523' and logdate<='20160613'" +
				" group by hid " +
				" order by hnum asc";*/
		
	/*	String hiveSql="select ip,count(1) hnum from adstat" +
				"  where amid='18702466' and logdate>='20160523' and logdate<='20160613'" +
				" group by ip " +
				" order by hnum desc limit 50";*/
		
//		String hiveSql="select * from fact_adst_history  where amid='18702466' limit 10";
		
		/*String hiveSql="select hid,count(1) hnum from fact_adst_history" +
				"  where amid='18702466' and day>='2016-05-23' and day<='2016-06-13'" +
				" group by hid " +
				" order by hnum desc limit 20";*/
/*		
		String hiveSql="select ip,count(1) hnum from fact_adst_history" +
				"  where amid='18702466' and day>='2016-05-23' and day<='2016-06-13'" +
				" group by ip " +
				" order by hnum desc limit 20";*/
		
		/*String hiveSql = "select SUM(1) allsum,count(distinct(hid)) as usernum,areaparentid" +
				" from fact_adst_history where day>='2016-06-11' and day<='2016-07-10'  and ven = 900101 and amid = 2016060801 group by areaparentid";
				*/
		
	/*	String hiveSql = "select SUM(1) allsum,count(distinct(hid)) as usernum" +
				" from fact_adst_history where day>='2016-06-11' and day<='2016-07-10'  and ven = 900101 and amid = 2016060801";
*/
		
	/*	String hiveSql = "select ven,amid,count(1) allnum,count(distinct hid) usernum " +
				" from fact_adap_adst_history group by ven,amid" +
				" order by 	ven,amid	 ";*/
		
		
		/*String hiveSql = "select ven,count(1) allnum,count(distinct hid) usernum " +
				" from fact_adap_adst_history group by ven" +
				" order by 	ven	 ";*/
		//厂家分省
		/*String hiveSql = "select ven,day,count(1) allnum,count(distinct hid) usernum " +
				" from fact_adap_adst_history group by ven,day" +
				" order by 	ven,day ";*/
		
		/*String hiveSql = "select k_record.ven,k_record.amid,count(1) allnum,count(distinct k_record.hid) usernum " +
				" from kafka.kafka_adap_adst_0" +
				" group by k_record.ven,k_record.amid" +
				" order by k_record.ven,k_record.amid ";*/
		
//		String hiveSql = "select * from kafka.kafka_adap_adst_0 where day='2016-07-17'";
		
		
//		String hiveSql = "SELECT count(1) allsum FROM  adstat WHERE logdate >= 20160630  AND logdate <= 20160706   AND amid = 95661128";
		
/*		String hiveSql = "select oemid,sum(exposure) as exposure,sum(case when exposure >=1 then 1 else 0 end) as touchone " +
				"	from (select hid,oemid,adposid,count(distinct sessionid) as exposure " +
				"	 from adstat where logdate >= '20160719' and logdate <= '20160719'	 and oemid = 100001" +
				"	group by hid,oemid,adposid) as temp1 group by oemid";*/
		
		/*String hiveSql = "select oemid,sum(expsum) as expsum,sum(case when usersum >=1 then 1 else 0 end) as touchone " +
				"  from (select hid,oemid,count(distinct sessionid) usersum,sum(expsum) expsum" +
				"	from	(select hid,oemid,adposid,sessionid,count(distinct sessionid) expsum" +
				"	from adstat where logdate >= '20160719' and logdate <= '20160719'	and oemid in (10003,100045,100051)		group by hid,oemid,adposid,sessionid) as tmp" +
				"		group by hid,oemid) as temp1  group by oemid ";*/
		
	/*	String hiveSql = "select provinceid,sum(exposure) exposure" +
				" from (select provinceid,hid,count(distinct sessionid) as exposure" +
				" from adstat where logdate = '20160719' and adposid = 15101010 and uv = 0" +
				" and oemid in (100054)  and provinceid in (23,12,11,10,24,25,26,27)  group by provinceid,hid) as tmp" +
				" group by  provinceid ";*/
		
		//汇总
	/*	String hiveSql = "select count(distinct sessionid) as exposure" +
				" from adstat where logdate = '20160719' and adposid = 15101010 and uv = 0" +
				" and oemid in (100054)  and provinceid in (23,12,11,10,24,25,26,27) ";*/
/*		
		String hiveSql="select provinceid,count(distinct sessionid) as exposure " +
				" from adstat where logdate = '20160719' and adposid = 15101010 and uv = 0 and oemid in (100054)" +
				"  and provinceid in (23,12,11,10,24,25,26,27) group by provinceid ";*/
		
	/*	String hiveSql = "SELECT  DAY,  HOUR,  k_record.amid,  SUM(1),  COUNT(DISTINCT (k_record.hid))" +
				" FROM  kafka.kafka_adap_adst_0  WHERE DAY = '2016-07-25'  AND k_record.ven = 900109  GROUP BY DAY,  HOUR,  k_record.amid" +
				" order by k_record.amid,HOUR";*/
		
//		String hiveSql = "select distinct ip from fact_adap_adst_history t where t.ven = 900109 and t.areaparentid=0";
		
		/*String hiveSql = "select amid,sum(exposure) as exposure,	 sum(case when exposure >=1 then 1 else 0 end) as touchone " +
				"  from 	 (select amid,hid,count(sessionid) as exposure 	" +
				"	from adstat where logdate = 20160805 and type=0	and uv = 0 and amid = 1050000021" +
				"	group by hid,amid) as temp1 group by amid";*/
		
		
	/*	String hiveSql = " SELECT hid,sessionid,COUNT(1) AS  exposure " +
				"	FROM ( " +
		" SELECT provinceid,hid,sessionid,1 AS exposure" +
		" FROM adstat " +
		" WHERE TYPE=0 " +
		" AND pv=1 " +
		" 	AND logdate >= 20160801" + 
		" AND logdate <= 20160807 " +
		" AND adposid = 17101110 " + 
		" AND uv = 0  " +
		" AND oemid IN (126,158,176,177,178,180,222,226,227,229,273,276,285,296,297,309,321,323,329,332,337,342,374,380,384,397,414,415,416,425,440,445,446,541,544,546,547,559,566,593,597,598,599,602,610,615,616,617,620,627,636,637,638,639,640,641,644,646,655,658,665,667,669,670,673,674,676,677,680,682,686,690,691,694,698,699,705,706,708,726,729,737,738,739,741,745,746,747,750,757,760,763,764,768,769,772,773,777,778,779,780,784,785,786,788,789,791,792,796,799,807,808,809,814,816,818,819,821,822,828,829,831,833,834,838,839,841,842,843,848,849,851,852,853,854,861,868,869,870,875,877,878,880,881,882,883,889,890,891,892,893,894,895,896,898,100002,100005,100006,100007,100008,100009,100010,100011,100012,100014,100015,100018,100019,100020,100021,100022,100023,100024,100025,100026,100027,100028,100029,100030,100031,100032,100033,100034,100035,100036,100037,100038,100039,100040,100042,100043,100044,100045,100046,100047,100048,100051,100052,100053,100054,100055,100056,100057,100058,100060,100062,100063,100064,100065,100066,100067,100068,100069,100070,100071,100072,100073,100075,100076,100077,100078,100079,100080,100082,100083,100085,100086,100088,100089,100090,100092,100095,100096,100097,100098,100099,100100,100102,100104,100108,100109,100114,100116,100117,100118,100119,100120,100121,100122,100123,100124,100125,100126,100127,100128,100129,100130,100131,100132,100136,100137,100138,100139,100140,100141,100142,100144,100146,100147,100148,100149,100150,100152,100153,100154,100155,100156,100157,100158,100160,100161,100162,100163,100164,100165,100166,100167,100168,100169,100170,100171,100172,100173,100174,100175,100176,100177,100178,100179,100180,100181,100182,100183,100184,100185,100186,100187,100188,100189,100190,100191,100192,100193,100194,100195,100196,100197,100198,100200,100203,100204,100206,100207,100208,100210,100211,100212,100213,100214,100215,100216,100217,100218,100219,100221,100222,150001,150002,150003,150004,150005,150006,150007,150008,150009,160001,160002,160003,160004,160005,160006,160007,160008,160009,160010,170001,170002,170003,170004,170005,170006,170007,180001,180002,180003,180004,180005,180006,180007,180008,200005,200006,200007,200008,200011,200012,200013,200015,200017,200018,200019,200023,200025,200027,200029,200030,200031,200032,200033,200035,200042,200043,200045,200046,200052,200053,200056,200061,200062,250001,300004,300062,400001,401000,500001,500002,500004,500005,500214,600001,725,300025,300136,300003,797,368,225,545,100199,100202,100223,100224,100225,100227,100233,100234,100237,100240,100241,100243,134,135,136,142,143,145,148,153,155,156,157,170008,160013,172,181,182,183,200002,200003,200004,200009,200010,200014,200020,200021,200022,200024,200026,200028,200034,200036,200040,200041,200055,200044,200048,200049,200050,200057,200058,200059,200060,200063,200064,200065,200068,200100,205,208,209,211,212,217,223,258,268,275,278,293,369,303,305,325,335,345,355,367,371,379,386,395,406,409,433,410,411,412,441,442,443,444,447,448,449,532,534,538,565,548,555,606,612,619,622,625,650,654,656,661,679,696,702,727,740,742,781,800002,800008,804,805,815,837,823,847,856,858,859,860,862,886,100074,840,100059,100061,100103,100115,100134,100151,531,681,776,100049)" +  
		" AND provinceid IN (23,12,11,10,24,25,26,27,40,41,42,43,44,45,60,61,62,63,64,65,80,81,82,83,84,95,96,97,98,99,46,01,00,66,67,04,06,05)" +
		"  GROUP BY provinceid,hid,sessionid" +
		"  ) tmp " +
		"  GROUP BY hid,sessionid" +
		"  HAVING COUNT(1)>1" ;*/
//		 String hiveSql = "";
		
		//华扬监测分区域省份
/*		String hiveSql = "select day,areaparentid,count(1) allnum,count(distinct hid) usernum" +
				" from fact_adap_adst_history" +
				" where ven = 900100 " +
				" and amid in  (1050000033,1050000034,1050000035,1050000036,1050000037,1050000038,1050000039,1050000040,1050000041,1050000042,1050000043,1050000044,1050000020)" +
				" and te=0" +
				" and day>='2016-08-12' and day<='2016-08-14' " +
				" group by day,areaparentid" +
				" order by day";*/
		
		//华扬监测分省份按地市累加
		/*String hiveSql = "select day,areaparentid,sum(allnum) allnum,sum(usernum) usernum" +
				" from (select day,areaparentid,areaid,count(1) allnum,count(distinct hid) usernum" +
				" from fact_adap_adst_history" +
				" where ven = 900101 " +
				//" and amid in  (1050000033,1050000034,1050000035,1050000036,1050000037,1050000038,1050000039,1050000040,1050000041,1050000042,1050000043,1050000044,1050000020)" +
				" and amid in  (2016072901)" +
				" and te=0" +
				" and day>='2016-08-10' and day<='2016-08-12' " +
				" group by day,areaparentid,areaid" +
				" ) tmp group by day,areaparentid" +
				" order by day";*/
		 //华扬监测分地市
	/*		String hiveSql = "select day,areaparentid,areaid,count(1) allnum,count(distinct hid) usernum" +
					" from fact_adap_adst_history" +
					" where ven = 900100 " +
					" and amid in  (1050000033,1050000034,1050000035,1050000036,1050000037,1050000038,1050000039,1050000040,1050000041,1050000042,1050000043,1050000044,1050000020)" +
					" and te=0" +
					" and day>='2016-08-12' and day<='2016-08-14' " +
					" group by day,areaparentid,areaid";*/
		//华扬监测分频道
		/*	String hiveSql = "select day,channelid,count(1) allnum,count(distinct hid) usernum" +
		" from fact_adap_adst_history" +
		" where ven = 900100 " +
		" and amid in  (1050000033,1050000034,1050000035,1050000036,1050000037,1050000038,1050000039,1050000040,1050000041,1050000042,1050000043,1050000044,1050000020)" +
		" and te=0" +
		" and day>='2016-08-12' and day<='2016-08-14' " +
		" group by day,channelid" +
		" order by day";*/
		//13、14、15三天，广州电蟒-溜娃，缓冲，暂停，退出分频道数据
/*		String hiveSql = "select day,amid,channelid,count(1) allnum,count(distinct hid) usernum" +
				" from fact_adap_adst_history" +
				" where ven = 900100 " +
				" and amid in  (1050000036,1050000037,1050000038)" +
				" and te=0" +
				" and day>='2016-08-13' and day<='2016-08-15' " +
				" group by day,amid,channelid" +
				" order by day,amid,channelid";*/
		//曝光
/*		String sql = "select logdate,oemid,sum(expsum) expsum" +
				" from (" +
				" select logdate,hid,oemid,adposid,amid,count(distinct sessionid) as expsum " +
				" from adstat where  logdate >= 20160701 and logdate <= 20160822" +
				" and adposid=15101010" +
				" and type=0 and uv=0" +
				" and amid<>0" +
				" and oemid in (0,1,3,4,5,25,26,27,28,29,30,100,101,102,103,104,105,106,108,109,111,112,114,116,118,119,120,121,122,123,124,125,126,130,131,132,133,134,135,136,138,140,141,142,143,145,146,147,148,149,150,153,154,155,156,157,158,160,165,167,169,170,172,176,177,178,179,180,181,182,183,200,201,202,205,207,208,209,210,211,212,215,216,217,218,220,221,222,223,225,226,227,229,230,231,232,235,236,237,239,240,241,242,243,244,245,246,249,250,251,256,257,258,260,261,262,264,268,270,271,272,273,274,275,276,278,280,281,282,283,285,288,289,290,291,292,293,294,296,297,299,300,301,302,303,304,305,307,309,310,312,313,314,315,316,318,319,320,321,322,323,324,325,326,327,328,329,330,331,332,333,334,335,336,337,338,339,340,341,342,343,344,345,346,347,348,350,351,352,355,356,357,358,359,360,361,365,367,368,369,370,371,372,374,379,380,384,386,393,394,395,396,397,400,401,402,403,404,405,406,407,409,410,411,412,414,415,416,417,418,419,420,421,422,425,427,428,429,430,431,432,433,434,440,441,442,443,444,445,446,447,448,449,450,451,452,453,454,455,456,457,458,459,460,461,462,463,464,465,469,470,471,472,477,489,490,491,492,493,494,495,496,497,498,500,501,502,503,504,505,506,507,508,509,510,511,512,513,514,515,516,517,519,520,521,522,523,531,532,534,535,537,538,539,540,541,542,543,544,545,546,547,548,549,550,551,552,553,554,555,556,557,558,559,565,566,567,568,588,589,590,592,593,594,595,597,598,599,601,602,605,606,607,609,610,612,613,614,615,616,617,618,619,620,622,625,626,627,628,629,630,631,634,635,636,637,638,639,640,641,642,643,644,645,646,647,649,650,654,655,656,657,658,659,661,664,665,666,667,668,669,670,671,672,673,674,675,676,677,678,679,680,681,682,684,686,687,688,689,690,691,694,695,696,697,698,699,700,701,702,703,705,706,707,708,709,711,712,713,714,715,716,717,718,719,720,721,722,723,724,725,726,727,728,729,730,731,732,733,734,735,736,737,738,739,740,741,742,743,744,745,746,747,748,749,750,751,752,753,754,755,756,757,758,759,760,762,763,764,765,766,767,768,769,770,771,772,773,774,776,777,778,779,780,781,782,783,784,785,786,787,788,789,791,792,793,794,795,796,797,798,799,800,801,802,803,804,805,807,808,809,810,811,812,814,815,816,817,818,819,820,821,822,823,824,825,826,827,828,829,830,831,832,833,834,835,836,837,838,839,840,841,842,843,844,845,846,847,848,849,850,851,852,853,854,855,856,857,858,859,860,861,862,863,864,865,866,867,868,869,870,871,872,873,874,875,876,877,878,879,880,881,882,883,884,885,886,887,888,889,890,891,892,893,894,895,896,897,898,899,920,998,999,1001,1111,1222,2000,3000,8801,8802,8803,8882,10000,10001,10002,10003,10004,10005,10006,10007,10008,10009,10010,10011,10012,10013,10014,10015,10016,10017,10018,10019,10020,10021,10022,10023,10024,10025,10026,10027,10028,10029,10030,10031,10032,10033,10034,10035,10036,10037,10038,10039,10040,10041,10042,10043,10044,10045,10046,10047,10048,10049,10050,10051,10052,10053,10054,10055,10056,10057,10058,10059,10060,10061,10062,10063,10064,10065,10066,10067,10068,10069,11111,12345,12522,15000,15001,15002,17000,20000,20001,20002,20003,20004,20005,20006,20007,20008,20009,20010,20011,30019,30020,30021,30022,30023,30024,30025,30026,30027,30028,30029,30030,30031,30032,30033,30034,30035,30036,30037,30038,30039,30040,30041,30042,30043,30045,30046,30047,30048,30049,30050,30051,30052,30053,30054,30055,30058,30059,30060,30061,30062,30063,30064,30065,40000,40001,40002,40003,40004,40005,40006,40007,40008,40009,40010,45000,45001,45002,45003,45004,45005,45006,46000,49000,49001,49002,49003,49004,49005,49006,49007,100001,100002,100005,100006,100007,100008,100009,100010,100011,100012,100013,100014,100015,100016,100017,100018,100019,100020,100021,100022,100023,100024,100025,100026,100027,100028,100029,100030,100031,100032,100033,100034,100035,100036,100037,100038,100039,100040,100041,100042,100043,100044,100045,100046,100047,100048,100049,100051,100052,100053,100054,100055,100056,100057,100058,100059,100060,100061,100062,100063,100064,100065,100066,100067,100068,100069,100070,100071,100072,100073,100074,100075,100076,100077,100078,100079,100080,100081,100082,100083,100084,100085,100086,100087,100088,100089,100090,100091,100092,100093,100094,100095,100096,100097,100098,100099,100100,100101,100102,100103,100104,100105,100106,100107,100108,100109,100110,100111,100112,100113,100114,100115,100116,100117,100118,100119,100120,100121,100122,100123,100124,100125,100126,100127,100128,100129,100130,100131,100132,100133,100134,100136,100137,100138,100139,100140,100141,100142,100144,100145,100146,100147,100148,100149,100150,100151,100152,100153,100154,100155,100156,100157,100158,100160,100161,100162,100163,100164,100165,100166,100167,100168,100169,100170,100171,100172,100173,100174,100175,100176,100177,100178,100179,100180,100181,100182,100183,100184,100185,100186,100187,100188,100189,100190,100191,100192,100193,100194,100195,100196,100197,100198,100199,100200,100201,100202,100203,100204,100206,100207,100208,100209,100210,100211,100212,100213,100214,100215,100216,100217,100218,100219,100220,100221,100222,100223,100224,100225,100226,100227,100230,100231,100232,100233,100234,100235,100236,100237,100238,100239,100240,100241,100242,100243,100244,100245,100246,100247,100248,100249,100250,100251,100252,100253,100254,100255,100256,100257,100258,100259,100260,100261,100262,100762,100817,110263,110264,150001,150002,150003,150004,150005,150006,150007,150008,150009,150010,150011,150012,150013,150014,160001,160002,160003,160004,160005,160006,160007,160008,160009,160010,160013,170001,170002,170003,170004,170005,170006,170007,170008,170009,170010,180001,180002,180003,180004,180005,180006,180007,180008,200002,200003,200004,200005,200006,200007,200008,200009,200010,200011,200012,200013,200014,200015,200016,200017,200018,200019,200020,200021,200022,200023,200024,200025,200026,200027,200028,200029,200030,200031,200032,200033,200034,200035,200036,200037,200038,200039,200040,200041,200042,200043,200044,200045,200046,200047,200048,200049,200050,200051,200052,200053,200054,200055,200056,200057,200058,200060,200061,200062,200063,200064,200065,200066,200067,200068,200069,200100,220070,220071,250001,250002,300001,300002,300003,300004,300005,300006,300007,300008,300009,300010,300011,300012,300013,300014,300015,300016,300017,300018,300019,300020,300021,300022,300023,300024,300025,300026,300027,300028,300029,300030,300031,300032,300033,300034,300035,300036,300037,300038,300039,300040,300041,300042,300043,300044,300045,300046,300047,300048,300049,300050,300051,300052,300053,300054,300055,300056,300057,300058,300060,300061,300062,300063,300064,300065,300066,300067,300068,300069,300070,300071,300072,300073,300074,300075,300076,300077,300078,300079,300080,300081,300082,300083,300084,300085,300086,300087,300088,300089,300090,300091,300092,300093,300094,300095,300096,300097,300098,300099,300101,300102,300103,300104,300105,300106,300107,300108,300109,300110,300111,300112,300113,300114,300115,300116,300117,300119,300120,300121,300122,300123,300124,300125,300126,300127,300128,300129,300130,300131,300132,300133,300134,300135,300136,300137,300138,300139,300140,300141,300142,300143,300144,300145,300146,300147,300148,300149,300150,300151,300152,300153,300154,300155,300156,300157,300158,300159,300160,300161,300162,300163,300164,300165,300166,300167,300169,300170,300171,300172,300173,300174,300175,300176,300177,300178,300179,300180,300181,300182,300183,300184,300185,300186,300187,300188,300189,300190,300191,300192,300193,300194,300195,300196,300197,300198,330199,400001,400002,401000,500001,500002,500003,500004,500005,500214,600001,800002,800006,800008,800014,800017,800024,800035,800044,800047,800055,800099,800801,800802,800803,909090,999999,1234567,20150819,123456789)" +
				" group by logdate,hid,oemid,adposid,amid" +
				" ) tmp" +
				" group by logdate,oemid";*/
		//触达
		/*String sql = "select logdate,oemid,count(distinct hid) as expsum " +
				" from adstat where  logdate >= 20160701 and logdate <= 20160822" +
				" and adposid=15101010" +
				" and type=0 and uv=0" +
				" and amid<>0" +
				" and oemid in (0,1,3,4,5,25,26,27,28,29,30,100,101,102,103,104,105,106,108,109,111,112,114,116,118,119,120,121,122,123,124,125,126,130,131,132,133,134,135,136,138,140,141,142,143,145,146,147,148,149,150,153,154,155,156,157,158,160,165,167,169,170,172,176,177,178,179,180,181,182,183,200,201,202,205,207,208,209,210,211,212,215,216,217,218,220,221,222,223,225,226,227,229,230,231,232,235,236,237,239,240,241,242,243,244,245,246,249,250,251,256,257,258,260,261,262,264,268,270,271,272,273,274,275,276,278,280,281,282,283,285,288,289,290,291,292,293,294,296,297,299,300,301,302,303,304,305,307,309,310,312,313,314,315,316,318,319,320,321,322,323,324,325,326,327,328,329,330,331,332,333,334,335,336,337,338,339,340,341,342,343,344,345,346,347,348,350,351,352,355,356,357,358,359,360,361,365,367,368,369,370,371,372,374,379,380,384,386,393,394,395,396,397,400,401,402,403,404,405,406,407,409,410,411,412,414,415,416,417,418,419,420,421,422,425,427,428,429,430,431,432,433,434,440,441,442,443,444,445,446,447,448,449,450,451,452,453,454,455,456,457,458,459,460,461,462,463,464,465,469,470,471,472,477,489,490,491,492,493,494,495,496,497,498,500,501,502,503,504,505,506,507,508,509,510,511,512,513,514,515,516,517,519,520,521,522,523,531,532,534,535,537,538,539,540,541,542,543,544,545,546,547,548,549,550,551,552,553,554,555,556,557,558,559,565,566,567,568,588,589,590,592,593,594,595,597,598,599,601,602,605,606,607,609,610,612,613,614,615,616,617,618,619,620,622,625,626,627,628,629,630,631,634,635,636,637,638,639,640,641,642,643,644,645,646,647,649,650,654,655,656,657,658,659,661,664,665,666,667,668,669,670,671,672,673,674,675,676,677,678,679,680,681,682,684,686,687,688,689,690,691,694,695,696,697,698,699,700,701,702,703,705,706,707,708,709,711,712,713,714,715,716,717,718,719,720,721,722,723,724,725,726,727,728,729,730,731,732,733,734,735,736,737,738,739,740,741,742,743,744,745,746,747,748,749,750,751,752,753,754,755,756,757,758,759,760,762,763,764,765,766,767,768,769,770,771,772,773,774,776,777,778,779,780,781,782,783,784,785,786,787,788,789,791,792,793,794,795,796,797,798,799,800,801,802,803,804,805,807,808,809,810,811,812,814,815,816,817,818,819,820,821,822,823,824,825,826,827,828,829,830,831,832,833,834,835,836,837,838,839,840,841,842,843,844,845,846,847,848,849,850,851,852,853,854,855,856,857,858,859,860,861,862,863,864,865,866,867,868,869,870,871,872,873,874,875,876,877,878,879,880,881,882,883,884,885,886,887,888,889,890,891,892,893,894,895,896,897,898,899,920,998,999,1001,1111,1222,2000,3000,8801,8802,8803,8882,10000,10001,10002,10003,10004,10005,10006,10007,10008,10009,10010,10011,10012,10013,10014,10015,10016,10017,10018,10019,10020,10021,10022,10023,10024,10025,10026,10027,10028,10029,10030,10031,10032,10033,10034,10035,10036,10037,10038,10039,10040,10041,10042,10043,10044,10045,10046,10047,10048,10049,10050,10051,10052,10053,10054,10055,10056,10057,10058,10059,10060,10061,10062,10063,10064,10065,10066,10067,10068,10069,11111,12345,12522,15000,15001,15002,17000,20000,20001,20002,20003,20004,20005,20006,20007,20008,20009,20010,20011,30019,30020,30021,30022,30023,30024,30025,30026,30027,30028,30029,30030,30031,30032,30033,30034,30035,30036,30037,30038,30039,30040,30041,30042,30043,30045,30046,30047,30048,30049,30050,30051,30052,30053,30054,30055,30058,30059,30060,30061,30062,30063,30064,30065,40000,40001,40002,40003,40004,40005,40006,40007,40008,40009,40010,45000,45001,45002,45003,45004,45005,45006,46000,49000,49001,49002,49003,49004,49005,49006,49007,100001,100002,100005,100006,100007,100008,100009,100010,100011,100012,100013,100014,100015,100016,100017,100018,100019,100020,100021,100022,100023,100024,100025,100026,100027,100028,100029,100030,100031,100032,100033,100034,100035,100036,100037,100038,100039,100040,100041,100042,100043,100044,100045,100046,100047,100048,100049,100051,100052,100053,100054,100055,100056,100057,100058,100059,100060,100061,100062,100063,100064,100065,100066,100067,100068,100069,100070,100071,100072,100073,100074,100075,100076,100077,100078,100079,100080,100081,100082,100083,100084,100085,100086,100087,100088,100089,100090,100091,100092,100093,100094,100095,100096,100097,100098,100099,100100,100101,100102,100103,100104,100105,100106,100107,100108,100109,100110,100111,100112,100113,100114,100115,100116,100117,100118,100119,100120,100121,100122,100123,100124,100125,100126,100127,100128,100129,100130,100131,100132,100133,100134,100136,100137,100138,100139,100140,100141,100142,100144,100145,100146,100147,100148,100149,100150,100151,100152,100153,100154,100155,100156,100157,100158,100160,100161,100162,100163,100164,100165,100166,100167,100168,100169,100170,100171,100172,100173,100174,100175,100176,100177,100178,100179,100180,100181,100182,100183,100184,100185,100186,100187,100188,100189,100190,100191,100192,100193,100194,100195,100196,100197,100198,100199,100200,100201,100202,100203,100204,100206,100207,100208,100209,100210,100211,100212,100213,100214,100215,100216,100217,100218,100219,100220,100221,100222,100223,100224,100225,100226,100227,100230,100231,100232,100233,100234,100235,100236,100237,100238,100239,100240,100241,100242,100243,100244,100245,100246,100247,100248,100249,100250,100251,100252,100253,100254,100255,100256,100257,100258,100259,100260,100261,100262,100762,100817,110263,110264,150001,150002,150003,150004,150005,150006,150007,150008,150009,150010,150011,150012,150013,150014,160001,160002,160003,160004,160005,160006,160007,160008,160009,160010,160013,170001,170002,170003,170004,170005,170006,170007,170008,170009,170010,180001,180002,180003,180004,180005,180006,180007,180008,200002,200003,200004,200005,200006,200007,200008,200009,200010,200011,200012,200013,200014,200015,200016,200017,200018,200019,200020,200021,200022,200023,200024,200025,200026,200027,200028,200029,200030,200031,200032,200033,200034,200035,200036,200037,200038,200039,200040,200041,200042,200043,200044,200045,200046,200047,200048,200049,200050,200051,200052,200053,200054,200055,200056,200057,200058,200060,200061,200062,200063,200064,200065,200066,200067,200068,200069,200100,220070,220071,250001,250002,300001,300002,300003,300004,300005,300006,300007,300008,300009,300010,300011,300012,300013,300014,300015,300016,300017,300018,300019,300020,300021,300022,300023,300024,300025,300026,300027,300028,300029,300030,300031,300032,300033,300034,300035,300036,300037,300038,300039,300040,300041,300042,300043,300044,300045,300046,300047,300048,300049,300050,300051,300052,300053,300054,300055,300056,300057,300058,300060,300061,300062,300063,300064,300065,300066,300067,300068,300069,300070,300071,300072,300073,300074,300075,300076,300077,300078,300079,300080,300081,300082,300083,300084,300085,300086,300087,300088,300089,300090,300091,300092,300093,300094,300095,300096,300097,300098,300099,300101,300102,300103,300104,300105,300106,300107,300108,300109,300110,300111,300112,300113,300114,300115,300116,300117,300119,300120,300121,300122,300123,300124,300125,300126,300127,300128,300129,300130,300131,300132,300133,300134,300135,300136,300137,300138,300139,300140,300141,300142,300143,300144,300145,300146,300147,300148,300149,300150,300151,300152,300153,300154,300155,300156,300157,300158,300159,300160,300161,300162,300163,300164,300165,300166,300167,300169,300170,300171,300172,300173,300174,300175,300176,300177,300178,300179,300180,300181,300182,300183,300184,300185,300186,300187,300188,300189,300190,300191,300192,300193,300194,300195,300196,300197,300198,330199,400001,400002,401000,500001,500002,500003,500004,500005,500214,600001,800002,800006,800008,800014,800017,800024,800035,800044,800047,800055,800099,800801,800802,800803,909090,999999,1234567,20150819,123456789)" +
				" group by logdate,oemid";*/
	
		
		//曝光，包括节目，计算节目时长
		/*String sql = "select logdate,oemid,amid,sum(expsum) expsum" +
		" from (" +
		" select logdate,hid,oemid,adposid,amid,count(distinct sessionid) as expsum " +
		" from adstat where  logdate >= 20160701 and logdate <= 20160822" +
		" and adposid=15101010" +
		" and type=0 and uv=0" +
		" and amid<>0" +
		" and oemid in (0,1,3,4,5,25,26,27,28,29,30,100,101,102,103,104,105,106,108,109,111,112,114,116,118,119,120,121,122,123,124,125,126,130,131,132,133,134,135,136,138,140,141,142,143,145,146,147,148,149,150,153,154,155,156,157,158,160,165,167,169,170,172,176,177,178,179,180,181,182,183,200,201,202,205,207,208,209,210,211,212,215,216,217,218,220,221,222,223,225,226,227,229,230,231,232,235,236,237,239,240,241,242,243,244,245,246,249,250,251,256,257,258,260,261,262,264,268,270,271,272,273,274,275,276,278,280,281,282,283,285,288,289,290,291,292,293,294,296,297,299,300,301,302,303,304,305,307,309,310,312,313,314,315,316,318,319,320,321,322,323,324,325,326,327,328,329,330,331,332,333,334,335,336,337,338,339,340,341,342,343,344,345,346,347,348,350,351,352,355,356,357,358,359,360,361,365,367,368,369,370,371,372,374,379,380,384,386,393,394,395,396,397,400,401,402,403,404,405,406,407,409,410,411,412,414,415,416,417,418,419,420,421,422,425,427,428,429,430,431,432,433,434,440,441,442,443,444,445,446,447,448,449,450,451,452,453,454,455,456,457,458,459,460,461,462,463,464,465,469,470,471,472,477,489,490,491,492,493,494,495,496,497,498,500,501,502,503,504,505,506,507,508,509,510,511,512,513,514,515,516,517,519,520,521,522,523,531,532,534,535,537,538,539,540,541,542,543,544,545,546,547,548,549,550,551,552,553,554,555,556,557,558,559,565,566,567,568,588,589,590,592,593,594,595,597,598,599,601,602,605,606,607,609,610,612,613,614,615,616,617,618,619,620,622,625,626,627,628,629,630,631,634,635,636,637,638,639,640,641,642,643,644,645,646,647,649,650,654,655,656,657,658,659,661,664,665,666,667,668,669,670,671,672,673,674,675,676,677,678,679,680,681,682,684,686,687,688,689,690,691,694,695,696,697,698,699,700,701,702,703,705,706,707,708,709,711,712,713,714,715,716,717,718,719,720,721,722,723,724,725,726,727,728,729,730,731,732,733,734,735,736,737,738,739,740,741,742,743,744,745,746,747,748,749,750,751,752,753,754,755,756,757,758,759,760,762,763,764,765,766,767,768,769,770,771,772,773,774,776,777,778,779,780,781,782,783,784,785,786,787,788,789,791,792,793,794,795,796,797,798,799,800,801,802,803,804,805,807,808,809,810,811,812,814,815,816,817,818,819,820,821,822,823,824,825,826,827,828,829,830,831,832,833,834,835,836,837,838,839,840,841,842,843,844,845,846,847,848,849,850,851,852,853,854,855,856,857,858,859,860,861,862,863,864,865,866,867,868,869,870,871,872,873,874,875,876,877,878,879,880,881,882,883,884,885,886,887,888,889,890,891,892,893,894,895,896,897,898,899,920,998,999,1001,1111,1222,2000,3000,8801,8802,8803,8882,10000,10001,10002,10003,10004,10005,10006,10007,10008,10009,10010,10011,10012,10013,10014,10015,10016,10017,10018,10019,10020,10021,10022,10023,10024,10025,10026,10027,10028,10029,10030,10031,10032,10033,10034,10035,10036,10037,10038,10039,10040,10041,10042,10043,10044,10045,10046,10047,10048,10049,10050,10051,10052,10053,10054,10055,10056,10057,10058,10059,10060,10061,10062,10063,10064,10065,10066,10067,10068,10069,11111,12345,12522,15000,15001,15002,17000,20000,20001,20002,20003,20004,20005,20006,20007,20008,20009,20010,20011,30019,30020,30021,30022,30023,30024,30025,30026,30027,30028,30029,30030,30031,30032,30033,30034,30035,30036,30037,30038,30039,30040,30041,30042,30043,30045,30046,30047,30048,30049,30050,30051,30052,30053,30054,30055,30058,30059,30060,30061,30062,30063,30064,30065,40000,40001,40002,40003,40004,40005,40006,40007,40008,40009,40010,45000,45001,45002,45003,45004,45005,45006,46000,49000,49001,49002,49003,49004,49005,49006,49007,100001,100002,100005,100006,100007,100008,100009,100010,100011,100012,100013,100014,100015,100016,100017,100018,100019,100020,100021,100022,100023,100024,100025,100026,100027,100028,100029,100030,100031,100032,100033,100034,100035,100036,100037,100038,100039,100040,100041,100042,100043,100044,100045,100046,100047,100048,100049,100051,100052,100053,100054,100055,100056,100057,100058,100059,100060,100061,100062,100063,100064,100065,100066,100067,100068,100069,100070,100071,100072,100073,100074,100075,100076,100077,100078,100079,100080,100081,100082,100083,100084,100085,100086,100087,100088,100089,100090,100091,100092,100093,100094,100095,100096,100097,100098,100099,100100,100101,100102,100103,100104,100105,100106,100107,100108,100109,100110,100111,100112,100113,100114,100115,100116,100117,100118,100119,100120,100121,100122,100123,100124,100125,100126,100127,100128,100129,100130,100131,100132,100133,100134,100136,100137,100138,100139,100140,100141,100142,100144,100145,100146,100147,100148,100149,100150,100151,100152,100153,100154,100155,100156,100157,100158,100160,100161,100162,100163,100164,100165,100166,100167,100168,100169,100170,100171,100172,100173,100174,100175,100176,100177,100178,100179,100180,100181,100182,100183,100184,100185,100186,100187,100188,100189,100190,100191,100192,100193,100194,100195,100196,100197,100198,100199,100200,100201,100202,100203,100204,100206,100207,100208,100209,100210,100211,100212,100213,100214,100215,100216,100217,100218,100219,100220,100221,100222,100223,100224,100225,100226,100227,100230,100231,100232,100233,100234,100235,100236,100237,100238,100239,100240,100241,100242,100243,100244,100245,100246,100247,100248,100249,100250,100251,100252,100253,100254,100255,100256,100257,100258,100259,100260,100261,100262,100762,100817,110263,110264,150001,150002,150003,150004,150005,150006,150007,150008,150009,150010,150011,150012,150013,150014,160001,160002,160003,160004,160005,160006,160007,160008,160009,160010,160013,170001,170002,170003,170004,170005,170006,170007,170008,170009,170010,180001,180002,180003,180004,180005,180006,180007,180008,200002,200003,200004,200005,200006,200007,200008,200009,200010,200011,200012,200013,200014,200015,200016,200017,200018,200019,200020,200021,200022,200023,200024,200025,200026,200027,200028,200029,200030,200031,200032,200033,200034,200035,200036,200037,200038,200039,200040,200041,200042,200043,200044,200045,200046,200047,200048,200049,200050,200051,200052,200053,200054,200055,200056,200057,200058,200060,200061,200062,200063,200064,200065,200066,200067,200068,200069,200100,220070,220071,250001,250002,300001,300002,300003,300004,300005,300006,300007,300008,300009,300010,300011,300012,300013,300014,300015,300016,300017,300018,300019,300020,300021,300022,300023,300024,300025,300026,300027,300028,300029,300030,300031,300032,300033,300034,300035,300036,300037,300038,300039,300040,300041,300042,300043,300044,300045,300046,300047,300048,300049,300050,300051,300052,300053,300054,300055,300056,300057,300058,300060,300061,300062,300063,300064,300065,300066,300067,300068,300069,300070,300071,300072,300073,300074,300075,300076,300077,300078,300079,300080,300081,300082,300083,300084,300085,300086,300087,300088,300089,300090,300091,300092,300093,300094,300095,300096,300097,300098,300099,300101,300102,300103,300104,300105,300106,300107,300108,300109,300110,300111,300112,300113,300114,300115,300116,300117,300119,300120,300121,300122,300123,300124,300125,300126,300127,300128,300129,300130,300131,300132,300133,300134,300135,300136,300137,300138,300139,300140,300141,300142,300143,300144,300145,300146,300147,300148,300149,300150,300151,300152,300153,300154,300155,300156,300157,300158,300159,300160,300161,300162,300163,300164,300165,300166,300167,300169,300170,300171,300172,300173,300174,300175,300176,300177,300178,300179,300180,300181,300182,300183,300184,300185,300186,300187,300188,300189,300190,300191,300192,300193,300194,300195,300196,300197,300198,330199,400001,400002,401000,500001,500002,500003,500004,500005,500214,600001,800002,800006,800008,800014,800017,800024,800035,800044,800047,800055,800099,800801,800802,800803,909090,999999,1234567,20150819,123456789)" +
		" group by logdate,hid,oemid,adposid,amid" +
		" ) tmp" +
		" group by logdate,oemid,amid";*/
		
	/*	分日节目时长汇总 */
/*		String sql = "select logdate,amid,sum(expsum) expsum" +
				" from (" +
				" select logdate,hid,oemid,adposid,amid,count(distinct sessionid) as expsum " +
				" from adstat where  logdate >= 20160701 and logdate <= 20160822" +
				" and adposid=15101010" +
				" and type=0 and uv=0" +
				" and amid<>0" +
				" and oemid in (0,1,3,4,5,25,26,27,28,29,30,100,101,102,103,104,105,106,108,109,111,112,114,116,118,119,120,121,122,123,124,125,126,130,131,132,133,134,135,136,138,140,141,142,143,145,146,147,148,149,150,153,154,155,156,157,158,160,165,167,169,170,172,176,177,178,179,180,181,182,183,200,201,202,205,207,208,209,210,211,212,215,216,217,218,220,221,222,223,225,226,227,229,230,231,232,235,236,237,239,240,241,242,243,244,245,246,249,250,251,256,257,258,260,261,262,264,268,270,271,272,273,274,275,276,278,280,281,282,283,285,288,289,290,291,292,293,294,296,297,299,300,301,302,303,304,305,307,309,310,312,313,314,315,316,318,319,320,321,322,323,324,325,326,327,328,329,330,331,332,333,334,335,336,337,338,339,340,341,342,343,344,345,346,347,348,350,351,352,355,356,357,358,359,360,361,365,367,368,369,370,371,372,374,379,380,384,386,393,394,395,396,397,400,401,402,403,404,405,406,407,409,410,411,412,414,415,416,417,418,419,420,421,422,425,427,428,429,430,431,432,433,434,440,441,442,443,444,445,446,447,448,449,450,451,452,453,454,455,456,457,458,459,460,461,462,463,464,465,469,470,471,472,477,489,490,491,492,493,494,495,496,497,498,500,501,502,503,504,505,506,507,508,509,510,511,512,513,514,515,516,517,519,520,521,522,523,531,532,534,535,537,538,539,540,541,542,543,544,545,546,547,548,549,550,551,552,553,554,555,556,557,558,559,565,566,567,568,588,589,590,592,593,594,595,597,598,599,601,602,605,606,607,609,610,612,613,614,615,616,617,618,619,620,622,625,626,627,628,629,630,631,634,635,636,637,638,639,640,641,642,643,644,645,646,647,649,650,654,655,656,657,658,659,661,664,665,666,667,668,669,670,671,672,673,674,675,676,677,678,679,680,681,682,684,686,687,688,689,690,691,694,695,696,697,698,699,700,701,702,703,705,706,707,708,709,711,712,713,714,715,716,717,718,719,720,721,722,723,724,725,726,727,728,729,730,731,732,733,734,735,736,737,738,739,740,741,742,743,744,745,746,747,748,749,750,751,752,753,754,755,756,757,758,759,760,762,763,764,765,766,767,768,769,770,771,772,773,774,776,777,778,779,780,781,782,783,784,785,786,787,788,789,791,792,793,794,795,796,797,798,799,800,801,802,803,804,805,807,808,809,810,811,812,814,815,816,817,818,819,820,821,822,823,824,825,826,827,828,829,830,831,832,833,834,835,836,837,838,839,840,841,842,843,844,845,846,847,848,849,850,851,852,853,854,855,856,857,858,859,860,861,862,863,864,865,866,867,868,869,870,871,872,873,874,875,876,877,878,879,880,881,882,883,884,885,886,887,888,889,890,891,892,893,894,895,896,897,898,899,920,998,999,1001,1111,1222,2000,3000,8801,8802,8803,8882,10000,10001,10002,10003,10004,10005,10006,10007,10008,10009,10010,10011,10012,10013,10014,10015,10016,10017,10018,10019,10020,10021,10022,10023,10024,10025,10026,10027,10028,10029,10030,10031,10032,10033,10034,10035,10036,10037,10038,10039,10040,10041,10042,10043,10044,10045,10046,10047,10048,10049,10050,10051,10052,10053,10054,10055,10056,10057,10058,10059,10060,10061,10062,10063,10064,10065,10066,10067,10068,10069,11111,12345,12522,15000,15001,15002,17000,20000,20001,20002,20003,20004,20005,20006,20007,20008,20009,20010,20011,30019,30020,30021,30022,30023,30024,30025,30026,30027,30028,30029,30030,30031,30032,30033,30034,30035,30036,30037,30038,30039,30040,30041,30042,30043,30045,30046,30047,30048,30049,30050,30051,30052,30053,30054,30055,30058,30059,30060,30061,30062,30063,30064,30065,40000,40001,40002,40003,40004,40005,40006,40007,40008,40009,40010,45000,45001,45002,45003,45004,45005,45006,46000,49000,49001,49002,49003,49004,49005,49006,49007,100001,100002,100005,100006,100007,100008,100009,100010,100011,100012,100013,100014,100015,100016,100017,100018,100019,100020,100021,100022,100023,100024,100025,100026,100027,100028,100029,100030,100031,100032,100033,100034,100035,100036,100037,100038,100039,100040,100041,100042,100043,100044,100045,100046,100047,100048,100049,100051,100052,100053,100054,100055,100056,100057,100058,100059,100060,100061,100062,100063,100064,100065,100066,100067,100068,100069,100070,100071,100072,100073,100074,100075,100076,100077,100078,100079,100080,100081,100082,100083,100084,100085,100086,100087,100088,100089,100090,100091,100092,100093,100094,100095,100096,100097,100098,100099,100100,100101,100102,100103,100104,100105,100106,100107,100108,100109,100110,100111,100112,100113,100114,100115,100116,100117,100118,100119,100120,100121,100122,100123,100124,100125,100126,100127,100128,100129,100130,100131,100132,100133,100134,100136,100137,100138,100139,100140,100141,100142,100144,100145,100146,100147,100148,100149,100150,100151,100152,100153,100154,100155,100156,100157,100158,100160,100161,100162,100163,100164,100165,100166,100167,100168,100169,100170,100171,100172,100173,100174,100175,100176,100177,100178,100179,100180,100181,100182,100183,100184,100185,100186,100187,100188,100189,100190,100191,100192,100193,100194,100195,100196,100197,100198,100199,100200,100201,100202,100203,100204,100206,100207,100208,100209,100210,100211,100212,100213,100214,100215,100216,100217,100218,100219,100220,100221,100222,100223,100224,100225,100226,100227,100230,100231,100232,100233,100234,100235,100236,100237,100238,100239,100240,100241,100242,100243,100244,100245,100246,100247,100248,100249,100250,100251,100252,100253,100254,100255,100256,100257,100258,100259,100260,100261,100262,100762,100817,110263,110264,150001,150002,150003,150004,150005,150006,150007,150008,150009,150010,150011,150012,150013,150014,160001,160002,160003,160004,160005,160006,160007,160008,160009,160010,160013,170001,170002,170003,170004,170005,170006,170007,170008,170009,170010,180001,180002,180003,180004,180005,180006,180007,180008,200002,200003,200004,200005,200006,200007,200008,200009,200010,200011,200012,200013,200014,200015,200016,200017,200018,200019,200020,200021,200022,200023,200024,200025,200026,200027,200028,200029,200030,200031,200032,200033,200034,200035,200036,200037,200038,200039,200040,200041,200042,200043,200044,200045,200046,200047,200048,200049,200050,200051,200052,200053,200054,200055,200056,200057,200058,200060,200061,200062,200063,200064,200065,200066,200067,200068,200069,200100,220070,220071,250001,250002,300001,300002,300003,300004,300005,300006,300007,300008,300009,300010,300011,300012,300013,300014,300015,300016,300017,300018,300019,300020,300021,300022,300023,300024,300025,300026,300027,300028,300029,300030,300031,300032,300033,300034,300035,300036,300037,300038,300039,300040,300041,300042,300043,300044,300045,300046,300047,300048,300049,300050,300051,300052,300053,300054,300055,300056,300057,300058,300060,300061,300062,300063,300064,300065,300066,300067,300068,300069,300070,300071,300072,300073,300074,300075,300076,300077,300078,300079,300080,300081,300082,300083,300084,300085,300086,300087,300088,300089,300090,300091,300092,300093,300094,300095,300096,300097,300098,300099,300101,300102,300103,300104,300105,300106,300107,300108,300109,300110,300111,300112,300113,300114,300115,300116,300117,300119,300120,300121,300122,300123,300124,300125,300126,300127,300128,300129,300130,300131,300132,300133,300134,300135,300136,300137,300138,300139,300140,300141,300142,300143,300144,300145,300146,300147,300148,300149,300150,300151,300152,300153,300154,300155,300156,300157,300158,300159,300160,300161,300162,300163,300164,300165,300166,300167,300169,300170,300171,300172,300173,300174,300175,300176,300177,300178,300179,300180,300181,300182,300183,300184,300185,300186,300187,300188,300189,300190,300191,300192,300193,300194,300195,300196,300197,300198,330199,400001,400002,401000,500001,500002,500003,500004,500005,500214,600001,800002,800006,800008,800014,800017,800024,800035,800044,800047,800055,800099,800801,800802,800803,909090,999999,1234567,20150819,123456789)" +
				" group by logdate,hid,oemid,adposid,amid" +
				" ) tmp" +
				" group by logdate,oemid,amid";*/
		
		/*	分日曝光汇总	*/
/*		 String sql = "select logdate,sum(expsum) expsum,count(distinct hid) as usersum" +
		" from (" +
		" select logdate,hid,oemid,adposid,amid,count(distinct sessionid) as expsum " +
		" from adstat where  logdate >= 20160818 and logdate <= 20160831" +
		" and adposid=15101010" +
		" and type=0 and uv=0" +
		" and amid<>0" +
		" and oemid in (0,1,3,4,5,25,26,27,28,29,30,100,101,102,103,104,105,106,108,109,111,112,114,116,118,119,120,121,122,123,124,125,126,130,131,132,133,134,135,136,138,140,141,142,143,145,146,147,148,149,150,153,154,155,156,157,158,160,165,167,169,170,172,176,177,178,179,180,181,182,183,200,201,202,205,207,208,209,210,211,212,215,216,217,218,220,221,222,223,225,226,227,229,230,231,232,235,236,237,239,240,241,242,243,244,245,246,249,250,251,256,257,258,260,261,262,264,268,270,271,272,273,274,275,276,278,280,281,282,283,285,288,289,290,291,292,293,294,296,297,299,300,301,302,303,304,305,307,309,310,312,313,314,315,316,318,319,320,321,322,323,324,325,326,327,328,329,330,331,332,333,334,335,336,337,338,339,340,341,342,343,344,345,346,347,348,350,351,352,355,356,357,358,359,360,361,365,367,368,369,370,371,372,374,379,380,384,386,393,394,395,396,397,400,401,402,403,404,405,406,407,409,410,411,412,414,415,416,417,418,419,420,421,422,425,427,428,429,430,431,432,433,434,440,441,442,443,444,445,446,447,448,449,450,451,452,453,454,455,456,457,458,459,460,461,462,463,464,465,469,470,471,472,477,489,490,491,492,493,494,495,496,497,498,500,501,502,503,504,505,506,507,508,509,510,511,512,513,514,515,516,517,519,520,521,522,523,531,532,534,535,537,538,539,540,541,542,543,544,545,546,547,548,549,550,551,552,553,554,555,556,557,558,559,565,566,567,568,588,589,590,592,593,594,595,597,598,599,601,602,605,606,607,609,610,612,613,614,615,616,617,618,619,620,622,625,626,627,628,629,630,631,634,635,636,637,638,639,640,641,642,643,644,645,646,647,649,650,654,655,656,657,658,659,661,664,665,666,667,668,669,670,671,672,673,674,675,676,677,678,679,680,681,682,684,686,687,688,689,690,691,694,695,696,697,698,699,700,701,702,703,705,706,707,708,709,711,712,713,714,715,716,717,718,719,720,721,722,723,724,725,726,727,728,729,730,731,732,733,734,735,736,737,738,739,740,741,742,743,744,745,746,747,748,749,750,751,752,753,754,755,756,757,758,759,760,762,763,764,765,766,767,768,769,770,771,772,773,774,776,777,778,779,780,781,782,783,784,785,786,787,788,789,791,792,793,794,795,796,797,798,799,800,801,802,803,804,805,807,808,809,810,811,812,814,815,816,817,818,819,820,821,822,823,824,825,826,827,828,829,830,831,832,833,834,835,836,837,838,839,840,841,842,843,844,845,846,847,848,849,850,851,852,853,854,855,856,857,858,859,860,861,862,863,864,865,866,867,868,869,870,871,872,873,874,875,876,877,878,879,880,881,882,883,884,885,886,887,888,889,890,891,892,893,894,895,896,897,898,899,920,998,999,1001,1111,1222,2000,3000,8801,8802,8803,8882,10000,10001,10002,10003,10004,10005,10006,10007,10008,10009,10010,10011,10012,10013,10014,10015,10016,10017,10018,10019,10020,10021,10022,10023,10024,10025,10026,10027,10028,10029,10030,10031,10032,10033,10034,10035,10036,10037,10038,10039,10040,10041,10042,10043,10044,10045,10046,10047,10048,10049,10050,10051,10052,10053,10054,10055,10056,10057,10058,10059,10060,10061,10062,10063,10064,10065,10066,10067,10068,10069,11111,12345,12522,15000,15001,15002,17000,20000,20001,20002,20003,20004,20005,20006,20007,20008,20009,20010,20011,30019,30020,30021,30022,30023,30024,30025,30026,30027,30028,30029,30030,30031,30032,30033,30034,30035,30036,30037,30038,30039,30040,30041,30042,30043,30045,30046,30047,30048,30049,30050,30051,30052,30053,30054,30055,30058,30059,30060,30061,30062,30063,30064,30065,40000,40001,40002,40003,40004,40005,40006,40007,40008,40009,40010,45000,45001,45002,45003,45004,45005,45006,46000,49000,49001,49002,49003,49004,49005,49006,49007,100001,100002,100005,100006,100007,100008,100009,100010,100011,100012,100013,100014,100015,100016,100017,100018,100019,100020,100021,100022,100023,100024,100025,100026,100027,100028,100029,100030,100031,100032,100033,100034,100035,100036,100037,100038,100039,100040,100041,100042,100043,100044,100045,100046,100047,100048,100049,100051,100052,100053,100054,100055,100056,100057,100058,100059,100060,100061,100062,100063,100064,100065,100066,100067,100068,100069,100070,100071,100072,100073,100074,100075,100076,100077,100078,100079,100080,100081,100082,100083,100084,100085,100086,100087,100088,100089,100090,100091,100092,100093,100094,100095,100096,100097,100098,100099,100100,100101,100102,100103,100104,100105,100106,100107,100108,100109,100110,100111,100112,100113,100114,100115,100116,100117,100118,100119,100120,100121,100122,100123,100124,100125,100126,100127,100128,100129,100130,100131,100132,100133,100134,100136,100137,100138,100139,100140,100141,100142,100144,100145,100146,100147,100148,100149,100150,100151,100152,100153,100154,100155,100156,100157,100158,100160,100161,100162,100163,100164,100165,100166,100167,100168,100169,100170,100171,100172,100173,100174,100175,100176,100177,100178,100179,100180,100181,100182,100183,100184,100185,100186,100187,100188,100189,100190,100191,100192,100193,100194,100195,100196,100197,100198,100199,100200,100201,100202,100203,100204,100206,100207,100208,100209,100210,100211,100212,100213,100214,100215,100216,100217,100218,100219,100220,100221,100222,100223,100224,100225,100226,100227,100230,100231,100232,100233,100234,100235,100236,100237,100238,100239,100240,100241,100242,100243,100244,100245,100246,100247,100248,100249,100250,100251,100252,100253,100254,100255,100256,100257,100258,100259,100260,100261,100262,100762,100817,110263,110264,150001,150002,150003,150004,150005,150006,150007,150008,150009,150010,150011,150012,150013,150014,160001,160002,160003,160004,160005,160006,160007,160008,160009,160010,160013,170001,170002,170003,170004,170005,170006,170007,170008,170009,170010,180001,180002,180003,180004,180005,180006,180007,180008,200002,200003,200004,200005,200006,200007,200008,200009,200010,200011,200012,200013,200014,200015,200016,200017,200018,200019,200020,200021,200022,200023,200024,200025,200026,200027,200028,200029,200030,200031,200032,200033,200034,200035,200036,200037,200038,200039,200040,200041,200042,200043,200044,200045,200046,200047,200048,200049,200050,200051,200052,200053,200054,200055,200056,200057,200058,200060,200061,200062,200063,200064,200065,200066,200067,200068,200069,200100,220070,220071,250001,250002,300001,300002,300003,300004,300005,300006,300007,300008,300009,300010,300011,300012,300013,300014,300015,300016,300017,300018,300019,300020,300021,300022,300023,300024,300025,300026,300027,300028,300029,300030,300031,300032,300033,300034,300035,300036,300037,300038,300039,300040,300041,300042,300043,300044,300045,300046,300047,300048,300049,300050,300051,300052,300053,300054,300055,300056,300057,300058,300060,300061,300062,300063,300064,300065,300066,300067,300068,300069,300070,300071,300072,300073,300074,300075,300076,300077,300078,300079,300080,300081,300082,300083,300084,300085,300086,300087,300088,300089,300090,300091,300092,300093,300094,300095,300096,300097,300098,300099,300101,300102,300103,300104,300105,300106,300107,300108,300109,300110,300111,300112,300113,300114,300115,300116,300117,300119,300120,300121,300122,300123,300124,300125,300126,300127,300128,300129,300130,300131,300132,300133,300134,300135,300136,300137,300138,300139,300140,300141,300142,300143,300144,300145,300146,300147,300148,300149,300150,300151,300152,300153,300154,300155,300156,300157,300158,300159,300160,300161,300162,300163,300164,300165,300166,300167,300169,300170,300171,300172,300173,300174,300175,300176,300177,300178,300179,300180,300181,300182,300183,300184,300185,300186,300187,300188,300189,300190,300191,300192,300193,300194,300195,300196,300197,300198,330199,400001,400002,401000,500001,500002,500003,500004,500005,500214,600001,800002,800006,800008,800014,800017,800024,800035,800044,800047,800055,800099,800801,800802,800803,909090,999999,1234567,20150819,123456789)" +
		" group by logdate,hid,oemid,adposid,amid" +
		" ) tmp" +
		" group by logdate";*/
		
		//分日总触达
			String sql = "select logdate,count(distinct hid) as expsum " +
					" from adstat where  logdate >= 20160818 and logdate <= 20160831" +
					" and adposid=15101010" +
					" and type=0 and uv=0" +
					" and amid<>0" +
					" and oemid in (0,1,3,4,5,25,26,27,28,29,30,100,101,102,103,104,105,106,108,109,111,112,114,116,118,119,120,121,122,123,124,125,126,130,131,132,133,134,135,136,138,140,141,142,143,145,146,147,148,149,150,153,154,155,156,157,158,160,165,167,169,170,172,176,177,178,179,180,181,182,183,200,201,202,205,207,208,209,210,211,212,215,216,217,218,220,221,222,223,225,226,227,229,230,231,232,235,236,237,239,240,241,242,243,244,245,246,249,250,251,256,257,258,260,261,262,264,268,270,271,272,273,274,275,276,278,280,281,282,283,285,288,289,290,291,292,293,294,296,297,299,300,301,302,303,304,305,307,309,310,312,313,314,315,316,318,319,320,321,322,323,324,325,326,327,328,329,330,331,332,333,334,335,336,337,338,339,340,341,342,343,344,345,346,347,348,350,351,352,355,356,357,358,359,360,361,365,367,368,369,370,371,372,374,379,380,384,386,393,394,395,396,397,400,401,402,403,404,405,406,407,409,410,411,412,414,415,416,417,418,419,420,421,422,425,427,428,429,430,431,432,433,434,440,441,442,443,444,445,446,447,448,449,450,451,452,453,454,455,456,457,458,459,460,461,462,463,464,465,469,470,471,472,477,489,490,491,492,493,494,495,496,497,498,500,501,502,503,504,505,506,507,508,509,510,511,512,513,514,515,516,517,519,520,521,522,523,531,532,534,535,537,538,539,540,541,542,543,544,545,546,547,548,549,550,551,552,553,554,555,556,557,558,559,565,566,567,568,588,589,590,592,593,594,595,597,598,599,601,602,605,606,607,609,610,612,613,614,615,616,617,618,619,620,622,625,626,627,628,629,630,631,634,635,636,637,638,639,640,641,642,643,644,645,646,647,649,650,654,655,656,657,658,659,661,664,665,666,667,668,669,670,671,672,673,674,675,676,677,678,679,680,681,682,684,686,687,688,689,690,691,694,695,696,697,698,699,700,701,702,703,705,706,707,708,709,711,712,713,714,715,716,717,718,719,720,721,722,723,724,725,726,727,728,729,730,731,732,733,734,735,736,737,738,739,740,741,742,743,744,745,746,747,748,749,750,751,752,753,754,755,756,757,758,759,760,762,763,764,765,766,767,768,769,770,771,772,773,774,776,777,778,779,780,781,782,783,784,785,786,787,788,789,791,792,793,794,795,796,797,798,799,800,801,802,803,804,805,807,808,809,810,811,812,814,815,816,817,818,819,820,821,822,823,824,825,826,827,828,829,830,831,832,833,834,835,836,837,838,839,840,841,842,843,844,845,846,847,848,849,850,851,852,853,854,855,856,857,858,859,860,861,862,863,864,865,866,867,868,869,870,871,872,873,874,875,876,877,878,879,880,881,882,883,884,885,886,887,888,889,890,891,892,893,894,895,896,897,898,899,920,998,999,1001,1111,1222,2000,3000,8801,8802,8803,8882,10000,10001,10002,10003,10004,10005,10006,10007,10008,10009,10010,10011,10012,10013,10014,10015,10016,10017,10018,10019,10020,10021,10022,10023,10024,10025,10026,10027,10028,10029,10030,10031,10032,10033,10034,10035,10036,10037,10038,10039,10040,10041,10042,10043,10044,10045,10046,10047,10048,10049,10050,10051,10052,10053,10054,10055,10056,10057,10058,10059,10060,10061,10062,10063,10064,10065,10066,10067,10068,10069,11111,12345,12522,15000,15001,15002,17000,20000,20001,20002,20003,20004,20005,20006,20007,20008,20009,20010,20011,30019,30020,30021,30022,30023,30024,30025,30026,30027,30028,30029,30030,30031,30032,30033,30034,30035,30036,30037,30038,30039,30040,30041,30042,30043,30045,30046,30047,30048,30049,30050,30051,30052,30053,30054,30055,30058,30059,30060,30061,30062,30063,30064,30065,40000,40001,40002,40003,40004,40005,40006,40007,40008,40009,40010,45000,45001,45002,45003,45004,45005,45006,46000,49000,49001,49002,49003,49004,49005,49006,49007,100001,100002,100005,100006,100007,100008,100009,100010,100011,100012,100013,100014,100015,100016,100017,100018,100019,100020,100021,100022,100023,100024,100025,100026,100027,100028,100029,100030,100031,100032,100033,100034,100035,100036,100037,100038,100039,100040,100041,100042,100043,100044,100045,100046,100047,100048,100049,100051,100052,100053,100054,100055,100056,100057,100058,100059,100060,100061,100062,100063,100064,100065,100066,100067,100068,100069,100070,100071,100072,100073,100074,100075,100076,100077,100078,100079,100080,100081,100082,100083,100084,100085,100086,100087,100088,100089,100090,100091,100092,100093,100094,100095,100096,100097,100098,100099,100100,100101,100102,100103,100104,100105,100106,100107,100108,100109,100110,100111,100112,100113,100114,100115,100116,100117,100118,100119,100120,100121,100122,100123,100124,100125,100126,100127,100128,100129,100130,100131,100132,100133,100134,100136,100137,100138,100139,100140,100141,100142,100144,100145,100146,100147,100148,100149,100150,100151,100152,100153,100154,100155,100156,100157,100158,100160,100161,100162,100163,100164,100165,100166,100167,100168,100169,100170,100171,100172,100173,100174,100175,100176,100177,100178,100179,100180,100181,100182,100183,100184,100185,100186,100187,100188,100189,100190,100191,100192,100193,100194,100195,100196,100197,100198,100199,100200,100201,100202,100203,100204,100206,100207,100208,100209,100210,100211,100212,100213,100214,100215,100216,100217,100218,100219,100220,100221,100222,100223,100224,100225,100226,100227,100230,100231,100232,100233,100234,100235,100236,100237,100238,100239,100240,100241,100242,100243,100244,100245,100246,100247,100248,100249,100250,100251,100252,100253,100254,100255,100256,100257,100258,100259,100260,100261,100262,100762,100817,110263,110264,150001,150002,150003,150004,150005,150006,150007,150008,150009,150010,150011,150012,150013,150014,160001,160002,160003,160004,160005,160006,160007,160008,160009,160010,160013,170001,170002,170003,170004,170005,170006,170007,170008,170009,170010,180001,180002,180003,180004,180005,180006,180007,180008,200002,200003,200004,200005,200006,200007,200008,200009,200010,200011,200012,200013,200014,200015,200016,200017,200018,200019,200020,200021,200022,200023,200024,200025,200026,200027,200028,200029,200030,200031,200032,200033,200034,200035,200036,200037,200038,200039,200040,200041,200042,200043,200044,200045,200046,200047,200048,200049,200050,200051,200052,200053,200054,200055,200056,200057,200058,200060,200061,200062,200063,200064,200065,200066,200067,200068,200069,200100,220070,220071,250001,250002,300001,300002,300003,300004,300005,300006,300007,300008,300009,300010,300011,300012,300013,300014,300015,300016,300017,300018,300019,300020,300021,300022,300023,300024,300025,300026,300027,300028,300029,300030,300031,300032,300033,300034,300035,300036,300037,300038,300039,300040,300041,300042,300043,300044,300045,300046,300047,300048,300049,300050,300051,300052,300053,300054,300055,300056,300057,300058,300060,300061,300062,300063,300064,300065,300066,300067,300068,300069,300070,300071,300072,300073,300074,300075,300076,300077,300078,300079,300080,300081,300082,300083,300084,300085,300086,300087,300088,300089,300090,300091,300092,300093,300094,300095,300096,300097,300098,300099,300101,300102,300103,300104,300105,300106,300107,300108,300109,300110,300111,300112,300113,300114,300115,300116,300117,300119,300120,300121,300122,300123,300124,300125,300126,300127,300128,300129,300130,300131,300132,300133,300134,300135,300136,300137,300138,300139,300140,300141,300142,300143,300144,300145,300146,300147,300148,300149,300150,300151,300152,300153,300154,300155,300156,300157,300158,300159,300160,300161,300162,300163,300164,300165,300166,300167,300169,300170,300171,300172,300173,300174,300175,300176,300177,300178,300179,300180,300181,300182,300183,300184,300185,300186,300187,300188,300189,300190,300191,300192,300193,300194,300195,300196,300197,300198,330199,400001,400002,401000,500001,500002,500003,500004,500005,500214,600001,800002,800006,800008,800014,800017,800024,800035,800044,800047,800055,800099,800801,800802,800803,909090,999999,1234567,20150819,123456789)" +
					" group by logdate";
		
		
		System.out.println("==============================================");
		System.out.println("hivesql is : " + sql);
		
		 List result =  hiveKafkaJt.queryForList(sql);
			 
//		 parseEpgResult(result);
//		 parseEpgAdLengthResult(result);
//		 parseAdlengthResult(result);
		 
		 long end = System.currentTimeMillis();
		 System.out.println("查询耗时["+(end-start)+"]毫秒");
		 
		 //直接打印数据
			 for (int j = 0; j < result.size(); j++) {
			      Map map = (Map)result.get(j);    
			      
			      System.out.println(map.toString());
			 }
		 
		/* for (int j = 0; j < result.size(); j++) {
		      Map map = (Map)result.get(j);
		      
		
		      String day =  (String) map.get("day");
//		      String areaparentid =  (Long) map.get("areaparentid")+"";
		      String channelid =  (Integer) map.get("channelid")+"";
		      String amid = (Long) map.get("amid")+"";
		      String allnum =  (Long) map.get("allnum")+"";
		      String usernum =  (Long) map.get("usernum")+"";
		      
//		      String venName = vendorMap.get(ven);
		      //节目
		      String amName = amidMap.get(amid);
		      
		      //地市
		      String proName = proMap.get(areaparentid);

		      proName = proName==null?"国外":proName;
		      

		      //分区域(注，华扬的其他为国外)
//		       System.out.println(day+","+proName+","+allnum+","+usernum);
		      //分地市
		      String areaid =  (Long) map.get("areaid")+"";
		      String areaName = cityMap.get(areaid);
		      if(areaName==null||areaName.equals("其它")){
		    	  areaName = proName;
		      }
		      // System.out.println(day+","+proName+","+areaName+","+allnum+","+usernum);
		      
		      
//		      分频道
		      String channelName = channelMap.get(channelid);
		      
		      System.out.println(day+","+channelid+","+amName+","+channelName+","+allnum+","+usernum);
		      
		 }*/
		
		 
		/* String hiveSql = "select hid,ven,ip " +
		 		" from  fact_adst_history where day='2016-02-07' and hour='01' and amid in (2016020701,2016020702) and ven=900101";*/
		
//		 List result =  hiveKafkaJt.queryForList(hiveSql);
		
/*		hiveKafkaJt.query(hiveSql, new RowMapper<Map>() {
			@Override
			public Map mapRow(ResultSet rs, int rowNum) throws SQLException {
				Map r = new HashMap();
				
				String usernum = rs.getString("hid");
				String ven = rs.getString("ven");
				String ip = rs.getString("ip");
				long longIp = ipToDecimal(ip);
				int areaId = getAreaId(longIp);
				
				StringBuffer out = new StringBuffer();
				out.append(hid)
				
				bw.write(line);
				bw.newLine();
				

				return r;
			}
		});*/
		
		 /*SqlRowSet set = hiveKafkaJt.queryForRowSet(hiveSql);
		 
		 List list =  new ArrayList<>();
		 long count = 0l;
		 HashMap map = null;
		 
		 while(set.next()){			
				String usernum = set.getString("hid");
				String ven = set.getString("ven");
				String ip = set.getString("ip");
				
				long longIp = ipToDecimal(ip);
				int areaId = getAreaId(longIp);
				count++;
				map =  new HashMap();
				map.put("username",usernum);
//				map.put("ven",areaId);
				map.put("ip",ip);
				map.put("longip",longIp+"");
				map.put("areaid",areaId);
				list.add(map);
				if(count%1000==0){
					 writeFile(list);
					 list.clear();
				}
			}
		 //打印剩余数据
		 writeFile(list);
		 
			System.out.println("=============================");
			System.out.println("hiveSql is :"+hiveSql);
			System.out.println("=============================");*/
			
			
		System.out.println("=============================");
	}
	
	/**
	 * 写到本地文件
	 * @param list
	 */
	private void writeFile(List list){
		System.out.println("===========================开始写文件==========================");
		//写文件
		File file = new File("e:\\work\\tmp\\lianmeng.txt");
		FileWriter fw = null;
		BufferedWriter bw = null;
		try {
			fw = new FileWriter(file,true);//true 追加写 
			bw = new BufferedWriter (fw);  
			for (int j = 0; j < list.size(); j++) {
			      Map map = (Map)list.get(j);
			      
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
	
	
	private int getAreaId (long ip){
		for(AdapAreaInfo areaInfo:areaInfoList){
			if(ip>=areaInfo.getNetseg1()&&ip<=areaInfo.getNetseg2()){
				return areaInfo.getCityid();				
			}
		}
		return 0;
	}
	
	
    /**
     *ip转换成十进制的数
     *ip格式：*.*.*.*
     * @param ip
     * @return
     */
    public static long ipToDecimal(String ip){
        long ipDec = 0;
        if(ip != null){
            String[] ipArr = ip.split("\\.");
            if (ipArr != null && ipArr.length == 4){
                for (int i = 3; i >= 0; i--) {
                    ipDec += (Long.valueOf(ipArr[i]) * Math.pow(256, 3-i));
                }
            }
        }
        return ipDec;
    }
	
    /**
     * 统计EPG数据
     * @param result
     */
    private void parseEpgResult(List result){
    	//logdate,oemid,sum(expsum) expsum
    	Map<String,HashMap<String,Long>> dayOemMap = new HashMap<String,HashMap<String,Long>>();
    	
    	 for (int j = 0; j < result.size(); j++) {
		      Map map = (Map)result.get(j);
		      
		      String day =  (String) map.get("logdate");
		      String oemid =  (String) map.get("oemid");
		      Long expsum =  (Long) map.get("expsum");
		      
		      long longOemExp = Long.valueOf(expsum);
		      
		      if(null!=dayOemMap.get(day)){
		    	  //存在该日期数据
//		    	  HashMap oemMap = dayOemMap.get(day);
		    	  
		    	  HashMap<String,Long> epgInnerMap = dayOemMap.get(day);
		    	  //获取epgid
		    	  String epgid = oemidEpgMap.get(oemid);
		    	  
		    	  if(null!=epgInnerMap.get(epgid)){
		    		  //epgid存在，获取累加
		    		  long tmpLong = epgInnerMap.get(epgid);
		    		  tmpLong+=longOemExp;
		    		  epgInnerMap.put(epgid, tmpLong);
		    	  }else{
		    		  epgInnerMap.put(epgid, longOemExp);
		    	  }
		      }else{
		    	  HashMap<String,Long> epgInnerMap = new HashMap<String,Long>();
		    	  //获取epgid
		    	  String epgid = oemidEpgMap.get(oemid);
		    	  epgInnerMap.put(epgid, longOemExp);		    	  
		    	  dayOemMap.put(day, epgInnerMap);
		      }
		    }
    	//遍历EPG map 输出结果。
    	 Iterator dayIt =  dayOemMap.keySet().iterator();
    	 while(dayIt.hasNext()){
    		 String day = (String)dayIt.next();
    		 HashMap<String,Long> epgInnerMap = dayOemMap.get(day);
    		 Iterator epgIt =  epgInnerMap.keySet().iterator();
    		 while(epgIt.hasNext()){
    			 String epgId = (String)epgIt.next();
    			 String epgName = epgDicMap.get(epgId);
    			 long epgExpSum = epgInnerMap.get(epgId);
    			 
    			 System.out.println(day+","+epgId+","+epgName+","+epgExpSum);
    		 }
    	 }
    	 
    }
    
    /**
     * 统计epg广告时长
     * @param result
     */
    private void parseEpgAdLengthResult(List result){
    	//logdate,oemid,sum(expsum) expsum
    	Map<String,HashMap<String,Long>> dayOemMap = new HashMap<String,HashMap<String,Long>>();
    	
    	 for (int j = 0; j < result.size(); j++) {
		      Map map = (Map)result.get(j);
		      
		      String day =  (String) map.get("logdate");
		      String oemid =  (String) map.get("oemid");
		      String amid =  (String) map.get("amid");
		      Long expsum =  (Long) map.get("expsum");
//		      long longOemExp = Long.valueOf(expsum);
		      //根据节目和曝光数计算时长
		      //1、获取节目时长
		      int len = null==adLenMap.get(amid)?0:adLenMap.get(amid);
		      long sumLen = expsum*len;
		      
		      if(null!=dayOemMap.get(day)){
		    	  //存在该日期数据
//		    	  HashMap oemMap = dayOemMap.get(day);
		    	  
		    	  HashMap<String,Long> epgInnerMap = dayOemMap.get(day);
		    	  //获取epgid
		    	  String epgid = oemidEpgMap.get(oemid);
		    	  
		    	  if(null!=epgInnerMap.get(epgid)){
		    		  //epgid存在，获取累加
		    		  long tmpLong = epgInnerMap.get(epgid);
		    		  tmpLong+=sumLen;
		    		  epgInnerMap.put(epgid, tmpLong);
		    	  }else{
		    		  epgInnerMap.put(epgid, sumLen);
		    	  }
		      }else{
		    	  HashMap<String,Long> epgInnerMap = new HashMap<String,Long>();
		    	  //获取epgid
		    	  String epgid = oemidEpgMap.get(oemid);
		    	  epgInnerMap.put(epgid, sumLen);		    	  
		    	  dayOemMap.put(day, epgInnerMap);
		      }
		    }
    	//遍历EPG map 输出结果。
    	 Iterator dayIt =  dayOemMap.keySet().iterator();
    	 while(dayIt.hasNext()){
    		 String day = (String)dayIt.next();
    		 HashMap<String,Long> epgInnerMap = dayOemMap.get(day);
    		 Iterator epgIt =  epgInnerMap.keySet().iterator();
    		 while(epgIt.hasNext()){
    			 String epgId = (String)epgIt.next();
    			 String epgName = epgDicMap.get(epgId);
    			 long epgExpSum = epgInnerMap.get(epgId);
    			 
    			 System.out.println(day+","+epgId+","+epgName+","+epgExpSum);
    		 }
    	 }
    	 
    }
    
    
    /**
     * 计算分日节目总时长
     * @param result
     */
    private void parseAdlengthResult(List result){
    	//logdate,oemid,sum(expsum) expsum
    	Map<String,Long> dayOemMap = new HashMap<String,Long>();
    	
    	 for (int j = 0; j < result.size(); j++) {
		      Map map = (Map)result.get(j);
		      
		      String day =  (String) map.get("logdate");
		      String amid =  (String) map.get("amid");
		      Long expsum =  (Long) map.get("expsum");
		      
		      int len = null==adLenMap.get(amid)?0:adLenMap.get(amid);
		      long sumLen = expsum*len;
//		      long longOemExp = Long.valueOf(expsum);
		      if(null!=dayOemMap.get(day)){
	    		  long tmpLong = dayOemMap.get(day);
	    		  tmpLong+=sumLen;
	    		  dayOemMap.put(day, tmpLong);
		      }else{
		    	  dayOemMap.put(day, sumLen);
		      }
		    }
    	//遍历EPG map 输出结果。
    	 Iterator dayIt =  dayOemMap.keySet().iterator();
    	 while(dayIt.hasNext()){
    		 String day = (String)dayIt.next();
    		 long epgExpSum = dayOemMap.get(day);
    			 System.out.println(day+","+epgExpSum);
    		
    	 }
    	 
    }
	
}
