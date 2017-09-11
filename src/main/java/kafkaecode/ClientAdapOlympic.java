package kafkaecode;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Repository;

import adap.AdapAreaInfo;
import adstat.AdposidStatGrid;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;


@Repository
public class ClientAdapOlympic {

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

	/**
	 * 
	 * @param sendtimeStart
	 *            format yyyy-MM-dd HH:mm:ss 在数组外
	 * @param sendtimeEnd
	 *            format yyyy-MM-dd HH:mm:ss 在数组外
	 * @param subsys
	 *            系统编号 在数组里
	 * @param errorcode
	 *            错误码 在数组里
	 * @return
	 */
	public List<KafkaErrorCodeSimple> getErrorCodeList(String sendtimeStart, String sendtimeEnd, String subsys, String errorcode) throws Exception {
		List<KafkaErrorCodeSimple> list_simple = new ArrayList<KafkaErrorCodeSimple>();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		long start = sdf.parse(sendtimeStart).getTime()/1000 ;
		long end = sdf.parse(sendtimeEnd).getTime()/1000 ;
		String sql = "select  k_record.client,from_unixtime(k_record.sendtime,'yyyy-MM-dd HH:mm:ss') as convertsendtime,k_record.datareportings from kafka_error_code_0 lateral view explode(k_record.datareportings) exploded_tab as f where 1 = 1 ";
		if (sendtimeStart != null && !"".equals(sendtimeStart) && sendtimeEnd != null && !"".equals(sendtimeEnd)) {
			sql += " and k_record.sendtime >= " + start + " and k_record.sendtime <= " + end;
		} else {
			return list_simple;
		}
		if (subsys != null && !"".equals(subsys)) {
			sql += " and f.subsys like '%" + subsys + "%'";
		}
		if (errorcode != null && !"".equals(errorcode)) {
			sql += " and f.errorcode like '%" + errorcode + "%'";
		}
		//sql += "  limit 10000000000";
	
		List<KafkaErrorCode> list = hiveKafkaJt.query(sql, new RowMapper<KafkaErrorCode>() {
			@Override
			public KafkaErrorCode mapRow(ResultSet rs, int rowNum) throws SQLException {
				
				KafkaErrorCode r = new KafkaErrorCode();
				String client = rs.getString("client");
				String sendtime = rs.getString("convertsendtime");
				String datareportings = rs.getString("datareportings");
				r.setClient(client);
				r.setSendtime(sendtime);
				r.setDatareportings(datareportings);
				List<ErrorCode> transformErrorCode = transformErrorCode(datareportings);
				KafkaErrorCodeSimple simple = null;
				for (ErrorCode ecode : transformErrorCode) {
					simple = new KafkaErrorCodeSimple();
					simple.setClient(r.getClient());
					simple.setSendtime(r.getSendtime());
					simple.setOeminfo(ecode.getOeminfo());
					simple.setOemtype(ecode.getOemtype());
					simple.setErrorcode(ecode.getErrorcode());
					simple.setSubsys(ecode.getSubsys());
					simple.setSubsysname(ecode.getSubsysname());
					simple.setMessage(ecode.getMessage());
					simple.setScene(ecode.getScene());
					simple.setSolution(ecode.getSolution());
					simple.setOlderrorcode(ecode.getOlderrorcode());
					simple.setErrorsources(ecode.getErrorsources());
					simple.setRemark(ecode.getRemark());
					simple.setStatus(ecode.getStatus());
					simple.setSessionid(ecode.getSessionid());
					simple.setErrorlevel(ecode.getErrorlevel());
					simple.setErrorstack(ecode.getErrorstack());
					simple.setActivatetime(ecode.getActivatetime());
				//	if(simple.getSubsys().equals("01")){
					
						System.out.println(simple);
				//	}
				}
				
			
			
				return null;
			}
		});
        if(list==null){
        	return list_simple;
        }
//		for (KafkaErrorCode e : list) {
//			String datareportings = e.getDatareportings();
//			List<ErrorCode> transformErrorCode = transformErrorCode(datareportings);
//			KafkaErrorCodeSimple simple = null;
//			for (ErrorCode ecode : transformErrorCode) {
//				simple = new KafkaErrorCodeSimple();
//				simple.setClient(e.getClient());
//				simple.setSendtime(e.getSendtime());
//				simple.setOeminfo(ecode.getOeminfo());
//				simple.setOemtype(ecode.getOemtype());
//				simple.setErrorcode(ecode.getErrorcode());
//				simple.setSubsys(ecode.getSubsys());
//				simple.setSubsysname(ecode.getSubsysname());
//				simple.setMessage(ecode.getMessage());
//				simple.setScene(ecode.getScene());
//				simple.setSolution(ecode.getSolution());
//				simple.setOlderrorcode(ecode.getOlderrorcode());
//				simple.setErrorsources(ecode.getErrorsources());
//				simple.setRemark(ecode.getRemark());
//				simple.setStatus(ecode.getStatus());
//				simple.setSessionid(ecode.getSessionid());
//				simple.setErrorlevel(ecode.getErrorlevel());
//				simple.setErrorstack(ecode.getErrorstack());
//				simple.setActivatetime(ecode.getActivatetime());
//				list_simple.add(simple);
//			}
//		}
		return list_simple;
	}

	/**
	 * 弃用
	 * @param sendtimeStart
	 *            format yyyy-MM-dd HH:mm:ss 在数组外
	 * @param sendtimeEnd
	 *            format yyyy-MM-dd HH:mm:ss 在数组外
	 * @param subsys
	 *            系统编号 在数组里
	 * @param errorcode
	 *            错误码 在数组里
	 * @return
	 */
	@Deprecated
	public List<KafkaErrorCodeSimple> getErrorCodeSimpleList(String sendtimeStart, String sendtimeEnd, String subsys, String errorcode) throws Exception {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		long start = sdf.parse(sendtimeStart).getTime() / 1000;
		long end = sdf.parse(sendtimeEnd).getTime() / 1000;
		String sql = "select k_record.client,from_unixtime(k_record.sendtime,'yyyy-MM-dd HH:mm:ss') as convertsendtime,"
				+ "f.oeminfo,f.oemtype,f.errorcode,f.subsys,f.subsysname,f.message,f.scene,f.solution,f.olderrorcode,f.errorsources,f.remark,f.status,"
				+ "f.sessionid,f.errorlevel,f.errorstack,f.activatetime " + " from kafka_error_code_0 lateral view explode(k_record.datareportings) exploded_tab as f where 1 = 1 ";
		if (sendtimeStart != null && !"".equals(sendtimeStart) && sendtimeEnd != null && !"".equals(sendtimeEnd)) {
			sql += " and k_record.sendtime >= " + start + " and k_record.sendtime <= " + end;
		} else {
			return null;
		}
		if (subsys != null && !"".equals(subsys)) {
			sql += " and f.subsys like '%" + subsys + "%'";
		}
		if (errorcode != null && !"".equals(errorcode)) {
			sql += " and f.errorcode like '%" + errorcode + "%'";
		}
		List<KafkaErrorCodeSimple> list = hiveKafkaJt.query(sql, new RowMapper<KafkaErrorCodeSimple>() {
			@Override
			public KafkaErrorCodeSimple mapRow(ResultSet rs, int rowNum) throws SQLException {
				KafkaErrorCodeSimple r = new KafkaErrorCodeSimple();
				r.setClient(rs.getString("client"));
				r.setSendtime(rs.getString("convertsendtime"));
				r.setOeminfo(rs.getString("oeminfo"));
				r.setOemtype(rs.getString("oemtype"));
				r.setErrorcode(rs.getString("errorcode"));
				r.setSubsys(rs.getString("subsys"));
				r.setSubsysname(rs.getString("subsysname"));
				r.setMessage(rs.getString("message"));
				r.setScene(rs.getString("scene"));
				r.setSolution(rs.getString("solution"));
				r.setOlderrorcode(rs.getString("olderrorcode"));
				r.setErrorsources(rs.getString("errorsources"));
				r.setRemark(rs.getString("remark"));
				r.setStatus(rs.getString("status"));
				r.setSessionid(rs.getString("sessionid"));
				r.setErrorlevel(rs.getString("errorlevel"));
				r.setErrorstack(rs.getString("errorstack"));
				r.setActivatetime(rs.getString("activatetime"));
				return r;
			}
		});
		return list;
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
		
		
	  /* 	 String hiveSql = "";
		 List result =  hiveKafkaJt.queryForList(hiveSql);
		 
		 for (int j = 0; j < result.size(); j++) {
		      Map map = (Map)result.get(j);
		      System.out.println(map.toString());
		 }
		 */
		 String hiveSql = "select hid,ven,ip " +
		 		" from  fact_adst_history where day='2016-02-07' and hour='01' and amid in (2016020701,2016020702) and ven=900101";
		
//		 List result =  hiveKafkaJt.queryForList(hiveSql);
		/*
		hiveKafkaJt.query(hiveSql, new RowMapper<Map>() {
			@Override
			public Map mapRow(ResultSet rs, int rowNum) throws SQLException {
				
				
				String usernum = rs.getString("hid");
				String ven = rs.getString("ven");
				String ip = rs.getString("ip");
				long longIp = ipToDecimal(ip);
				int areaId = getAreaId(longIp);
				
				StringBuffer out = new StringBuffer();
				out.append(hid)
				
				bw.write(line);
				bw.newLine();
				

				return null;
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
	
	
}
