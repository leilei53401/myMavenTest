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
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Repository;

import adap.AdapAreaInfo;
import adstat.AdposidStatGrid;

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

@Repository
public class ClientNew {

	@Autowired
	public JdbcTemplate hiveKafkaJt;
	@Autowired
	public JdbcTemplate mysqlJt;

	List<AdapAreaInfo> areaInfoList = new ArrayList<AdapAreaInfo>();
	static RangeMap<Long, Integer> rangeMap = TreeRangeMap.create();

	static DecimalFormat df = new DecimalFormat("#.00");
	// 测试环境
	// String [] outPutTitle =
	// {"adstat.sessionid","adstat.hid","adstat.oemid","adstat.adposid"};
	// 生产环境
	// String [] outPutTitle = {"sessionid","hid","oemid","adposid"};

	// 广告联盟春节投放
	String[] outPutTitle = { "username", "ven", "ip", "longip", "areaid" };

	

	/*
	 * private Map<String, String> initAdposidData() { Map<String, String> map =
	 * new HashMap<String, String>(); List<Map<String, Object>> list =
	 * mysqlJt_adguide .queryForList(
	 * "SELECT ad_location_code as adposid,guide_detailname as adposname FROM ad_guide_detail_loc"
	 * ); for (Map<String, Object> temp : list) { String adposid =
	 * temp.get("adposid").toString(); String adposname =
	 * CommonUtil.getJdbcNameObj(temp.get("adposname")); map.put(adposid,
	 * adposid + "_" + adposname); } return map; }
	 */

	public void testHIveSql() {

		String mysql_sql = "SELECT a.provinceid,a.provincename,a.areaid,a.netseg1dec,a.netseg2dec,ar.name cityname,ar.code cityid FROM "
				+ "	(SELECT pr.code provinceid,pr.name provincename,ip.areaid,netseg1dec,netseg2dec"
				+ "	 FROM ad_ip_topology ip,ad_ip_province pr"
				+ "	 WHERE ip.provinceid = pr.provinceid )"
				+ " a LEFT JOIN ad_ip_area ar ON a.areaid=ar.areaid ";

		// List<AdapAreaInfo> result = mysqlJt.queryForList(mysql_sql);

		// List<AdapAreaInfo> areaInfo = new ArrayList<AdapAreaInfo>();
		areaInfoList = mysqlJt.query(mysql_sql, new RowMapper<AdapAreaInfo>() {
			@Override
			public AdapAreaInfo mapRow(ResultSet rs, int rowNum)
					throws SQLException {
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
		for (AdapAreaInfo areaInfo : areaInfoList) {
			int areaId = areaInfo.getCityid();
			rangeMap.put(Range.closed(areaInfo.getNetseg1(),areaInfo.getNetseg2()),areaId);
		}
		

		System.out.println("init areaInfoList end , areaInfoList.size="
				+ areaInfoList.size());
//		System.out.println(rangeMap);

		// ------定义写文件信息------------
		// 写文件
		File file = new File("E:\\tmp\\aggreMangGuoProvince0607_0613.log");
		final AtomicLong at=new AtomicLong(0l);
		
		  long start = System.currentTimeMillis();

		try {
			FileWriter fw = new FileWriter(file);
		  final	BufferedWriter bw = new BufferedWriter(fw,1024*1024*10);
			// 查询msql表
			//String hiveSql = "select ven,amid,hid,ip from  fact_adst_history where day>='2016-02-07' and day<='2016-02-22' and amid in (2016020701,2016020702,2016011501,2016020703,2016020704) and ven in (900101,900102,900103,900104,900105)";
		  
		/*  String hiveSql = "SELECT   DAY,  k_record.ven,  k_record.amid, k_record.pos,getip(k_record.ip) areaid,  SUM(1),  COUNT(DISTINCT (k_record.hid)) usernum" +
				" FROM  kafka.kafka_adap_adst_0 WHERE DAY = '2016-06-14' and k_record.te=0" +
				" GROUP BY DAY,  k_record.amid,  k_record.ven,getip(k_record.ip)  " +
				" order by DAY,k_record.ven,k_record.amid";*/
		  
		 /**
		  * 1200000
		============done==耗时[392986]毫秒==============
		==============done================
		  */
		  String hiveSql = "SELECT   DAY,  k_record.ven,  k_record.amid, k_record.pos,k_record.ip,k_record.hid" +
					" FROM  kafka.kafka_adap_adst_0 WHERE DAY >= '2016-06-07' and day<='2016-06-13' and k_record.ven=900108 and k_record.te=0";
		  
		
		  
				  
			
			hiveKafkaJt.query(hiveSql, new RowMapper<Map>() {
				@Override
				public Map mapRow(ResultSet rs, int rowNum) throws SQLException {
					String day = rs.getString("day");
					int ven = rs.getInt("ven");
					long amid = rs.getLong("amid");
					long pos = rs.getLong("pos");
					String ip = rs.getString("ip");
					String hid = rs.getString("hid");
					long longIp = ipToDecimal(ip);
					int areaId = getAreaId(longIp);
					StringBuffer out = new StringBuffer();
					out.append(day).append(",").append(ven).append(",").append(amid).append(",").append(pos).append(",").append(ip).append(",").append(hid).append(",")
							.append(longIp).append(",").append(areaId);

					try {
						bw.write(out.toString());
						bw.newLine();
						if(at.incrementAndGet()%100000==0){
							System.out.println(at.get());
							bw.flush();
						}
					
					} catch (Exception e) {
						System.err.println("写文件出错:"+out.toString()+"  :" + e);
					}

					return null;
				}
			});

			try {
				bw.flush();
				bw.close();
				fw.close();
			} catch (IOException e1) {
				System.err.println("关闭流出错：" + e1);
			}
			
			
		} catch (DataAccessException e) {

		} catch (IOException e) {

			System.err.println("写文件出错2：" + e);
			
		
		} 
		
		long end = System.currentTimeMillis();
		
		

		/*
		 * SqlRowSet set = hiveKafkaJt.queryForRowSet(hiveSql);
		 * 
		 * List list = new ArrayList<>(); long count = 0l; HashMap map = null;
		 * 
		 * while(set.next()){ String usernum = set.getString("hid"); String ven
		 * = set.getString("ven"); String ip = set.getString("ip");
		 * 
		 * long longIp = ipToDecimal(ip); int areaId = getAreaId(longIp);
		 * count++; map = new HashMap(); map.put("username",usernum); //
		 * map.put("ven",areaId); map.put("ip",ip); map.put("longip",longIp+"");
		 * map.put("areaid",areaId); list.add(map); if(count%1000==0){
		 * writeFile(list); list.clear(); } } //打印剩余数据 writeFile(list);
		 * 
		 * System.out.println("=============================");
		 * System.out.println("hiveSql is :"+hiveSql);
		 * System.out.println("=============================");
		 */

		System.out.println("============done==耗时["+(end-start)+"]毫秒==============");
	}

	/**
	 * 写到本地文件
	 * 
	 * @param list
	 */
	private void writeFile(List list) {
		System.out
				.println("===========================开始写文件==========================");
		// 写文件
		File file = new File("e:\\work\\tmp\\lianmeng.txt");
		FileWriter fw = null;
		BufferedWriter bw = null;
		try {
			fw = new FileWriter(file, true);// true 追加写
			bw = new BufferedWriter(fw);
			for (int j = 0; j < list.size(); j++) {
				Map map = (Map) list.get(j);

				System.out.println(map.toString());

				String line = "";
				// 按指定表头写入文件
				for (String oneTitle : outPutTitle) {
					String str = "";
					try {
						str = String.valueOf(map.get(oneTitle));
						if (null == str || "null".equals(str)) {
							str = "";
						}
					} catch (Exception exp) {
						str = "";
					}
					line += str + ",";
				}

				line = line.substring(0, line.length() - 1);
				// 写入一条数据
				bw.write(line);
				bw.newLine();
			}

			try {
				bw.flush();
				bw.close();
				fw.close();
			} catch (IOException e1) {
				System.err.println("写文件出错：" + e1);
			}
		} catch (IOException e) {
			System.err.println("写文件出错2：" + e);
			try {

				if (bw != null) {
					bw.close();
					bw = null;
				}
				if (fw != null) {
					fw.close();
					fw = null;
				}

			} catch (IOException ex) {
				System.err.println("关闭流出错！" + ex);
			}
		}

		System.out
				.println("===========================写文件结束==========================");
	}

	private int getAreaId(long ip) {
		/*for (AdapAreaInfo areaInfo : areaInfoList) {
			if (ip >= areaInfo.getNetseg1() && ip <= areaInfo.getNetseg2()) {
				return areaInfo.getCityid();
			}
		}*/
		Object o=rangeMap.get(ip);
		if(o==null){
			return -1;
		}
		return  Integer.parseInt(o.toString());
	}

	/**
	 * ip转换成十进制的数 ip格式：*.*.*.*
	 * 
	 * @param ip
	 * @return
	 */
	public static long ipToDecimal(String ip) {
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
