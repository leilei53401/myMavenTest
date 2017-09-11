package udf;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.google.common.collect.RangeMap;
import com.google.common.collect.Ranges;
import com.google.common.collect.TreeRangeMap;

public class DbUtil {
	

	
	//连接数据库
	/*public static final String  driver= "com.mysql.jdbc.Driver";
	public static final String  url= "jdbc:mysql://localhost:3306/ad_guide?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull";
	public static final String  userName= "root";
	public static final String  passwd= "root123";
	*/
	
	public static final String  driver= "com.mysql.jdbc.Driver";
	public static final String  url= "jdbc:mysql://172.16.10.177:3306/ad_guide?useUnicode=true&characterEncoding=UTF-8";
	public static final String  userName= "root";
	public static final String  passwd= "mysql*()";

	// 重试次数
	public static final int NUM_RETRIES = 15;

		// 休眠时间 10 s
	public static final int SLEEP_TIME = 8000;
	
	/**
	 * 多次重试连接oracle
	 * 
	 * @return
	 */
	public static Connection getRetriesConnection() {
		Connection con = null;
		for (int tries = 1; tries <= NUM_RETRIES; tries++) {
			con = getCon();
			if (null != con) {
				break;
			}
			try {
				Thread.sleep(SLEEP_TIME);
			} 
			catch (InterruptedException e) {
				e.printStackTrace();
				// Do this conversion rather than let it out because do not want
				// to
				// change the method signature.
				Thread.currentThread().interrupt();

				// throw new IOException("Interrupted", e);
			}
		}
		return con;
	}
	
	/**
	 * 
	 * @param dbType 支持oracle和hive
	 * @return 
	 * @return
	 */
	public static Connection  getCon() {
		Connection con = null;
		try {
				Class.forName(driver);
				con =DriverManager.getConnection(url, userName, passwd);
		} catch (Exception e) {
			//e.printStackTrace();
			System.err.println(e);
		}
		return con;
	}
	
	public static void closeConandSta(Connection con,Statement sta){

		if(null != con){
			try {
				con.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				//e.printStackTrace();
			}
		}
		if(null != sta){
			try {
				sta.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				//e.printStackTrace();
			}
		}
		
	}
	
	/**
	 * 获取IP范围查找区域地市字典表
	 */
	public static RangeMap<Long, Integer> getAreaDic(){
		Connection con = null ;
		Statement sta = null;
		RangeMap<Long, Integer> rangeMap = TreeRangeMap.create();
		
		try {
			 con = getCon();
			 sta = con.createStatement();
			 
				String mysql_sql = "SELECT a.provinceid,a.provincename,a.areaid,a.netseg1dec,a.netseg2dec,ar.name cityname,ar.code cityid FROM "
						+ "	(SELECT pr.code provinceid,pr.name provincename,ip.areaid,netseg1dec,netseg2dec"
						+ "	 FROM ad_ip_topology ip,ad_ip_province pr"
						+ "	 WHERE ip.provinceid = pr.provinceid )"
						+ " a LEFT JOIN ad_ip_area ar ON a.areaid=ar.areaid ";
				System.out.println("mysql_sql is :"+mysql_sql);
			ResultSet rs =  sta.executeQuery(mysql_sql);
			while (rs.next()){
				long start = rs.getLong("netseg1dec");
				long end = rs.getLong("netseg2dec");
				int areaId = rs.getInt("cityid");
				rangeMap.put(Ranges.closed(start,end),areaId);
			}
		} catch (SQLException e) {
			System.err.println(e);
		}
		finally{
			closeConandSta(con, sta);
		}
		return rangeMap;
	}
	

	
	public static void main(String[] args) {
	/*	System.out.println(Runtime.getRuntime().freeMemory()/1024/1024+"M");
		Map<Long,String> map = new IdentityHashMap<Long, String>(); 
		for(int i = 0; i < 1000000 ; i ++){
			map.put(1921681123l+i, "");
			if( i % 10000 == 0){
				System.out.println(Runtime.getRuntime().freeMemory()/1024/1024+"M");
			}
		}*/
		
		String ip ="12.100.200.222";
		long l = DbUtil.getConvertIp(ip);
		System.out.println(l);
	}
	
	/**
	 *  Result:=to_number(ip1)*16777216+to_number(ip2)*65536+to_number(ip3)*256+to_number(ip4);
	 *  返回处理后的IP段,规则如上
	 * @param obj
	 */
	public static Long getConvertIp(String obj){
		String temp[] = obj.split("\\.");
		Long result =  0l;
		if(temp.length < 4){
			return result;
		}
		
		try{
			result = Long.parseLong(temp[0])*16777216+Long.parseLong(temp[1])*65536+Long.parseLong(temp[2])*256+Long.parseLong(temp[3]);
		}
		catch(Exception e){
			return 0l;
		}
		return result;
	}

}
