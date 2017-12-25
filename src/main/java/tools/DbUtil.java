package tools;

import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class DbUtil {
	
	//�������ݿ�
	public static final String driver= "com.mysql.jdbc.Driver";
	public static final String url= "jdbc:mysql://localhost:3306/ad_guide?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull";
	public static final String userName= "root";
	public static final String passwd= "root123";
		
//	 ���Կ�
/*	public static final String  driver= "com.mysql.jdbc.Driver";
	public static final String  url= "jdbc:mysql://172.16.10.177:3306/ad_guide?useUnicode=true&characterEncoding=UTF-8";
	public static final String  userName= "root";
	public static final String  passwd= "mysql*()";
	*/
	
	//������
/*	public static final String driver = "com.mysql.jdbc.Driver";
	public static final String url = "jdbc:mysql://125.39.27.114:3306/ad_guide?useUnicode=true&characterEncoding=UTF-8";
	public static final String userName = "root_ad";
	public static final String passwd = "E_CFXPT4";*/

	// ���Դ���
	public static final int NUM_RETRIES = 15;

		// ����ʱ�� 10 s
	public static final int SLEEP_TIME = 8000;
	
	/**
	 * �����������oracle
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
	 * @param dbType ֧��oracle��hive
	 * @return 
	 * @return
	 */
	public static Connection getCon() {
		Connection con = null;
		try {
				Class.forName(driver);
				con = DriverManager.getConnection(url, userName, passwd);
		} catch (Exception e) {
			//e.printStackTrace();
			System.err.println(e);
		}
		return con;
	}
	
	public static void closeConandSta(Connection con, Statement sta){

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
	 * ����ITV��ַ�⵼��
	 * @param btb
	 * @param list
	 * @return
	 */
	public static boolean insertJiangsuItvIpDatabases(List list){
		
		
		// �������Դ���ݿ�����
		Connection conn = null;
		Statement st = null;
		
		try {
			conn = getCon();
			st = conn.createStatement();
			conn.setAutoCommit(false);
			
			for(int i=0;i<list.size();i++){
				Map map = (HashMap)list.get(i);
				/*
				 * �ֶβο���
				   `TopoID` int(10) unsigned NOT NULL AUTO_INCREMENT,
				  `OrgSeg1` varchar(30) DEFAULT NULL COMMENT '���ʽip��ʼ��ַ',
				  `OrgSeg2` varchar(30) DEFAULT NULL COMMENT '���ʽip������ַ',
				  `Mask` int(10) unsigned DEFAULT NULL COMMENT '����ֵ',
				  `NetSeg1` varchar(30) DEFAULT NULL COMMENT '�������ʽipСֵ',
				  `NetSeg2` varchar(30) DEFAULT NULL COMMENT '�������ʽip��ֵ',
				  `NetSeg1Dec` double(20,0) DEFAULT NULL COMMENT 'ʮ����ipСֵ',
				  `NetSeg2Dec` double(20,0) DEFAULT NULL COMMENT 'ʮ����ip��ֵ',
				  `ProvinceID` int(10) unsigned NOT NULL  COMMENT 'ʡid',
				  `ProvinceName` varchar(30) DEFAULT NULL COMMENT 'ʡ������',
				  `ProvinceCode` int(10) unsigned DEFAULT NULL COMMENT 'ʡ�ݱ��',
				  `AreaID` int(10) unsigned NOT NULL COMMENT '��id',
				  `AreaName` varchar(30) DEFAULT NULL COMMENT '��������',
				  `AreaCode` int(10) unsigned DEFAULT NULL COMMENT '���б���',
				  `ven` varchar(20) DEFAULT NULL COMMENT 'B2B���ҿͻ����',
				  `createtime` timestamp NULL DEFAULT CURRENT_TIMESTAMP COMMENT '���ʱ��',
				 */
				String OrgSeg1 = map.get("OrgSeg1").toString();
				String OrgSeg2 = map.get("OrgSeg2").toString();
				String Mask = map.get("Mask").toString();
				String NetSeg1 = map.get("NetSeg1").toString();
				String NetSeg2 = map.get("NetSeg2").toString();
				String NetSeg1Dec = map.get("NetSeg1Dec").toString();
				String NetSeg2Dec = map.get("NetSeg2Dec").toString();
				String ProvinceID = map.get("ProvinceID").toString();
				String ProvinceName = map.get("ProvinceName").toString();
				String ProvinceCode = map.get("ProvinceCode").toString();
				String AreaID = map.get("AreaID").toString();
				String AreaName = map.get("AreaName").toString();
				String AreaCode = map.get("AreaCode").toString();
				String ven = map.get("ven").toString();
								
				String sql =  "insert into adap_b2b_ip_topology(OrgSeg1,OrgSeg2,Mask,NetSeg1,NetSeg2,NetSeg1Dec,NetSeg2Dec,ProvinceID,ProvinceName,ProvinceCode,AreaID,AreaName,AreaCode,ven) " +
						" values('"+OrgSeg1+"','"+OrgSeg2+"',"+Mask+",'"+NetSeg1+"','"+NetSeg2+"',"+NetSeg1Dec+","+NetSeg2Dec+","
						+ProvinceID+",'"+ProvinceName+"','"+ProvinceCode+"',"+AreaID+",'"+AreaName+"','"+AreaCode+"','"+ven+"')";
				
				st.addBatch(sql);
			}

			int[] result = st.executeBatch();
			conn.commit();
			conn.setAutoCommit(true);
			
			System.out.println("�������ݡ�"+result.length+"����;");
			
		} catch (Exception e) {
			System.err.println(e);
		}finally{
			closeConandSta(conn, st);
		}
		return true;
	}
	/**
	 * ����ʡ�����ƻ�ȡid��code
	 * @param proName
	 * @return
	 */
	public static Map<String, String> getProvinceCodeByProName(String proName){
		Connection con = null ;
		Statement sta = null;
		Map<String, String> provinceMap = new HashMap<String,String>();
		try {
			    con = getCon();
			    sta = con.createStatement();
				String mysql_sql = "SELECT t.ProvinceID,t.name,t.code from ad_ip_province t where name='"+proName+"'";
				System.out.println("mysql_sql is :"+mysql_sql);
			ResultSet rs =  sta.executeQuery(mysql_sql);
			if (rs.next()){
				String pid = rs.getString("provinceid");
				String code = rs.getString("code");
			
				provinceMap.put("pid", pid);
				provinceMap.put("code", code);
			}
		} catch (SQLException e) {
			System.err.println(e);
		}
		finally{
			closeConandSta(con, sta);
		}
		return provinceMap;
	}
	
	/**
	 * ��ȡʡ�ݱ���
	 * Map<ʡ��,Map<ʡ��id/ʡ��code,value>
	 */
	public static Map<String, Map<String, String>> getProvinceCode(){
		Connection con = null ;
		Statement sta = null;
		Map<String,Map<String, String>> provinceMap  = new HashMap<String,Map<String, String>>();
		try {
			    con = getCon();
			    sta = con.createStatement();
				String mysql_sql = "SELECT t.ProvinceID,t.name,t.code from ad_ip_province t";
				System.out.println("mysql_sql is :"+mysql_sql);
			ResultSet rs =  sta.executeQuery(mysql_sql);
			while (rs.next()){
				String code = rs.getString("code");
				String name = rs.getString("name");
				String pid = rs.getString("provinceid");
				Map<String,String> map = new HashMap<String,String>();
				map.put("pid", pid);
				map.put("code", code);
				provinceMap.put(name,map);
			}
		} catch (SQLException e) {
			System.err.println(e);
		}
		finally{
			closeConandSta(con, sta);
		}
		return provinceMap;
	}
	/**
	 * ע��ͨ�� provinceid ��ѯ���У���������code��
	 * @param provinceid
	 * @return
	 */
	public static Map<String, Map<String, String>> getCityCodeByPid(String provinceid){
		Connection con = null ;
		Statement sta = null;
		Map<String,Map<String, String>> cityMap  = new HashMap<String,Map<String, String>>();
		try {
			    con = getCon();
			    sta = con.createStatement();
				String mysql_sql = "SELECT ta.AreaID,ta.Name,ta.Code FROM ad_ip_area ta WHERE ta.`ProvinceID`=10";
				System.out.println("����ʡ�ݱ�Ų�ѯ���б�� :"+mysql_sql);
			ResultSet rs =  sta.executeQuery(mysql_sql);
			while (rs.next()){
				String code = rs.getString("code");
				String name = rs.getString("name");
				String areaid = rs.getString("areaid");
				Map<String,String> map = new HashMap<String,String>();
				map.put("areaid", areaid);
				map.put("code", code);
				cityMap.put(name,map);
			}
		} catch (SQLException e) {
			System.err.println(e);
		}
		finally{
			closeConandSta(con, sta);
		}
		return cityMap;
	}
	
	
	/**
	 * ��ȡIP��Χ������������ֵ��
	 */
	public static HashMap<String, Integer> getCityCode(){
		Connection con = null ;
		Statement sta = null;
		Map<String,Integer> provinceMap  = new HashMap<String,Integer>();
		
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
			}
		} catch (SQLException e) {
			System.err.println(e);
		}
		finally{
			closeConandSta(con, sta);
		}
		return null;
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
	 *  ���ش�����IP��,��������
	 * @param obj
	 */
	public static Long getConvertIp(String obj){
		String temp[] = obj.split("\\.");
		Long result =  0l;
		if(temp.length < 4){
			return result;
		}
		
		try{
			result = Long.parseLong(temp[0])*16777216+ Long.parseLong(temp[1])*65536+ Long.parseLong(temp[2])*256+ Long.parseLong(temp[3]);
		}
		catch(Exception e){
			return 0l;
		}
		return result;
	}

}
