package hive;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.dbcp.BasicDataSource;
import org.junit.Test;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

/**
 * 将hive数据导入mysql工具类
 * @author shaoyl
 *
 */
public class ExportToMysqlTest {
	
	private JdbcTemplate hiveJt;// 连接池
	private JdbcTemplate mysqlJt;// 连接池

	public ExportToMysqlTest(){
		BasicDataSource dataSource1 = new BasicDataSource();
		dataSource1.setDriverClassName("org.apache.hive.jdbc.HiveDriver");
		dataSource1.setUrl("jdbc:hive2://60.29.252.4:10000/default");
//		dataSource1.setUrl("jdbc:hive2://172.16.10.95:10000/default");
		dataSource1.setUsername("statuser");
		dataSource1.setPassword("statuser*()");		
		this.hiveJt = new JdbcTemplate();
		this.hiveJt.setDataSource(dataSource1);
		
	/*	url=jdbc:mysql://172.16.10.177:3306/ad_guide?useUnicode=true&characterEncoding=UTF-8
			userName=root
			password=mysql*()
			*/
		
		
		//初始化MySql连接配置
		BasicDataSource mysqlDS = new BasicDataSource();
		mysqlDS.setDriverClassName("com.mysql.jdbc.Driver");
		mysqlDS.setUrl("jdbc:mysql://172.16.10.177:3306/ad_guide?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull");
		mysqlDS.setUsername("root");
		mysqlDS.setPassword("mysql*()");		
		this.mysqlJt = new JdbcTemplate();
		this.mysqlJt.setDataSource(mysqlDS);
		
		initDict();
	}
	
	public void initDict(){
		//初始化各个字典表
	}
	
	@Test
	public void exportToMysql() {
	
		String hiveSql ="SELECT * FROM adstat WHERE logdate=20160831 and adposid<>15101010  limit 500";
		
		System.out.println("hive sql is :"+ hiveSql);
		final List<String> sqlList = new ArrayList<String>();
		final StringBuffer insertSql = new StringBuffer();
		this.hiveJt.query(hiveSql, new RowMapper<Object>(){
			@Override
			public Object mapRow(ResultSet hiveRes, int rownum) {
				
				try {
					int	columnCount = hiveRes.getMetaData().getColumnCount();
					insertSql.append("insert into adstat values(");
					for(int j = 1 ; j <= columnCount ; j ++ ){
						if(j > 1 ){
							insertSql.append(",'"+hiveRes.getString(j)+"'");
						}
						else{
							insertSql.append("'"+hiveRes.getString(j)+"'");
						}
					}
					insertSql.append(")");
					sqlList.add(insertSql.toString());
					insertSql.setLength(0);
					
				} catch (SQLException e) {
				
					e.printStackTrace();
				}
				
				return null;
			}
		});
		
//		sqlList.toArray();		
	
//	   mysqlJt.batchUpdate((String[])sqlList.toArray());
		
		int i=1;
		
		for(String oneSql:sqlList){
			if(i==1){
				System.out.println("oneSql is "+oneSql);
				i++;
			}
			try {
				mysqlJt.execute(oneSql);
			} catch (DataAccessException e) {
				System.out.println("err sql is "+oneSql );
				e.printStackTrace();
				continue;
			}
		}

		System.out.println("done");
		
		
	}
	
	
	/**
	 * 8月25日点播APK启动和首页EPG浮层的数据明细，包含HID和OEMID。
	 */
	@Test
	public void getApkStartAndEpgDetail() {

		long start = System.currentTimeMillis();

		
//		String sql = "SELECT * FROM adstat WHERE logdate=20160825 and adposid in (11101010,17101610) and uv=0 and amid<>0 and type=0";
		String sql1 = "SELECT hid FROM adstat WHERE logdate=20160825 and adposid in (11101010) and uv=0 and type=0";
		String sql2 = "SELECT hid FROM adstat WHERE logdate=20160825 and adposid in (17101610) and uv=0 and type=0";

		List result = null;
		HashSet<String> set1 = new HashSet<String>();
		try {
			result = hiveJt.queryForList(sql1);
		} catch (DataAccessException e) {
			e.printStackTrace();
		}
		
		for (int j = 0; j < result.size(); j++) {
		      Map map = (Map)result.get(j);   
		      set1.add(map.get("hid").toString());
		     
		 }	
		
		List<String> list1 = new ArrayList<String>();
		list1.addAll(set1);

		List result2 = null;
		
		HashSet<String> set2 = new HashSet<String>();
		try {
			result2 = hiveJt.queryForList(sql2);
		} catch (DataAccessException e) {
			e.printStackTrace();
		}
		
		for (int j = 0; j < result2.size(); j++) {
		      Map map = (Map)result2.get(j);    
		      set2.add(map.get("hid").toString());
		 }	
		List<String> list2 = new ArrayList<String>();
		list2.addAll(set2);
		
		list1.removeAll(list2);
		
		System.out.println("list1.toString()=="+list1.toString());
		
		long end = System.currentTimeMillis();
		System.out.println("查询耗时[" + (end - start) + "]毫秒");
	
	
		
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
	 * 将Result数据写到本地文件
	 * @param result
	 */
	private void writeResult(List result){
		System.out.println("===========================开始写文件==========================");
		//定义写入字段顺序
		String [] outPutTitle = {"username","ven","ip","longip","areaid"};
		//写文件
		File file = new File("e:\\work\\tmp\\lianmeng.txt");
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

