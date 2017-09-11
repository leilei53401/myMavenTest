package hive;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.dbcp.BasicDataSource;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

public class Hivejdbc_function {
	
	private JdbcTemplate hiveJt;// 连接池

	public Hivejdbc_function(){
		BasicDataSource dataSource1 = new BasicDataSource();
		dataSource1.setDriverClassName("org.apache.hive.jdbc.HiveDriver");
		dataSource1.setUrl("jdbc:hive2://ad-master:10000/default");
		dataSource1.setUsername("statuser");
		dataSource1.setPassword("statuser*()");
		
		this.hiveJt = new JdbcTemplate();
		this.hiveJt.setDataSource(dataSource1);
	}
	
	@Test
	public void testGetIP(){
		String sql = "select getip('218.68.233.139')";
		this.hiveJt.query(sql, new RowMapper<Object>(){
			@Override
			public Object mapRow(ResultSet rs, int rownum) throws SQLException {
				System.out.println(rs.getString(1));
				return null;
			}
		});
	}
	
	/**
	 * 解析成功
	 */
	@Test
	public void testGetIP1(){
		String sql = "select getip(ip),ip from fact_adst_history limit 10";
		this.hiveJt.query(sql, new RowMapper<Object>(){
			@Override
			public Object mapRow(ResultSet rs, int rownum) throws SQLException {
				System.out.println(rs.getString(1) + "|" + rs.getString(2));
				return null;
			}
		});
	}
	
	@Test
	public void testPartition() throws Exception{
		String sql = "select day from fact_adst_history limit 10";
		this.hiveJt.query(sql, new RowMapper<Object>(){
			@Override
			public Object mapRow(ResultSet rs, int rownum) throws SQLException {
				System.out.println(rs.getString(1));
				return null;
			}
		});
	}
	
	@Test
	public void testSpring() throws Exception{
		File f = new File("D://c.txt");
		FileWriter fw = new FileWriter(f, true);
		final BufferedWriter bw = new BufferedWriter(fw);
		String sql = "select count(distinct(tt.hid)) as usernum,tt.ven,tt.a "+
" from ( "+
" select hid,ven,getip(ip) as a"+
" from  fact_adst_history "+
" where day >= '2016-02-07' and day <= '2016-02-28' and amid in (2016020701,2016020702,2016011501,2016020703,2016020704)  "+
" and ven in (900101,900102)) tt "+
" where tt.a != 0 group by tt.ven,tt.a "+
" limit 1000";
		this.hiveJt.query(sql, new RowMapper<Object>(){
			@Override
			public Object mapRow(ResultSet rs, int rownum) throws SQLException {
				String usernum = rs.getString(1);
				String ven = rs.getString(2);
				String areaid = rs.getString(3);
				try {
					bw.write(usernum + "|" + ven + "|" + areaid);
					bw.newLine();
				} catch (IOException e) {
					e.printStackTrace();
				}
				return null;
			}
		});
		bw.close();
		System.out.println("done");
	}
	
	
	@Test
	public void testSpring1() throws Exception{
		File f = new File("D://a.txt");
		FileWriter fw = new FileWriter(f, true);
		final BufferedWriter bw = new BufferedWriter(fw);
		String sql = " select hid,ven,getip(ip),ip"+
" from  fact_adst_history "+
" where day >= '2016-02-07' and day <= '2016-02-22' and amid in (2016020701,2016020702,2016011501,2016020703,2016020704)  "+
" and ven in (900101,900102) limit 1000";
		this.hiveJt.query(sql, new RowMapper<Object>(){
			@Override
			public Object mapRow(ResultSet rs, int rownum) throws SQLException {
				String usernum = rs.getString(1);
				String ven = rs.getString(2);
				int areaid = rs.getInt(3);
				String ip = rs.getString(4);
				try {
					bw.write(usernum + "|" + ven + "|" + areaid + "|" + ip);
					bw.newLine();
				} catch (IOException e) {
					e.printStackTrace();
				}
				return null;
			}
		});
		bw.close();
		System.out.println("done");
	}
	
	@Test
	public void testSpring2() throws Exception{
		File f = new File("D://a.txt");
		FileWriter fw = new FileWriter(f, true);
		final BufferedWriter bw = new BufferedWriter(fw);
		String sql = " select getip(ip),ip"+
" from  fact_adst_history "+
" where day >= '2016-02-07' and day <= '2016-02-22' and amid in (2016020701,2016020702,2016011501,2016020703,2016020704)  "+
" and ven in (900101,900102) limit 1000";
		this.hiveJt.query(sql, new RowMapper<Object>(){
			@Override
			public Object mapRow(ResultSet rs, int rownum) throws SQLException {
				int areaid = rs.getInt(1);
				String ip = rs.getString(2);
				try {
					bw.write(areaid + "|" + ip);
					bw.newLine();
				} catch (IOException e) {
					e.printStackTrace();
				}
				return null;
			}
		});
		bw.close();
		System.out.println("done");
	}
	
	@Test
	public void testSpring3() throws Exception{
		File f = new File("D://a.txt");
		FileWriter fw = new FileWriter(f, true);
		final BufferedWriter bw = new BufferedWriter(fw);
		String sql = " select getip(ip)"+
" from  fact_adst_history "+
" where day >= '2016-02-07' and day <= '2016-02-22' and amid in (2016020701,2016020702,2016011501,2016020703,2016020704)  "+
" and ven in (900101,900102) limit 1000";
		this.hiveJt.query(sql, new RowMapper<Object>(){
			@Override
			public Object mapRow(ResultSet rs, int rownum) throws SQLException {
				String areaid = rs.getString(1);
				try {
					bw.write(areaid);
					bw.newLine();
				} catch (IOException e) {
					e.printStackTrace();
				}
				return null;
			}
		});
		bw.close();
		System.out.println("done");
	}
	
	@Test
	public void testSpring4() throws Exception{
		File f = new File("D://a.txt");
		FileWriter fw = new FileWriter(f, true);
		final BufferedWriter bw = new BufferedWriter(fw);
		String sql = " select getip(ip)"+
" from  fact_adst_history "+
" where day = '2016-02-20' and amid in (2016020701,2016020702,2016011501,2016020703,2016020704)  "+
" and ven in (900101,900102) limit 10000";
		this.hiveJt.query(sql, new RowMapper<Object>(){
			@Override
			public Object mapRow(ResultSet rs, int rownum) throws SQLException {
				String areaid = rs.getString(1);
				try {
					bw.write(areaid);
					bw.newLine();
				} catch (IOException e) {
					e.printStackTrace();
				}
				return null;
			}
		});
		bw.close();
		System.out.println("done");
	}
	
	@Test
	public void testSpring9() throws Exception{
		String sql = "select ip,amid,ven from fact_adst_history where day = '2016-02-20' limit 10";
		this.hiveJt.query(sql, new RowMapper<Object>(){
			@Override
			public Object mapRow(ResultSet rs, int rownum) throws SQLException {
				String ip = rs.getString(1);
				String amid = rs.getString(2);
				String ven = rs.getString(3);
				System.out.println(ip + "|" + amid + "|" + ven);
				return null;
			}
		});
		System.out.println("done");
	}
	
	@Test
	public void testSpring5() throws Exception{
		String sql = " select getip(ip),ip"+
" from  fact_adst_history "+
" where day = '2016-02-20' and amid in ('2016020701','2016020702','2016011501','2016020703','2016020704')  "+
" and ven in ('900101','900102') limit 1000";
		this.hiveJt.query(sql, new RowMapper<Object>(){
			@Override
			public Object mapRow(ResultSet rs, int rownum) throws SQLException {
				String areaid = rs.getString(1);
				String ip = rs.getString(2);
				System.out.println(areaid + "|" + ip);
				return null;
			}
		});
		System.out.println("done");
	}
	
	@Test
	public void testSpring8() throws Exception{
		String sql = " select getip(ip),ip"+
" from  fact_adst_history "+
" where day >= '2016-02-25' and day <= '2016-02-30' and ip in ('111.147.57.103','27.185.75.46') limit 1000";
		this.hiveJt.query(sql, new RowMapper<Object>(){
			@Override
			public Object mapRow(ResultSet rs, int rownum) throws SQLException {
				String areaid = rs.getString(1);
				String ip = rs.getString(2);
				System.out.println(areaid + "|" + ip);
				return null;
			}
		});
		System.out.println("done");
	}
	
	@Test
	public void testSpring6() throws Exception{
		String sql = " select getb(ip),ip from  aaa where day = '20160614'";
		this.hiveJt.query(sql, new RowMapper<Object>(){
			@Override
			public Object mapRow(ResultSet rs, int rownum) throws SQLException {
				String areaid = rs.getString(1);
				String ip = rs.getString(2);
				System.out.println(areaid + "|" + ip);
				return null;
			}
		});
		System.out.println("done");
	}
	
	@Test
	public void testSpring7() throws Exception{
		String sql = " select geta(hid),hid from  adap_count_tmp";
		this.hiveJt.query(sql, new RowMapper<Object>(){
			@Override
			public Object mapRow(ResultSet rs, int rownum) throws SQLException {
				String areaid = rs.getString(1);
				String ip = rs.getString(2);
				System.out.println(areaid + "|" + ip);
				return null;
			}
		});
		System.out.println("done");
	}

	
	@Test
	public void testGetIP2(){
		String sql = "select getb('1.10.7.255')";
		this.hiveJt.query(sql, new RowMapper<Object>(){
			@Override
			public Object mapRow(ResultSet rs, int rownum) throws SQLException {
				System.out.println(rs.getString(1));
				return null;
			}
		});
	}
}
