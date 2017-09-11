package udf;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;

/**
 * 根据IP地址获取区域地市id
 * @author shaoyl
 *
 */
public class GetAreaIdByIP  extends UDF {
	
	/**
	 * 存放数据库查出来网段记录
	 */
	public static RangeMap<Long, Integer> rangeMap = TreeRangeMap.create();
	
	static {
		
		rangeMap = DbUtil.getAreaDic();
		
		System.out.println(rangeMap.toString());
	
	}
	
	public int evaluate(String obj) {
		int code  = 0;
		if(StringUtils.isBlank(obj) || obj.indexOf(".") < 0 ){
			return code;
		}
		long longIp = 0l;
		
		try{
			longIp = ipToDecimal(obj);
		}
		catch(Exception e){
			return 0 ;
		}
		
		
		//serverip有问题的记录
		if(longIp == 0l){
			return 0;
		}
		Object o=rangeMap.get(longIp);
		if(o==null){
			return 0;
		}
		code = Integer.parseInt(o.toString());
		return code;
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
	
	public static void main(String[] args){
		GetAreaIdByIP ns = new GetAreaIdByIP();
		System.out.println("-------------------");
		int i = ns.evaluate("123.125.114.144");
		
		System.out.println("-------------------");
		System.out.println(i);
	}
	
}
