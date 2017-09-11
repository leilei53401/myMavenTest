package file;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;

public class CountInterfaceSqlLogFile {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
	long all = 0L;
	long ok =  0L;
	long fail = 0L;
	
	File theFile = new File("/opt/data/adstat/file/20161215_701_100.txt");
	try {
		LineIterator it = FileUtils.lineIterator(theFile, "UTF-8");
		try {
		    while (it.hasNext()) {
		    	String line = it.nextLine();
		    	//数据样例
				//INSERT INTO rt_og_play_log5(oemid,oemname,hid,adverno,agentno,spid,provinceid,cityid,channelid,programid,movname,sid,adlength,coderate,resolution,starttime,endtime,playtime,speed,fullplay,amid,adname,fid,logid,adposid,sessionid,admt,uid,batchplanid,adorder) VALUES ('705','116.248.71.36','1CA770A37089','900005','900005','100001','83','8317','101','112','116.248.71.36','null','null','null','null','2016-12-15 00:00:06','2016-12-15 00:00:06','0', CAST('0'/1024 AS SIGNED ),'1','1050000508','161207夏有乔木-免费','179d4af67acc07faed896675571e07be','74f84724_2864423011','701','452377821076530841200000','5','126169642','0','1') ON DUPLICATE KEY UPDATE fullplay=values(fullplay);
//				String s = "INSERT INTO rt_og_play_log5(oemid,oemname,hid,adverno,agentno,spid,provinceid,cityid,channelid,programid,movname,sid,adlength,coderate,resolution,starttime,endtime,playtime,speed,fullplay,amid,adname,fid,logid,adposid,sessionid,admt,uid,batchplanid,adorder) VALUES ('705','116.248.71.36','1CA770A37089','900005','900005','100001','83','8317','101','112','116.248.71.36','null','null','null','null','2016-12-15 00:00:06','2016-12-15 00:00:06','0', CAST('0'/1024 AS SIGNED ),'1','1050000508','161207夏有乔木-免费','179d4af67acc07faed896675571e07be','74f84724_2864423011','701','452377821076530841200000','5','126169642','0','1') ON DUPLICATE KEY UPDATE fullplay=values(fullplay);";
				
				try {
					   if(parse(line)){
						   ok++;
						}else{
							fail++;
						}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					fail++;
				}
					all++;
		    }
		
    
			} finally {
			    LineIterator.closeQuietly(it);
			}
		System.out.println("共处理数据["+all+"]条，成功["+ok+"]条，其他["+fail+"]条");
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	}

	
	public static boolean parse(String data) throws Exception{	
		
		String values = StringUtils.substringBetween(data, "VALUES (", ") ON DUPLICATE KEY");
		
//		System.out.println("values = " + values);
		
		String [] arrays = values.split(",");
		
		if(arrays.length<30){
			System.out.println("数据["+data+"]处理异常!");
			return false;
		}
		
	/*	System.out.println(arrays.length);
		
		for(String str:arrays){
		System.out.println(str);	
		}*/
		
		String adposid = arrays[24];
		String adposidValue =StringUtils.substringBetween(adposid, "'", "'");
		
		if(!"701".equals(adposidValue)){
			System.out.println("数据["+data+"]不是前贴数据");
			return false;
		}
		String logid =  arrays[23];
		String logidValue =StringUtils.substringBetween(logid, "'", "'");
		
		if(null!=logidValue && logidValue.contains("_")){
			//取最后一位
			String lastPosValue = logidValue.substring(logidValue.length()-1,logidValue.length());
			if("1".equals(lastPosValue)){
				return true;
			}else{
				return false;
			}
		}else{
			return false;
		}
		
	}

}
