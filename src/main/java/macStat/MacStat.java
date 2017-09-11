package macStat;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;

public class MacStat {
	
	private static HashMap<String,MacInfo> allMap = new HashMap<String,MacInfo>();	

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		//解析去重，统计次数
		 start();
//		 printFile();
		
	}
	
	public static void start() {
		DateTime startDateTime=new DateTime(2015, 03, 01, 0, 0,0);
		DateTime endDateTime=new DateTime(2017, 03, 1, 0, 0,0);
/*		System.out.println(dateTime.getMillis());
//		DateTime date = DateTime.parse(startTime, fromat);
		String strDate =  dateTime.toString("yyyy-MM-dd");
		DateTime dateTime2 = dateTime.plusDays(1);
		String strDate2 =  dateTime2.toString("yyyy-MM-dd");
		System.out.println(strDate2);*/
		DateTime currDatetime =  startDateTime;
		while(currDatetime.getMillis()<endDateTime.getMillis()){
			String strDate =  currDatetime.toString("yyyy-MM-dd");
			//执行sql查询并写文件。
			System.out.println("开始处理["+strDate+"]的数据，请稍后...");
			
			try {
				parseFile(strDate);
				
			} catch (Exception e) {
				System.err.println(e);
				System.out.println("处理["+strDate+"]的数据出错!");
				System.out.println("*********************************");
				currDatetime = currDatetime.plusDays(1);
				continue;
			}
			
			System.out.println("结束处理["+strDate+"]的数据!");
			System.out.println("*********************************");
			currDatetime = currDatetime.plusDays(1);
		}
		System.out.println("开始导出数据......");
		printFile();
		System.out.println("结束导出数据!");
		System.out.println("=======================");
	}
	
	private static void parseFile(String date){
		String fileName = "wuhan_detail_"+date+".txt";
		String path = "d:\\tmp\\mac_wuhan\\";

		System.out.println("============开始处理文件["+fileName+"]============");
		//开始处理文件
		File input = new File(path+fileName);
		BufferedReader br = null;
		File f1 = null;
		FileWriter fw1 = null;
		BufferedWriter bw1 = null;

		try {
			br = new BufferedReader(new FileReader(input));
			String line = null;
		
			int count=0;
			// 读
			//232946|E0BC431D314D|135|2300|23|1897867240|1457413100|1457414439|1339|
			while ((line = br.readLine()) != null) {
				count++;
				String[] macArray = line.split("\\|");
				if(macArray.length<6){
					System.err.println("line=["+line+"]不符合要求!");
					continue;
				}
				
				String mac  = macArray[1].toUpperCase();
				
				if(allMap.get(mac)!=null){
					MacInfo mInfo = allMap.get(mac);
					long num = mInfo.getExp();
					num = num + 1L;
					mInfo.setExp(num);
				}else{
					//userid | mac | oemid | cityid | provinceid | userip十进制 | 起播时间 | 停播时间 | 播放时长
				    //232946|E0BC431D314D|135|2300|23|1897867240|1457444070|1457446682|2612|
					MacInfo mInfo =  new MacInfo();
					mInfo.setMac(mac);
					mInfo.setOemid(macArray[2]);
					mInfo.setCityid(macArray[3]);
					mInfo.setProvinceid(macArray[4]);
					mInfo.setUserip(macArray[5]);
					mInfo.setExp(1L);
					allMap.put(mac, mInfo);
				}
		
			
			}
			
			System.out.println("文件["+fileName+"]处理完成，处理数据["+count+"]条!");
			
		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			try {
				if (input != null && input.exists()) {
					input.delete();
				}
				if (br != null)
					br.close();

			} catch (IOException e) {
				e.printStackTrace();
			}

		}
	}
	
	//打印结果文件
	private static void printFile(){
		
		File f1 = null;
		FileWriter fw1 = null;
		BufferedWriter bw1 = null;
		//数据样例：
		//<2016-11-30 00:00:00,111>  INFO (?:?) [TaskExePool-2-TaskProcessor13] (com.danga.MemCached.MemCachedClient) - ++++ key not found to incr/decr for key: 1010000790-1129
		try {
		
			// 写文件
			f1 = new File("D:\\tmp\\mac_all\\mac_wuhan_all.txt");
			fw1 = new FileWriter(f1, true);
			bw1 = new BufferedWriter(fw1);
			int count=0;
			// 读
			Iterator<MacInfo> it = allMap.values().iterator();
			
			while (it.hasNext()){
				MacInfo macInfo =  it.next();
				String newLine = macInfo.getMac()+"|"+ macInfo.getOemid()+"|"
						+macInfo.getCityid()+"|"+macInfo.getProvinceid()+"|"
						+macInfo.getUserip()+"|"+macInfo.getExp();
				bw1.write(newLine);
				bw1.newLine();
			}
			
			System.out.println("文件处理完成，处理数据["+allMap.size()+"]条!");
			
		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			try {
				if (bw1 != null) {
					bw1.flush();
					bw1.close();
				}

			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	
	System.out.println("done!");
		
	}

}
