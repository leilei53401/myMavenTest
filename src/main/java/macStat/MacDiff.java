package macStat;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
/**
 * 计算两次差异量，并记录结果
 * @author shaoyl
 *
 */
public class MacDiff {
	private static HashMap<String,MacInfo> macMap_2015 = new HashMap<String,MacInfo>();	
	private static Set<String> set2016 = new HashSet<String>();
	private static HashMap<String,MacInfo> macMap_diff = new HashMap<String,MacInfo>();	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		parseFile("d:\\tmp\\mac_out\\mac_201503-201603.txt",0);
		parseFile("d:\\tmp\\mac_out\\mac_201603-201703.txt",1);
		printFile();
	}
	
	
	private static void parseFile(String wholePath,int type){

		System.out.println("============开始处理文件["+wholePath+"]============");
		//开始处理文件
		File input = new File(wholePath);
		BufferedReader br = null;

		try {
			br = new BufferedReader(new FileReader(input));
			String line = null;
		
			int count=0;
			// 读
			//232946|E0BC431D314D|135|2300|23|1897867240|1457413100|1457414439|1339|
			while ((line = br.readLine()) != null) {
				
				String[] macArray = line.split("\\|");
				if(macArray.length<5){
					System.err.println("line=["+line+"]不符合要求!");
					continue;
				}
//				count++;
				//userid | mac | oemid | cityid | provinceid | userip十进制 | 起播时间 | 停播时间 | 播放时长
			    //232946|E0BC431D314D|135|2300|23|1897867240|1457444070|1457446682|2612|
				String mac  = macArray[0].toUpperCase();
				if(type == 0){
			
					MacInfo mInfo =  new MacInfo();
					mInfo.setMac(mac);
					mInfo.setOemid(macArray[1]);
					mInfo.setCityid(macArray[2]);
					mInfo.setProvinceid(macArray[3]);
					mInfo.setUserip(macArray[4]);
					mInfo.setExp(Long.parseLong(macArray[5]));
					macMap_2015.put(mac, mInfo);
				}else{
					set2016.add(mac);
				}
				
				count++;
			
			}
			System.out.println("文件["+wholePath+"]处理完成，处理数据["+count+"]条!");
			
			
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
			System.out.println("=====开始处理差异数据======");
			System.out.println("macMap_2015.size()="+macMap_2015.size());
			System.out.println("set2016.size()="+set2016.size());
			
			Iterator<String> it2015 = macMap_2015.keySet().iterator();
			while(it2015.hasNext()){
				String key = it2015.next();
				if(!set2016.contains(key)){
					macMap_diff.put(key, macMap_2015.get(key));
				}
			}
			
			
			//打印结果
			
			File f1 = null;
			FileWriter fw1 = null;
			BufferedWriter bw1 = null;
			//数据样例：
			//<2016-11-30 00:00:00,111>  INFO (?:?) [TaskExePool-2-TaskProcessor13] (com.danga.MemCached.MemCachedClient) - ++++ key not found to incr/decr for key: 1010000790-1129
			try {
			
				// 写文件
				f1 = new File("D:\\tmp\\mac_out\\mac_diff.txt");
				fw1 = new FileWriter(f1, true);
				bw1 = new BufferedWriter(fw1);
				// 读
				Iterator<MacInfo> it = macMap_diff.values().iterator();
				
				while (it.hasNext()){
					MacInfo macInfo =  it.next();
					String newLine = macInfo.getMac()+"|"+ macInfo.getOemid()+"|"
							+macInfo.getCityid()+"|"+macInfo.getProvinceid()+"|"
							+macInfo.getUserip()+"|"+macInfo.getExp();
					bw1.write(newLine);
					bw1.newLine();
				}
				
				System.out.println("文件处理完成，处理数据["+macMap_diff.size()+"]条!");
				
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
