package adstatMemRepeatKey;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.lang.StringUtils;


public class getErrorHidOemid {
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Map<String,String>  hidMap =  new HashMap<String,String>();
		
		
	
		File errKeyInput = new File("E:\\tmp\\oemid\\code_all20161206_22_all.key");
		
		
		BufferedReader brKey = null;
	
		//数据样例：
		//1010000801-408bf68ad812-20161206:9
	
			try {
				brKey = new BufferedReader(new FileReader(errKeyInput));
				String line = null;
				while ((line = brKey.readLine()) != null) {
					String 	hid = StringUtils.substringBetween(line,"-","-"); 
					String value = StringUtils.substringAfter(line, ":");
					
//					if(Integer.valueOf(value)>10){
						hidMap.put(hid, value);
						hidMap.put(hid.toUpperCase(), value);
//					}
				}
				
				System.out.println("hidMap.size="+hidMap.size());
				
				brKey.close();
			} catch (FileNotFoundException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
	
		
		
		//解析日志文件中的OEMID
			
		String path = "E:\\tmp\\oemid\\";
		File f = new File(path);
		String[] files = f.list(new FilenameFilter() {
			public boolean accept(File dir, String name) {
				if (name.endsWith(".txt")) {
					return true;
				}
				return false;
			}
		});
		
		//循环处理
		for (String processFile : files) {
			System.out.println("============开始处理文件["+processFile+"]============");
			//开始处理文件
			//"E:\\tmp\\ip\\2016101101_900101_20161112.txt"
			File input = new File(path+processFile);
			BufferedReader br = null;
			File f1 = null;
			FileWriter fw1 = null;
			BufferedWriter bw1 = null;
			//数据样例：
			//"GET /15/1.gif?sessionid=122906060378426377390000&uv=0&type=0&hid=408BF6A87265&oemid=200053&uid=19255437&provinceid=25&cityid=2502&adposid=15101010&amid=1050000281&channelid=101&planid=1010000801&admt=5&pv=0&vv=1&isadap=0&ip=183.197.59.143 HTTP/1.1&starttime=20161205235824"
			try {
				br = new BufferedReader(new FileReader(input));
				String line = null;
				// 写文件
				String outFile =  processFile.replaceAll(".txt", ".out");
				//"E:\\tmp\\ip\\2016101101_900101_20161112.out"
				f1 = new File(path+outFile);
				fw1 = new FileWriter(f1, true);
				bw1 = new BufferedWriter(fw1);
				HashMap<String,String>  hidOemidMap =  new HashMap<String,String>();
				HashMap<String,Integer> oemidmap = new HashMap<String,Integer>();
				HashMap<String,HashSet<String>> oemidHidSizeMap = new HashMap<String,HashSet<String>>();
				
				int count=0;
				// 读
				while ((line = br.readLine()) != null) {
					count++;
					String hid = StringUtils.substringBetween(line,"hid=","&oemid=");
					String oemid = StringUtils.substringBetween(line, "oemid=","&uid=");
					
					if(hidMap.get(hid)!=null){
						/*
						if(oemidmap.get(oemid)!=null){
							int num = oemidmap.get(oemid);
							num++;
							oemidmap.put(oemid, num);
						}else{
							oemidmap.put(oemid, 1);
						}*/
						
//						hidOemidMap.put(hid, oemid);
						
						if(oemidHidSizeMap.get(oemid)!=null){
							HashSet<String> tmpSet = oemidHidSizeMap.get(oemid);
							tmpSet.add(hid);
							oemidHidSizeMap.put(oemid, tmpSet);
						}else{
							HashSet<String> set = new HashSet<String>();
							set.add(hid);
							oemidHidSizeMap.put(oemid, set);
						}
						
						
					}		
				}				
				
				//处理oemidmap
			/*	Iterator oemit = oemidmap.keySet().iterator();
				
				while(oemit.hasNext()){
					String oemid = (String)oemit.next();
					Integer value = (Integer)oemidmap.get(oemid);
					// 写					
					bw1.write(oemid+","+value);
					bw1.newLine();
				}*/
								
				
				//hid 和 oemid 映射关系。
				
				/*Iterator hidOemidIt = hidOemidMap.keySet().iterator();
				
				while(hidOemidIt.hasNext()){
						String hid = (String)hidOemidIt.next();
						String oemid = (String)hidOemidMap.get(hid);
						// 写					
						bw1.write(hid+","+oemid);
						bw1.newLine();
					}*/
				
				//未控制住oemid 包含hid个数查看
				Iterator oemidHidIt = oemidHidSizeMap.keySet().iterator();
				
				while(oemidHidIt.hasNext()){
						String oemid = (String)oemidHidIt.next();
						HashSet<String> set = (HashSet<String>)oemidHidSizeMap.get(oemid);
						// 写					
						bw1.write(oemid+","+set.size());
						bw1.newLine();
				}
				
				System.out.println("文件["+processFile+"]处理完成，处理数据["+count+"]条!");
				
			} catch (IOException ex) {
				ex.printStackTrace();
			} finally {
				try {
					if (input != null && input.exists()) {
						input.delete();
					}
					if (br != null)
						br.close();
	
					if (bw1 != null) {
						bw1.flush();
						bw1.close();
					}
	
				} catch (IOException e) {
					e.printStackTrace();
				}
	
			}
		
		}
		
		System.out.println("done!");

	}
	
	

	public static Map<String,String> lineMap(String line){
		String currStr = "";
		try {
			String[] arr = line.split("\\&");
			Map<String,String> map = new HashMap<String,String>();
			//倒序输出,防止starttime多个的时候取串前面出现的starttime
			for(String str : arr){
			
				currStr = str;
			
				String[] kv = str.split("\\=");
				if(kv.length>=2){
					String k = kv[0];
					String v = kv[1];
					map.put(k, v);
				}else{
					//遇到值为空情况
					String k = kv[0];
					map.put(k, "");
				}
			}
			return map;
		} catch (Exception e) {
			System.out.println("解析串["+line+"]中["+currStr+"]时报错："+e);
			return null;
		}
	}

}
