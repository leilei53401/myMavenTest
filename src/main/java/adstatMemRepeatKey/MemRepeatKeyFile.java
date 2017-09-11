package adstatMemRepeatKey;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;


public class MemRepeatKeyFile {
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		//目录
		String path = "E:\\tmp\\";
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
			//<2016-11-30 00:00:00,111>  INFO (?:?) [TaskExePool-2-TaskProcessor13] (com.danga.MemCached.MemCachedClient) - ++++ key not found to incr/decr for key: 1010000790-1129
			try {
				br = new BufferedReader(new FileReader(input));
				String line = null;
				// 写文件
				String outFile =  processFile.replaceAll(".txt", ".out");
				//"E:\\tmp\\ip\\2016101101_900101_20161112.out"
				f1 = new File(path+outFile);
				fw1 = new FileWriter(f1, true);
				bw1 = new BufferedWriter(fw1);
				int count=0;
				// 读
				while ((line = br.readLine()) != null) {
					count++;
					String time_all = StringUtils.substringBetween(line,"<",">");
					String time = StringUtils.substringBefore(time_all, ",");
					String key = StringUtils.substringAfter(line, "for key:").trim();
			
					// 写
					String newline = time+","+key;
					
					bw1.write(newline);
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
