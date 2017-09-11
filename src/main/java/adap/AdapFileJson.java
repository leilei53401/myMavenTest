package adap;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import net.sf.json.JSONObject;

import org.apache.commons.lang.StringUtils;


public class AdapFileJson {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		//初始化常见字典表
		Map<String,String> venMap = new HashMap<String,String>();

		venMap.put("900101", "海信");
		venMap.put("900102", "创维");
		venMap.put("900103", "康佳");
		venMap.put("900107", "联想");
		
		//初始化节目字典表
		
		Map<String,String> amInfoMap = new HashMap<String,String>();

		amInfoMap.put("2016101001", "创维-汉兰达_开机");
		amInfoMap.put("2016102801", "创维-广汽致炫2");
		amInfoMap.put("2016111001", "凡士林开机视频");
		amInfoMap.put("2016101101", "海信-汉兰达");
		amInfoMap.put("2016102901", "海信-广汽致炫2");
		amInfoMap.put("2016101201", "康佳-汉兰达_开机 ");
		amInfoMap.put("2016103001", "康佳-广汽致炫2");
		amInfoMap.put("2016092401", "梦龙开机");
		
		//目录
//		String path = "E:\\tmp\\ip\\900103_examplefile\\";
		String path = "E:\\tmp\\ip\\";
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
			//[INFO ] [2016-11-10 00:01:14] loggerAdStRequestInfo - amid=2016101001&ven=900102&ip=223.210.50.251&mac=fca386892359&devid=E3500&ts=1478707274&size=40&d=15&stamp=1478707274587
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
					String param = StringUtils.substringAfter(line, "loggerAdSt - param==>");
					JSONObject jsonObj = JSONObject.fromObject(param);
					if(jsonObj == null){
						System.out.println("播放串["+line+"]解析异常,jsonObj==null!");
						continue;
					}
					
					String ven = jsonObj.getString("ven");
					String venName = venMap.get(ven);
					String amid = jsonObj.getString("amid");
					String adName = amInfoMap.get(amid);
					String ip = jsonObj.getString("ip");
					// 写
					String newline = ven+","+venName+","+amid+","+adName+","+ip;
					
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
