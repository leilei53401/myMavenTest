package AnhuiDianxinYiqi;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * 安徽电信一期，处理文件，为了计算累计触达。
 * @author Administrator
 *
 */
public class AnHuiDianxinYiQi {

	/**
	 * 数据样例
[INFO ] [2016-11-02 00:00:08] loggerAdStRequestInfo - ty=IP906H_36T1&ts=1478015806231&ven=900111&devid=0bc76860dd9c6467951fd760091b6b9f&st=authen&aid=AUTHEN201610140122&amid=2016092920&city=0551&pos=10151010&ip=127.0.0.1&stamp=1478016008881
[INFO ] [2016-11-02 00:00:09] loggerAdStRequestInfo - devid=h55016769387&st=logo&ty=1477996991120&pos=10151010&city=0551&ip=192.168.1.1&amid=2016092918&aid=LOGO201611010160&ven=900111&ts=1477996991120&stamp=1478016009037
[INFO ] [2016-11-02 00:00:09] loggerAdStRequestInfo - devid=h55016769387&st=logo&ty=1477996991120&pos=10151010&city=0551&ip=192.168.1.1&amid=2016092918&aid=LOGO201611010160&ven=900111&ts=1477996991120&stamp=1478016009243
[INFO ] [2016-11-02 00:00:09] loggerAdStRequestInfo - ty=B600V4H&ts=1478015935683&ven=900111&devid=e8d492189913de168a7735a80faf9962&st=Logo&aid=LOGO201611010164&amid=2016092918&city=5571&pos=10151010&ip=172.16.37.71&stamp=1478016009343
[INFO ] [2016-11-02 00:00:09] loggerAdStRequestInfo - ty=B600V4H&ts=1478015935686&ven=900111&devid=e8d492189913de168a7735a80faf9962&st=load&aid=LOAD201609230127&amid=2016092919&city=5571&pos=10151010&ip=172.16.37.71&stamp=1478016009345
[INFO ] [2016-11-02 00:00:09] loggerAdStRequestInfo - ty=B600V4H&ts=1478015935689&ven=900111&devid=e8d492189913de168a7735a80faf9962&st=authen&aid=AUTHEN201609210107&amid=2016092920&city=5571&pos=10151010&ip=172.16.37.71&stamp=1478016009347
	
	 */
	public static void main(String[] args) {

		File file = new File("e:\\work\\tmp\\900111_20161102-94-2.txt");
		
		//处理类型字典表

		
		LineIterator it = null;
		String line = "";
		
		try {
			it = FileUtils.lineIterator(file, "UTF-8");
			while (it.hasNext()) {
				line = it.nextLine();
				String param  = StringUtils.substringAfterLast(line, " - ");
				System.out.println("param =="+param);
			}
			
		
		} catch (Exception e) {
			System.out.println("err line = " + line);
			e.printStackTrace();
		} finally {
			LineIterator.closeQuietly(it);
		}

	}
	
	
	

	public static Map<String,String> lineMap(String line){
		String s ="";
		try {
			String[] arr = line.split("\\&|\\s+|\\?");
			Map<String,String> map = new HashMap<String,String>();
			//倒序输出,防止starttime多个的时候取串前面出现的starttime
			for(String str : arr){
			//for(int i=arr.length-1;i>=0;i--){
				//String str = arr[i];
//				System.out.println(str);
				s = str;
				if(str.contains("HTTP")){
					str = str.split("\\s+")[0];
				}else if(str.contains("GET ")){
					str = str.split("\\?")[1];
				}
				String[] kv = str.split("\\=");
//				System.out.println(kv.length);
				if(kv.length>1){
					String k = kv[0];
					String v = kv[1];
					map.put(k, v);
				}else{
					String k = kv[0];
					map.put(k, "");
				}
			
			}
			return map;
		} catch (Exception e) {
		   System.out.println("异常："+s);
			e.printStackTrace();
			return null;
		}
	}
}
