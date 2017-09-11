package file;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

public class InterfaceFile {
	
	//导流位转化
	/*adapter.paster.pos = 15101010,15101011,17101110,17101310,17101210,17101410
			match.adapter.paster.pos = 701,702,704,705,706,707*/
	//"GET /15/1.gif?sessionid=1481525279433252749&uv=0&type=1&hid=0a0adb04b5e5&oemid=10000&uid=148053832&provinceid=10&cityid=1009&adposid=15101010&amid=1050000464&channelid=105&planid=1010000865&admt=5&pv=1&vv=1&isadap=1&ip=119.57.155.125&mid=56457628 HTTP/1.1&starttime=20161212144759"
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String path = "E:\\tmp\\interfile\\interlogtail5.txt";
		File theFile = new File(path);
		AtomicLong num = new AtomicLong(0);
		try {
			LineIterator it = FileUtils.lineIterator(theFile, "UTF-8");
			try {
			    while (it.hasNext()) {
			    	String line ="";
			        try {
						line = it.nextLine();
						
						String values = StringUtils.substringBetween(line, "values('", ");");
						
						String [] arrays = values.split("','");
						for(String a:arrays){
							System.out.println(a);
						}
						
						
						//response
					/*	String response=StringUtils.substringBetween(line, "<?xml", "</response>");
						
						System.out.println(response);
						//转化为xml解析。
						parseXml("<?xml "+response+" </response>");*/
						
						if(num.incrementAndGet()%100 == 0){
							System.out.println("playlog date: " + num.get());
						}
//						System.out.println(line);
					} catch (Exception e) {
						System.err.println("处理出错："+e);
					}
			     
			    }
			} finally {
			    LineIterator.closeQuietly(it);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	//解析xml字符串。
	private static HashMap<String,String> parseXml(String responseStr){
		responseStr =  responseStr.replaceAll("\\\\r\\\\n","");
		responseStr =  responseStr.replaceAll("\\\\","");
		System.out.println("===== responseStr start ===========");
		System.out.println(responseStr);
		System.out.println("===== responseStr end ===========");
		try {
			SAXReader saxReader = new SAXReader();
//			Document document = saxReader.read("E:\\tmp\\interfile\\ad3.xml");
//			Document document = saxReader.read(responseStr);
//			Document document = saxReader.read(new ByteArrayInputStream(responseStr.getBytes()));
			Document document = DocumentHelper.parseText(responseStr);  
			Element responseEle=document.getRootElement();
			Element paramsEle = responseEle.element("params");
			String params = paramsEle.getTextTrim();
			System.out.println(params);
		} catch (DocumentException e) {
			e.printStackTrace();
		}
        
		return null;
	}
	
	public static void main_2(String[] args) {
		parseXml("");
	}

}
