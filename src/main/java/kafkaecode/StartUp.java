package kafkaecode;


import java.io.IOException;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class StartUp {
	public static void main(String[] args) throws IOException {
		try {
			final ApplicationContext ac = new  ClassPathXmlApplicationContext("applicationContext.xml");
			Client client = ac.getBean(Client.class);
			ClientNew clientNew = ac.getBean(ClientNew.class);
			/**
			 * sendtime db like 1458777622,pass in like 'yyyy-MM-dd HH:mm:ss'
			 * 前台判断输入时间合法性,最好使用时间控件
			 */
		/*	List<KafkaErrorCodeSimple> list = client.getErrorCodeList("2016-04-11 13:00:23","2016-04-11 23:30:24","","");
			for(KafkaErrorCodeSimple s : list){
			//	System.out.println(s.getClient() + "," + s.getSendtime() + "," + s.getErrorlevel());
			}*/
			//client.getErrorCodeSimpleList("2016-03-24 12:23:23","2016-03-24 12:23:24","","");
			
			/**
			 * adstat 相关查询 
			 */
			System.out.println("==============start================");
			//查询导流位统计
//			client.findAdposidSum();
			//查询终端类型下终端量
			client.testHIveSql();
//			clientNew.testHIveSql();
			
			System.out.println("==============done================");
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
	}
}
