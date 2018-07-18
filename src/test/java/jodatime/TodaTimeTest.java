package jodatime;

import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Test;

import java.util.Locale;

public class TodaTimeTest {
	
	@Test
	public void jodaTimeTest() {
		DateTime startDateTime=new DateTime(2016, 6, 10, 0, 0,0);
		DateTime endDateTime=new DateTime(2017, 5, 30, 0, 0,0);

		DateTime currDatetime =  startDateTime;
		while(currDatetime.getMillis()<endDateTime.getMillis()){
			String strDate =  currDatetime.toString("yyyy-MM-dd");
//			System.out.println("alter table fact_vod_history add IF NOT EXISTS partition (day='"+strDate+"');");
//			fact_adap_adst_history
			System.out.println("alter table fact_adap_adst_history add IF NOT EXISTS partition (day='"+strDate+"');");
			currDatetime = currDatetime.plusDays(1);
		}
		
	}
	
	@Test
	public void jodaTimeTest2() {
	
		DateTimeFormatter format = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");  
		  
	    //时间解析  
	    DateTime dateTime = DateTime.parse("2012-12-21 23:22:45.0", format);  
	      
	    //时间格式化，输出==> 2012/12/21 23:22:45 Fri  
	    String string_u = dateTime.toString("yyyy/MM/dd HH:mm:ss EE");  
	    System.out.println(string_u);  
	      
	    //格式化带Locale，输出==> 2012年12月21日 23:22:45 星期五  
	    String string_c = dateTime.toString("yyyy年MM月dd日 HH:mm:ss EE",Locale.CHINESE);  
	    System.out.println(string_c);  
	}

    @Test
    public void jodaTimeFromLong() {

       /* DateTime dt = new DateTime(1526997609000l);
        String strDt = dt.toString("yyyy-MM-dd HH:mm:ss");
        System.out.println(strDt);*/
        DateTime dt = new DateTime();
        int keepSize=1;
        DateTime dt2 = dt.plusDays(-keepSize);
        System.out.println(dt.toString());
        System.out.println(dt2.toString());

    }

    @Test
    public  void replaceTest(){
        String str = "2018-06-03 03:04:05";
        String str2 = str.replaceAll("-| |:","");
        System.out.println(str);
        System.out.println(str2);
        //错误：无法直接替换所有匹配字符。
        String str3 = StringUtils.replace(str,"-| |:","");
        System.out.println(str3);
    }

}
