package test;

import org.joda.time.DateTime;
import org.joda.time.Period;
import org.joda.time.PeriodType;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * Created by Administrator on 2017-12-22.
 */
public class JodaTimeTest {

    public static void main(String[] args){
        betweentest();
    }

    public static void betweentest(){
       /* DateTime dt = new DateTime();
        DateTime preday =  new DateTime(2017,9,1,0,0,0);
        Period p = new Period(preday, dt, PeriodType.days());
        int days = p.getDays();
        System.out.println(days);*/

   /*     DateTime dt = new DateTime();

        String stra =  dt.toString("yyyyMMddHHmmss",Locale.ENGLISH);

       System.out.println(stra);*/




//        DateTime.Property pDoW = dt.monthOfYear();
//        String strST = pDoW.getAsShortText(Locale.ENGLISH); // returns "Mon", "Tue", etc.

//        System.out.println(strST);

        String str  = "15/Jan/2018:18:57:02";
        DateTimeFormatter fmt = DateTimeFormat.forPattern("dd/MMM/yyyy:HH:mm:ss");
        DateTimeFormatter englishFmt = fmt.withLocale(Locale.ENGLISH);
        DateTime dateTime2 = DateTime.parse(str, englishFmt);
        String str2 =  dateTime2.toString("yyyyMMddHHmmss");
        System.out.println(str2);


        /*SimpleDateFormat dateFormat = new SimpleDateFormat("EEE MMM d HH:mm:ss 'CST' yyyy", Locale.ENGLISH);
        Date date = dateFormat.parse("Fri Aug 28 18:08:30 CST 2015");*/
      /*  String str  = "15/Jan/2018:18:57:01";
        SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
        try {
            Date date = dateFormat.parse(str);


            DateTime dt2 = new DateTime();
            DateTime dtnew =      dt2.withMillis(date.getTime());
            String dateStr = dtnew.toString("yyyyMMddHHmmss");
            System.out.println(dateStr);
        } catch (ParseException e) {
            e.printStackTrace();
        }*/


    }
}
