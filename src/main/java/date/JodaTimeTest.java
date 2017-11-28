package date;

import org.joda.time.DateTime;
import utils.DateUtils;

/**
 * Created by Administrator on 2017-9-29.
 */
public class JodaTimeTest {
    public static void main(String[] args){
//        System.out.println("111111111111111111122222222222222222");
       /* DateTime dt = new DateTime();
        DateTime nextDt = dt.plusDays(1);
        System.out.println(dt.isBefore(dt.getMillis()));

        String dtStr = dt.toString("yyy-MM-dd HH:mm:00");
        System.out.println(dtStr);*/

       //整点十分钟

        DateTime currDateStartTime = new DateTime();
        //转化为整点10分钟时刻
        DateTime tenEndTime =  currDateStartTime.withMillis(DateUtils.getEndTen(currDateStartTime.getMillis(),10));
        String str = currDateStartTime.toString("yyyy-MM-dd HH:mm:ss");
        String strten = tenEndTime.toString("yyyy-MM-dd HH:mm:ss");
        System.out.println(str);
        System.out.println(strten);


    }
}
