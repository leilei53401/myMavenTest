package hive;

import macStat.MacDiff;
import org.joda.time.DateTime;
import org.junit.Test;

/**
 * Created by Administrator on 2017-9-29.
 */
public class JodaTimeTest {


    @Test
    public void testCmpJodaTime(){
        DateTime dt = new DateTime();
        DateTime nextDt = dt.plusDays(1);

        System.out.println(dt.isBefore(nextDt.getMillis()));
    }


    @Test
    public void testDayOfWeekJodaTime(){
        DateTime dt = new DateTime();
        DateTime.Property p = dt.dayOfWeek();

        System.out.println(p.getAsString());
        System.out.println(p.getAsText());
        System.out.println(p.get());

    }
}
