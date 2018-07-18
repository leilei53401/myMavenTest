package base;

import org.joda.time.DateTime;
import org.junit.Test;

/**
 * Created by Administrator on 2017-9-29.
 */
public class JavaBaseTest {


    @Test
    public void testArray(){
        
        String a = "192.168.1.1";
        
        String [] array = a.split(",");
        System.out.println(array.length);
        System.out.println(array[0]);
        
    }


    @Test
    public void testDayOfWeekJodaTime(){
        DateTime dt = new DateTime();
        DateTime.Property p = dt.dayOfWeek();
        System.out.println(p.getAsText());
        System.out.println(p.getAsString());

        System.out.println(p.get());

    }
}
