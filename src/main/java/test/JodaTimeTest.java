package test;

import org.joda.time.DateTime;
import org.joda.time.Period;
import org.joda.time.PeriodType;

/**
 * Created by Administrator on 2017-12-22.
 */
public class JodaTimeTest {

    public static void main(String[] args){
        betweentest();
    }

    public static void betweentest(){
        DateTime dt = new DateTime();
        DateTime preday =  new DateTime(2017,9,1,0,0,0);
        Period p = new Period(preday, dt, PeriodType.days());
        int days = p.getDays();
        System.out.println(days);

    }
}
