package test;

import java.util.Random;

/**
 * Created by Administrator on 2018-2-5.
 */
public class RandomTest {
    public static void main(String[] args){
         Random r = new Random();
        int i = r.nextInt(2);
        System.out.println("i="+i);
    }
}
