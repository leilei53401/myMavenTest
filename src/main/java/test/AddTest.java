package test;

/**
 * Created by Administrator on 2018-3-15.
 */
public class AddTest {

    public static void main(String[] args){
        int a = 1, b = 1, c = 1, d = 1;
        a++;
        ++b;
        c = c++;
        d = ++d;
        System.out.println(a + "\t" + b + "\t" + c + "\t" + d);
    }
}
