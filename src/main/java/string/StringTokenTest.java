package string;

import org.apache.commons.lang.StringUtils;

import java.util.StringTokenizer;

/**
 * Created by shaoyl on 2018-2-28.
 */
public class StringTokenTest {

    public static void main(String[] args){
//        test1();
        System.out.println("===============================");
//        test2();

        test3();
    }

    public  static void test1(){
        String s = new String("This is a test string");
        StringTokenizer st = new StringTokenizer(s);
        System.out.println( "Token Total: " + st.countTokens() );
        while( st.hasMoreElements() ){
            System.out.println(st.nextToken());
        }
    }


    public  static  void test2(){
        String str = "100|66,55:200|567,90:102|43,54";

        StringTokenizer strToke = new StringTokenizer(str, ":,|");// 默认不打印分隔符
// StringTokenizer strToke=new StringTokenizer(str,":,|",true);//打印分隔符
// StringTokenizer strToke=new StringTokenizer(str,":,|",false);//不打印分隔符

        System.out.println( "Token2 Total: " + strToke.countTokens() );

        while(strToke.hasMoreTokens()){
            System.out.println(strToke.nextToken());
        }
    }



    public  static  void test3(){

    String s = "2,,3,4,,,5";

    String[] array1 = s.split(",");

    System.out.println(array1.length);

    String[] array2 = StringUtils.split(s,",");

        System.out.println(array2.length);



    }


}


