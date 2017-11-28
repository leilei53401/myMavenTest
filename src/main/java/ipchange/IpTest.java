package ipchange;

import org.apache.commons.lang3.StringUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Administrator on 2017-11-16.
 */
public class IpTest {

    public static void main(String[] args){
        String s = "192.168.1.1";
         if(!validIp(s) || StringUtils.startsWith(s,"192.168.")){
             System.out.println("1111");
         }else{
             System.out.println("22222");
         }
    }


    //简单验证IP
    public static boolean validIp(String ip){
        //((?:(?:25[0-5]|2[0-4]\d|((1\d{2})|([1-9]?\d)))\.){3}(?:25[0-5]|2[0-4]\d|((1\d{2})|([1-9]?\d))))
        //简单验证即可
        Pattern p = Pattern.compile("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}");
        Matcher m = p.matcher(ip);
        return m.find();
    }
}
