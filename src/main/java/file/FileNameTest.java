package file;

import org.apache.commons.lang.StringUtils;

/**
 * Created by Administrator on 2018-1-22.
 */
public class FileNameTest {
    public static void main(String[] args){
         String str = "reglog_2018-01-01.20181111-90010101.out";
        String[] array = StringUtils.split(str,".");

        System.out.println(array.length);

        for(String s:array){
            System.out.println(s);

        }


    }
}
