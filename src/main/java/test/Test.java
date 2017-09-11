package test;
import org.apache.commons.lang.StringUtils;

public class Test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		String s  = "apptype=1&ispid=20120629&epgid=100871&adclass=7&oemid=368&mid=58154887&hid=0090e62a7ce90000000000000000000000000000&fid=7f12b896645b45a35ce761381e82db6d&uid=105391453&mtype=12345&catcode=8&areacode=0&width=1280&height=720&fps=25&ip=106.115.105.60&version=1.0&usercache=day:20160125;dlmd:55189995;dpln:26061;dplc:3;wday:20160120;wpln:26061;wplc:3;mday:20160113;mpln:26061;mplc:3&suptype=adclass:7;adpos:701,704,706,707";
		String s2 = StringUtils.substringAfter(s, "mtype=");
		System.out.println(s2);
		String s3 = StringUtils.substringBefore(s2, "&");
		System.out.println(s3);
		String s4 = StringUtils.substringBefore(StringUtils.substringAfter(s, "mtype="), "&");
        
        
		System.out.println(s4);
        
        System.out.println("");
        System.out.println("");
        System.out.println("");
        System.out.println("");
	}

}
