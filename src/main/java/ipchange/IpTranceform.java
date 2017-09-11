package ipchange;

public class IpTranceform {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
//			System.out.println(ipToDecimal("193.3.0.10"));
//		System.out.println(ipToDecimal("10.0.3.193"));
			System.out.print(longToIP(167773121)); 
		
	}
	
	
	/**
	 * ip转换成十进制的数 ip格式：*.*.*.*
	 * 
	 * @param ip
	 * @return
	 */
	public static long ipToDecimal(String ip) {
		long ipDec = 0;
		if (ip != null) {
			String[] ipArr = ip.split("\\.");
			if (ipArr != null && ipArr.length == 4) {
				for (int i = 3; i >= 0; i--) {
					ipDec += (Long.valueOf(ipArr[i].trim()) * Math.pow(256, 3 - i));
				}
			}
		}
		return ipDec;
	}
	
/*	public static String int2ip(long ipInt){ 
		StringBuilder sb=new StringBuilder(); 
			sb.append(ipInt&0xFF).append("."); 
			sb.append((ipInt>>8)&0xFF).append("."); 
			sb.append((ipInt>>16)&0xFF).append("."); 
			sb.append((ipInt>>24)&0xFF); 
			return sb.toString(); 
	} */
	
	public static String int2ip(long ipInt){ 
		StringBuilder sb=new StringBuilder();
		sb.append((ipInt>>24)&0xFF).append("."); ; 
		sb.append((ipInt>>16)&0xFF).append(".");
		sb.append((ipInt>>8)&0xFF).append("."); 
		sb.append(ipInt&0xFF);
		return sb.toString(); 
	} 
	
	
	// 将十进制整数形式转换成127.0.0.1形式的ip地址  
	 public static String longToIP(long longIp) {  
	        StringBuffer sb = new StringBuffer("");  
	        // 直接右移24位  
	        sb.append(String.valueOf((longIp >>> 24)));  
	        sb.append(".");  
	        // 将高8位置0，然后右移16位  
	        sb.append(String.valueOf((longIp & 0x00FFFFFF) >>> 16));  
	        sb.append(".");  
	        // 将高16位置0，然后右移8位  
	        sb.append(String.valueOf((longIp & 0x0000FFFF) >>> 8));  
	        sb.append(".");  
	        // 将高24位置0  
	        sb.append(String.valueOf((longIp & 0x000000FF)));  
	        return sb.toString();  
	    }  


}
