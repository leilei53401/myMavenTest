package tools;

public class CommonTools {
	
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

}
