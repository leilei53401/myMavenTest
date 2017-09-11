package macStat;

public class MacInfo {
	//-- 字段说明： 
	//userid | mac | oemid | cityid | provinceid | userip十进制 | 起播时间 | 停播时间 | 播放时长
    //232946|E0BC431D314D|135|2300|23|1897867240|1457444070|1457446682|2612|
	String userid;
	String mac;
	String oemid;
	String cityid;
	String provinceid;
	String userip;
	long exp=0L;
	public String getUserid() {
		return userid;
	}
	public void setUserid(String userid) {
		this.userid = userid;
	}
	public String getMac() {
		return mac;
	}
	public void setMac(String mac) {
		this.mac = mac;
	}
	public String getOemid() {
		return oemid;
	}
	public void setOemid(String oemid) {
		this.oemid = oemid;
	}
	public String getCityid() {
		return cityid;
	}
	public void setCityid(String cityid) {
		this.cityid = cityid;
	}
	public String getProvinceid() {
		return provinceid;
	}
	public void setProvinceid(String provinceid) {
		this.provinceid = provinceid;
	}
	public String getUserip() {
		return userip;
	}
	public void setUserip(String userip) {
		this.userip = userip;
	}
	public long getExp() {
		return exp;
	}
	public void setExp(long exp) {
		this.exp = exp;
	}
	
	@Override
	public String toString() {
		return "MacInfo [userid=" + userid + ", mac=" + mac + ", oemid="
				+ oemid + ", cityid=" + cityid + ", provinceid=" + provinceid
				+ ", userip=" + userip + ", exp=" + exp + "]";
	}
}
