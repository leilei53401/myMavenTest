package adap;

/**
 * oemid类统计用，不要修改
 * 
 * @author jyc
 *
 */
public class AdapAreaInfo implements java.io.Serializable {

	private static final long serialVersionUID = -295229987303323988L;
	private int proid;
	private String proname;
	private int areaid;
	private long netseg1;
	private long netseg2;
	private String cityname;
	private int cityid;
	public int getProid() {
		return proid;
	}
	public void setProid(int proid) {
		this.proid = proid;
	}
	public String getProname() {
		return proname;
	}
	public void setProname(String proname) {
		this.proname = proname;
	}
	public int getAreaid() {
		return areaid;
	}
	public void setAreaid(int areaid) {
		this.areaid = areaid;
	}
	public long getNetseg1() {
		return netseg1;
	}
	public void setNetseg1(long netseg1) {
		this.netseg1 = netseg1;
	}
	public long getNetseg2() {
		return netseg2;
	}
	public void setNetseg2(long netseg2) {
		this.netseg2 = netseg2;
	}
	public String getCityname() {
		return cityname;
	}
	public void setCityname(String cityname) {
		this.cityname = cityname;
	}
	public int getCityid() {
		return cityid;
	}
	public void setCityid(int cityid) {
		this.cityid = cityid;
	}
	@Override
	public String toString() {
		return "AdapAreaInfo [proid=" + proid + ", proname=" + proname
				+ ", areaid=" + areaid + ", netseg1=" + netseg1 + ", netseg2="
				+ netseg2 + ", cityname=" + cityname + ", cityid=" + cityid
				+ "]";
	}

}
