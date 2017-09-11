package adstat;

/**
 * oemid类统计用，不要修改
 * 
 * @author jyc
 *
 */
public class AdposidStatGrid implements java.io.Serializable {

	private static final long serialVersionUID = -295229987303323988L;
	private String adposid;
	private String hid;
	private String frequency;
	private String touchone;

	private int exposure;

	public AdposidStatGrid() {

	}

	public AdposidStatGrid(String adposid, String frequency, String touchone, int exposure) {
		super();
		this.adposid = adposid;
		this.frequency = frequency;
		this.touchone = touchone;
		this.exposure = exposure;
	}

	public String getAdposid() {
		return adposid;
	}

	public int getExposure() {
		return exposure;
	}

	public String getFrequency() {
		return frequency;
	}

	public String getHid() {
		return hid;
	}

	public String getTouchone() {
		return touchone;
	}

	public void setAdposid(String adposid) {
		this.adposid = adposid;
	}

	public void setExposure(int exposure) {
		this.exposure = exposure;
	}

	public void setFrequency(String frequency) {
		this.frequency = frequency;
	}

	public void setHid(String hid) {
		this.hid = hid;
	}

	public void setTouchone(String touchone) {
		this.touchone = touchone;
	}

	@Override
	public String toString() {
		return "{\"adposid\":\"" + adposid + "\", \"exposure\":\"" + exposure + "\", \"touchone\":\"" + touchone + "\", \"frequency\":\"" + frequency + "\"}";
	}

}
