package adstat;

import java.util.Date;


public class Statistic extends ValueObject implements java.io.Serializable {
	//广告排期用
	private String startdate;
	private String enddate;
	private String oemid;
	private String amid;
	private String adname;
	private String hid;
	private String provinceid;
	private String provincename;
	private String cityid;
	private String cityname;
	private String channelid;

	private String adposid;

	private String uv;

	private String reportid;


	private int sumcount;
	private int playcnt;
	private String date;
	private String adlocationcode;
	private String frequency;
	private String startdateexcel;
	private String enddateexcel;
	private String adlocationcodeexcel;
	private String oemidexcel;
	private String amidexcel;
	private String pgloadoemid;
	private String pgloadamid;
	private String logdate;
	private String mindate;
	private String maxdate;
	private String param;
	private Date occupydate;
	private String touchone;
	private String touchtwo;
	private String touchthree;
	private String touchfour;
	private String touchfive;
	private String touchsix;
	private String touchseven;
	private String toucheight;
	private String touchnine;
	private String touchten;
	private int exposure;
	private String timequantum;


	//实时库存+广告位计算
	private String querystarttime;


	private String queryendtime;
	private String cityidlist;
	private String cityidflag;//0:不限制 1：指定
	private String channelidlist;
	private String channelidflag;//0:不限制 1：指定
	
	private String oemidlist;
	private String adposidlist;
	private String oemidflag;//0:不限制 1：指定
	private String adtypeflag; //0:贴片广告 1：图片广告
	public String getAdlocationcode() {
		return adlocationcode;
	}
	
	public String getAdlocationcodeexcel() {
		return adlocationcodeexcel;
	}
	public String getAdname() {
		return adname;
	}


	public String getAdposid() {
		return adposid;
	}
	public String getAdposidlist() {
		return adposidlist;
	}
	
	public String getAdtypeflag() {
		return adtypeflag;
	}
	public String getAmid() {
		return amid;
	}
	public String getAmidexcel() {
		return amidexcel;
	}
	public String getChannelid() {
		return channelid;
	}
	public String getChannelidflag() {
		return channelidflag;
	}
	public String getChannelidlist() {
		return channelidlist;
	}
	public String getCityid() {
		return cityid;
	}
	public String getCityidflag() {
		return cityidflag;
	}
	public String getCityidlist() {
		return cityidlist;
	}
	public String getCityname() {
		return cityname;
	}
	public String getDate() {
		return date;
	}
	public String getEnddate() {
		return enddate;
	}
	public String getEnddateexcel() {
		return enddateexcel;
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
	public String getLogdate() {
		return logdate;
	}
	public String getMaxdate() {
		return maxdate;
	}
	public String getMindate() {
		return mindate;
	}
	public Date getOccupydate() {
		return occupydate;
	}
	public String getOemid() {
		return oemid;
	}
	public String getOemidexcel() {
		return oemidexcel;
	}
	public String getOemidflag() {
		return oemidflag;
	}
	public String getOemidlist() {
		return oemidlist;
	}
	public String getParam() {
		return param;
	}
	public String getPgloadamid() {
		return pgloadamid;
	}
	public String getPgloadoemid() {
		return pgloadoemid;
	}
	public int getPlaycnt() {
		return playcnt;
	}


	public String getProvinceid() {
		return provinceid;
	}
	public String getProvincename() {
		return provincename;
	}
	public String getQueryendtime() {
		return queryendtime;
	}
	public String getQuerystarttime() {
		return querystarttime;
	}
	public String getReportid() {
		return reportid;
	}
	public String getStartdate() {
		return startdate;
	}
	public String getStartdateexcel() {
		return startdateexcel;
	}
	public int getSumcount() {
		return sumcount;
	}
	public String getTimequantum() {
		return timequantum;
	}
	public String getToucheight() {
		return toucheight;
	}
	public String getTouchfive() {
		return touchfive;
	}

	public String getTouchfour() {
		return touchfour;
	}
	public String getTouchnine() {
		return touchnine;
	}

	public String getTouchone() {
		return touchone;
	}
	public String getTouchseven() {
		return touchseven;
	}
	public String getTouchsix() {
		return touchsix;
	}
	public String getTouchten() {
		return touchten;
	}
	
	public String getTouchthree() {
		return touchthree;
	}
	public String getTouchtwo() {
		return touchtwo;
	}
	
	public String getUv() {
		return uv;
	}
	public void setAdlocationcode(String adlocationcode) {
		this.adlocationcode = adlocationcode;
	}
	public void setAdlocationcodeexcel(String adlocationcodeexcel) {
		this.adlocationcodeexcel = adlocationcodeexcel;
	}
	public void setAdname(String adname) {
		this.adname = adname;
	}
	public void setAdposid(String adposid) {
		this.adposid = adposid;
	}
	public void setAdposidlist(String adposidlist) {
		this.adposidlist = adposidlist;
	}
	public void setAdtypeflag(String adtypeflag) {
		this.adtypeflag = adtypeflag;
	}
	public void setAmid(String amid) {
		this.amid = amid;
	}
	public void setAmidexcel(String amidexcel) {
		this.amidexcel = amidexcel;
	}
	public void setChannelid(String channelid) {
		this.channelid = channelid;
	}
	public void setChannelidflag(String channelidflag) {
		this.channelidflag = channelidflag;
	}
	public void setChannelidlist(String channelidlist) {
		this.channelidlist = channelidlist;
	}
	public void setCityid(String cityid) {
		this.cityid = cityid;
	}
	public void setCityidflag(String cityidflag) {
		this.cityidflag = cityidflag;
	}
	public void setCityidlist(String cityidlist) {
		this.cityidlist = cityidlist;
	}
	public void setCityname(String cityname) {
		this.cityname = cityname;
	}
	public void setDate(String date) {
		this.date = date;
	}
	public void setEnddate(String enddate) {
		this.enddate = enddate;
	}
	public void setEnddateexcel(String enddateexcel) {
		this.enddateexcel = enddateexcel;
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
	public void setLogdate(String logdate) {
		this.logdate = logdate;
	}
	public void setMaxdate(String maxdate) {
		this.maxdate = maxdate;
	}
	public void setMindate(String mindate) {
		this.mindate = mindate;
	}
	public void setOccupydate(Date occupydate) {
		this.occupydate = occupydate;
	}
	public void setOemid(String oemid) {
		this.oemid = oemid;
	}
	public void setOemidexcel(String oemidexcel) {
		this.oemidexcel = oemidexcel;
	}
	public void setOemidflag(String oemidflag) {
		this.oemidflag = oemidflag;
	}
	public void setOemidlist(String oemidlist) {
		this.oemidlist = oemidlist;
	}
	public void setParam(String param) {
		this.param = param;
	}
	public void setPgloadamid(String pgloadamid) {
		this.pgloadamid = pgloadamid;
	}
	public void setPgloadoemid(String pgloadoemid) {
		this.pgloadoemid = pgloadoemid;
	}
	public void setPlaycnt(int playcnt) {
		this.playcnt = playcnt;
	}
	
	
	public void setProvinceid(String provinceid) {
		this.provinceid = provinceid;
	}
	public void setProvincename(String provincename) {
		this.provincename = provincename;
	}
	public void setQueryendtime(String queryendtime) {
		this.queryendtime = queryendtime;
	}
	public void setQuerystarttime(String querystarttime) {
		this.querystarttime = querystarttime;
	}
	public void setReportid(String reportid) {
		this.reportid = reportid;
	}
	public void setStartdate(String startdate) {
		this.startdate = startdate;
	}
	public void setStartdateexcel(String startdateexcel) {
		this.startdateexcel = startdateexcel;
	}
	public void setSumcount(int sumcount) {
		this.sumcount = sumcount;
	}
	public void setTimequantum(String timequantum) {
		this.timequantum = timequantum;
	}
	public void setToucheight(String toucheight) {
		this.toucheight = toucheight;
	}
	public void setTouchfive(String touchfive) {
		this.touchfive = touchfive;
	}
	public void setTouchfour(String touchfour) {
		this.touchfour = touchfour;
	}
	public void setTouchnine(String touchnine) {
		this.touchnine = touchnine;
	}
	public void setTouchone(String touchone) {
		this.touchone = touchone;
	}
	public void setTouchseven(String touchseven) {
		this.touchseven = touchseven;
	}
	public void setTouchsix(String touchsix) {
		this.touchsix = touchsix;
	}
	public void setTouchten(String touchten) {
		this.touchten = touchten;
	}

	public void setTouchthree(String touchthree) {
		this.touchthree = touchthree;
	}
	public void setTouchtwo(String touchtwo) {
		this.touchtwo = touchtwo;
	}
	

	public void setUv(String uv) {
		this.uv = uv;
	}

}
