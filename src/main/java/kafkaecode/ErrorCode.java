package kafkaecode;


import java.io.Serializable;

public class ErrorCode implements Serializable {
	private String oeminfo;
	private String oemtype;
	private String errorcode;
	private String subsys;
	private String subsysname;
	private String message;
	private String scene;
	private String solution;
	private String olderrorcode;
	private String errorsources;
	private String remark;
	private String status;
	private String sessionid;
	private String errorlevel;
	private String errorstack;
	public String getOeminfo() {
		return oeminfo;
	}
	public void setOeminfo(String oeminfo) {
		this.oeminfo = oeminfo;
	}
	public String getOemtype() {
		return oemtype;
	}
	public void setOemtype(String oemtype) {
		this.oemtype = oemtype;
	}
	public String getErrorcode() {
		return errorcode;
	}
	public void setErrorcode(String errorcode) {
		this.errorcode = errorcode;
	}
	public String getSubsys() {
		return subsys;
	}
	public void setSubsys(String subsys) {
		this.subsys = subsys;
	}
	public String getSubsysname() {
		return subsysname;
	}
	public void setSubsysname(String subsysname) {
		this.subsysname = subsysname;
	}
	public String getMessage() {
		return message;
	}
	public void setMessage(String message) {
		this.message = message;
	}
	public String getScene() {
		return scene;
	}
	public void setScene(String scene) {
		this.scene = scene;
	}
	public String getSolution() {
		return solution;
	}
	public void setSolution(String solution) {
		this.solution = solution;
	}
	public String getOlderrorcode() {
		return olderrorcode;
	}
	public void setOlderrorcode(String olderrorcode) {
		this.olderrorcode = olderrorcode;
	}
	public String getErrorsources() {
		return errorsources;
	}
	public void setErrorsources(String errorsources) {
		this.errorsources = errorsources;
	}
	public String getRemark() {
		return remark;
	}
	public void setRemark(String remark) {
		this.remark = remark;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public String getSessionid() {
		return sessionid;
	}
	public void setSessionid(String sessionid) {
		this.sessionid = sessionid;
	}
	public String getErrorlevel() {
		return errorlevel;
	}
	public void setErrorlevel(String errorlevel) {
		this.errorlevel = errorlevel;
	}
	public String getErrorstack() {
		return errorstack;
	}
	public void setErrorstack(String errorstack) {
		this.errorstack = errorstack;
	}
	public String getActivatetime() {
		return activatetime;
	}
	public void setActivatetime(String activatetime) {
		this.activatetime = activatetime;
	}
	private String activatetime;
}
