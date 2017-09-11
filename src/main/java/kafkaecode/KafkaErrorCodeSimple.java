package kafkaecode;

import java.io.Serializable;

public class KafkaErrorCodeSimple implements Serializable {
	
	private static final long serialVersionUID = -5435526040445723425L;
	
	private String client;
	private String sendtime;
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
	private String activatetime;
	public String getActivatetime() {
		return activatetime;
	}
	public String getClient() {
		return client;
	}
	public String getErrorcode() {
		return errorcode;
	}
	public String getErrorlevel() {
		return errorlevel;
	}
	public String getErrorsources() {
		return errorsources;
	}
	public String getErrorstack() {
		return errorstack;
	}
	public String getMessage() {
		return message;
	}
	public String getOeminfo() {
		return oeminfo;
	}
	public String getOemtype() {
		return oemtype;
	}
	public String getOlderrorcode() {
		return olderrorcode;
	}
	public String getRemark() {
		return remark;
	}
	public String getScene() {
		return scene;
	}
	public String getSendtime() {
		return sendtime;
	}
	public String getSessionid() {
		return sessionid;
	}
	public String getSolution() {
		return solution;
	}
	public String getStatus() {
		return status;
	}
	public String getSubsys() {
		return subsys;
	}
	public String getSubsysname() {
		return subsysname;
	}
	public void setActivatetime(String activatetime) {
		this.activatetime = activatetime;
	}
	public void setClient(String client) {
		this.client = client;
	}
	public void setErrorcode(String errorcode) {
		this.errorcode = errorcode;
	}
	public void setErrorlevel(String errorlevel) {
		this.errorlevel = errorlevel;
	}
	public void setErrorsources(String errorsources) {
		this.errorsources = errorsources;
	}
	public void setErrorstack(String errorstack) {
		this.errorstack = errorstack;
	}
	public void setMessage(String message) {
		this.message = message;
	}
	public void setOeminfo(String oeminfo) {
		this.oeminfo = oeminfo;
	}
	public void setOemtype(String oemtype) {
		this.oemtype = oemtype;
	}
	public void setOlderrorcode(String olderrorcode) {
		this.olderrorcode = olderrorcode;
	}
	public void setRemark(String remark) {
		this.remark = remark;
	}
	public void setScene(String scene) {
		this.scene = scene;
	}
	public void setSendtime(String sendtime) {
		this.sendtime = sendtime;
	}
	public void setSessionid(String sessionid) {
		this.sessionid = sessionid;
	}
	public void setSolution(String solution) {
		this.solution = solution;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public void setSubsys(String subsys) {
		this.subsys = subsys;
	}
	public void setSubsysname(String subsysname) {
		this.subsysname = subsysname;
	}
	@Override
	public String toString() {
		return "KafkaErrorCodeSimple [client=" + client + ", sendtime=" + sendtime + ", oeminfo=" + oeminfo
				+ ", oemtype=" + oemtype + ", errorcode=" + errorcode + ", subsys=" + subsys + ", subsysname="
				+ subsysname + ", message=" + message + ", scene=" + scene + ", solution=" + solution
				+ ", olderrorcode=" + olderrorcode + ", errorsources=" + errorsources + ", remark=" + remark
				+ ", status=" + status + ", sessionid=" + sessionid + ", errorlevel=" + errorlevel + ", errorstack="
				+ errorstack + ", activatetime=" + activatetime + "]";
	}
	
}
