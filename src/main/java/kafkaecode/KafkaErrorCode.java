package kafkaecode;

import java.io.Serializable;

public class KafkaErrorCode implements Serializable {
	
	private static final long serialVersionUID = -8383960320442070874L;
	private String client;
	private String sendtime;
	private String datareportings;
	public String getDatareportings() {
		return datareportings;
	}
	public void setDatareportings(String datareportings) {
		this.datareportings = datareportings;
	}
	public String getClient() {
		return client;
	}
	public void setClient(String client) {
		this.client = client;
	}
	public String getSendtime() {
		return sendtime;
	}
	public void setSendtime(String sendtime) {
		this.sendtime = sendtime;
	}
}
