package fr.telecom_paristech.msbgd2017.systemes_distribues.vignes_roville.master.util;

public class ProcessResponse {
	
	private String stdResponse;
	private String errResponse;
	
	public ProcessResponse(String stdResponse, String errResponse) {
		this.stdResponse = stdResponse;
		this.errResponse = errResponse;
	}
	
	public boolean hasError() {
		return errResponse.length() > 0;
	}
	
	public String getStdResponse() {
		return stdResponse;
	}
	
	public String getErrResponse() {
		return errResponse;
	}
}
