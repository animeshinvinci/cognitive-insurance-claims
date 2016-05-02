package com.ibm.bpm;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;

public class GenerateHostnameVerifyer implements HostnameVerifier{

	@Override
	public boolean verify(String arg0, SSLSession arg1) {
		// TODO Auto-generated method stub
		return true;
	}

}
