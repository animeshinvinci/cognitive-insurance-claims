package com.ibm.bpm;

import java.io.IOException;
import java.net.Proxy;

import com.squareup.okhttp.Authenticator;
import com.squareup.okhttp.Credentials;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;

public class Authenticate implements Authenticator {
	
	@Override
	public Request authenticate(Proxy proxy, Response response) throws IOException {
		String credential = Credentials.basic("admin", "passw0rd");
		System.out.print("BU creds: " + credential);
		return response.request().newBuilder().header("Authorization", credential).build();
	}

	@Override
	public Request authenticateProxy(Proxy arg0, Response arg1) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}
	
	

}
