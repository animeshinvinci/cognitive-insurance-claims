package com.ibm.bpm;

import java.net.Authenticator;
import java.net.PasswordAuthentication;

public class MyAuthenticator extends Authenticator {
	String username = "admin";
	String password = "passw0rd";

	@Override
	public PasswordAuthentication getPasswordAuthentication() {
		System.err.println("Feeding username and password for " + getRequestingScheme() + " authentication");
		return (new PasswordAuthentication(username, password.toCharArray()));
	}
}
