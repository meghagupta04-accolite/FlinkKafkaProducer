package com.pru.africa.ftpconnector;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.net.PrintCommandListener;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;

public class FtpClient {

	private String server;
	private int port;
	private String user;
	private String password;
	private FTPClient ftp;

	public static void main(String[] args) {
		FtpClient ftpClient = new FtpClient("test.rebex.net", 21, "demo", "password");
		try {
			ftpClient.open();
			ftpClient.listFiles("pub/example");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public FtpClient(String server, int port, String user, String password) {
		super();
		this.server = server;
		this.port = port;
		this.user = user;
		this.password = password;
	}
	public void open() throws IOException {
		ftp = new FTPClient();

		ftp.addProtocolCommandListener(new PrintCommandListener(new PrintWriter(System.out)));

		ftp.connect(server, port);
		int reply = ftp.getReplyCode();
		if (!FTPReply.isPositiveCompletion(reply)) {
			ftp.disconnect();
			throw new IOException("Exception in connecting to FTP Server");
		}

		ftp.login(user, password);
		System.out.println("Connected to ftp");
	}

	public void close() throws IOException {
		ftp.disconnect();
	}

	public Collection<String> listFiles(String path) throws IOException {
		FTPFile[] files = ftp.listFiles(path);
		ArrayList<String> filesList = new ArrayList<String>();
		for (FTPFile file : files) {
			filesList.add(file.getName());
			System.out.println(file.getName());
		}
		return filesList;
	}
}

