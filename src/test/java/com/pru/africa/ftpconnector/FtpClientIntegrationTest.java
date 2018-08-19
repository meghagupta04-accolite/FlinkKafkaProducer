//package com.pru.africa.ftpconnector;
//import java.io.IOException;
//import java.util.Collection;
//
//import org.junit.After;
//import org.junit.Before;
//import org.junit.jupiter.api.Test;
//import org.mockftpserver.fake.FakeFtpServer;
//import org.mockftpserver.fake.UserAccount;
//import org.mockftpserver.fake.filesystem.DirectoryEntry;
//import org.mockftpserver.fake.filesystem.FileEntry;
//import org.mockftpserver.fake.filesystem.FileSystem;
//import org.mockftpserver.fake.filesystem.UnixFakeFileSystem;
//
//
//public class FtpClientIntegrationTest {
//	 
//    private FakeFtpServer fakeFtpServer;
// 
//    private FtpClient ftpClient;
// 
//    @Before
//    public void setup() throws IOException {
//        fakeFtpServer = new FakeFtpServer();
//        fakeFtpServer.addUserAccount(new UserAccount("user", "password", "/data"));
// 
//        FileSystem fileSystem = new UnixFakeFileSystem();
//        fileSystem.add(new DirectoryEntry("/data"));
//        fileSystem.add(new FileEntry("/data/foobar.txt", "abcdef 1234567890"));
//        fakeFtpServer.setFileSystem(fileSystem);
//        fakeFtpServer.setServerControlPort(0);
// 
//        fakeFtpServer.start();
// 
//        ftpClient = new FtpClient("localhost", fakeFtpServer.getServerControlPort(), "user", "password");
//        ftpClient.open();
//    }
// 
//    @After
//    public void teardown() throws IOException {
//        ftpClient.close();
//        fakeFtpServer.stop();
//    }
//    
//    @Test
//    public void givenRemoteFile_whenListingRemoteFiles_thenItIsContainedInList() throws IOException {
//        Collection<String> files = ftpClient.listFiles("");
//        
//     //   assertThat(files).contains("foobar.txt");
//    }
//}
