package ch.generali.copa.di.connector.fssourceconnector.file.reader.sftp;

import ch.generali.copa.di.connector.fssourceconnector.file.reader.FileReaderTestBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.net.URI;

import com.github.stefanbirkner.fakesftpserver.rule.FakeSftpServerRule;
import org.junit.ClassRule;

public abstract class SftpFileReaderTestBase extends FileReaderTestBase {


    @ClassRule
    static public final FakeSftpServerRule sftpServer = new FakeSftpServerRule().setPort(1234);

    @BeforeClass
    public static void initFs() throws IOException {
        fsUri = URI.create("sftp://user:pass@localhost:" + sftpServer.getPort() + "/");
        Configuration config = new Configuration();
        config.set("fs.sftp.impl","org.apache.hadoop.fs.sftp.SFTPFileSystem");
        fs = FileSystem.newInstance(fsUri, config);
    }

    @AfterClass
    public static void finishFs() throws Exception {

    }
}
