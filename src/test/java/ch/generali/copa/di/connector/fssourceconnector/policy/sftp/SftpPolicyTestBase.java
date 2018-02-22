package ch.generali.copa.di.connector.fssourceconnector.policy.sftp;

import ch.generali.copa.di.connector.fssourceconnector.policy.PolicyTestBase;
import com.github.stefanbirkner.fakesftpserver.rule.FakeSftpServerRule;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.io.IOException;
import java.net.URI;

public abstract class SftpPolicyTestBase extends PolicyTestBase {

    @ClassRule
    static public final FakeSftpServerRule sftpServer = new FakeSftpServerRule().setPort(1234);

    @BeforeClass
    public static void initFs() throws IOException {
        fsUri = URI.create("sftp://ueserli:paessli@localhost:" + sftpServer.getPort() + "/");
        Configuration config = new Configuration();
        config.set("fs.sftp.impl","ch.generali.copa.di.connector.fssourceconnector.filesystem.SFTPFileSystemFixed");
        fs = FileSystem.newInstance(fsUri, config);
    }

    @AfterClass
    public static void finishFs() throws Exception {

    }
}
