package com.github.mmolimar.kafka.connect.fs.policy.sftp;

import com.github.mmolimar.kafka.connect.fs.policy.PolicyTestBase;
import com.github.stefanbirkner.fakesftpserver.rule.FakeSftpServerRule;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;

public abstract class SftpPolicyTestBase extends PolicyTestBase {

    @ClassRule
    static public final FakeSftpServerRule sftpServer = new FakeSftpServerRule().setPort(1234);

    @BeforeClass
    public static void initFs() throws IOException {
        fsUri = URI.create("sftp://ueserli:paessli@localhost:" + sftpServer.getPort() + "/");
        Configuration config = new Configuration();
        config.set("fs.sftp.impl","com.github.mmolimar.kafka.connect.fs.filesystem.SFTPFileSystemFixed");
        fs = FileSystem.newInstance(fsUri, config);
    }

    @AfterClass
    public static void finishFs() throws Exception {

    }
}
