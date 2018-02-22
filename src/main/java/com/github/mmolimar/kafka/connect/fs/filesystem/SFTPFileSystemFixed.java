package com.github.mmolimar.kafka.connect.fs.filesystem;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.sftp.SFTPFileSystem;

public class SFTPFileSystemFixed extends SFTPFileSystem {

    /**
     * Fixes upstream version, which for some returns this.getHomeDirectory();
     * This version returns the directory path extracted from the URI
     * @return
     */
    @Override
    public Path getWorkingDirectory() {
        return new Path(this.getUri().getPath());
    }
}
