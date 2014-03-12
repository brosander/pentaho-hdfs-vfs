package org.pentaho.hdfs.vfs.auth;

import org.apache.commons.vfs.FileSystemOptions;
import org.apache.hadoop.conf.Configuration;

public class MapRFSFileSystemAuthenticationConsumerArg {
  private final Configuration conf;
  private final FileSystemOptions fileSystemOptions;

  public Configuration getConf() {
    return conf;
  }

  public FileSystemOptions getFileSystemOptions() {
    return fileSystemOptions;
  }

  public MapRFSFileSystemAuthenticationConsumerArg( Configuration conf, FileSystemOptions fileSystemOptions ) {
    this.conf = conf;
    this.fileSystemOptions = fileSystemOptions;
  }

}
