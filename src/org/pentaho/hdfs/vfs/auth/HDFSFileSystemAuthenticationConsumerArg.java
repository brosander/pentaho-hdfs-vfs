package org.pentaho.hdfs.vfs.auth;

import org.apache.commons.vfs.FileName;
import org.apache.commons.vfs.FileSystemOptions;

public class HDFSFileSystemAuthenticationConsumerArg {
  private final FileName fileName;
  private final FileSystemOptions fileSystemOptions;

  public FileName getFileName() {
    return fileName;
  }

  public FileSystemOptions getFileSystemOptions() {
    return fileSystemOptions;
  }

  public HDFSFileSystemAuthenticationConsumerArg( FileName fileName, FileSystemOptions fileSystemOptions ) {
    this.fileName = fileName;
    this.fileSystemOptions = fileSystemOptions;
  }

}
