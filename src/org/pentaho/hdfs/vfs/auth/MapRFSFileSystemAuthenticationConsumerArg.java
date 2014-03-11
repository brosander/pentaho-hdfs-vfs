package org.pentaho.hdfs.vfs.auth;

import org.apache.commons.vfs.FileName;
import org.apache.commons.vfs.FileSystemOptions;

public class MapRFSFileSystemAuthenticationConsumerArg {
  private final FileName fileName;
  private final FileSystemOptions fileSystemOptions;

  public FileName getFileName() {
    return fileName;
  }

  public FileSystemOptions getFileSystemOptions() {
    return fileSystemOptions;
  }

  public MapRFSFileSystemAuthenticationConsumerArg( FileName fileName, FileSystemOptions fileSystemOptions ) {
    this.fileName = fileName;
    this.fileSystemOptions = fileSystemOptions;
  }

}
