package org.pentaho.hdfs.vfs.auth;

import org.apache.commons.vfs.FileSystem;
import org.pentaho.di.core.auth.NoAuthenticationAuthenticationProvider;
import org.pentaho.di.core.auth.core.AuthenticationConsumer;
import org.pentaho.di.core.auth.core.AuthenticationConsumptionException;
import org.pentaho.hdfs.vfs.HDFSFileSystem;

public class HdfsFileSystemNoAuthenticationConsumer implements
    AuthenticationConsumer<FileSystem, NoAuthenticationAuthenticationProvider> {
  private HDFSFileSystemAuthenticationConsumerArg hDFSFileSystemAuthenticationConsumerArg;

  public HdfsFileSystemNoAuthenticationConsumer(
      HDFSFileSystemAuthenticationConsumerArg hDFSFileSystemAuthenticationConsumerArg ) {
    this.hDFSFileSystemAuthenticationConsumerArg = hDFSFileSystemAuthenticationConsumerArg;
  }

  @Override
  public FileSystem consume( NoAuthenticationAuthenticationProvider authenticationProvider )
    throws AuthenticationConsumptionException {
    return new HDFSFileSystem( hDFSFileSystemAuthenticationConsumerArg.getFileName(),
        hDFSFileSystemAuthenticationConsumerArg.getFileSystemOptions() );
  }
}
