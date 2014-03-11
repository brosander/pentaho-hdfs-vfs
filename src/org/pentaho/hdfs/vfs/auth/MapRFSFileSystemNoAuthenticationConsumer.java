package org.pentaho.hdfs.vfs.auth;

import org.apache.commons.vfs.FileSystem;
import org.pentaho.di.core.auth.NoAuthenticationAuthenticationProvider;
import org.pentaho.di.core.auth.core.AuthenticationConsumer;
import org.pentaho.di.core.auth.core.AuthenticationConsumptionException;
import org.pentaho.hdfs.vfs.MapRFileSystem;

public class MapRFSFileSystemNoAuthenticationConsumer implements
    AuthenticationConsumer<FileSystem, NoAuthenticationAuthenticationProvider> {
  private HDFSFileSystemAuthenticationConsumerArg maprFSFileSystemAuthenticationConsumerArg;

  public MapRFSFileSystemNoAuthenticationConsumer(
      HDFSFileSystemAuthenticationConsumerArg hDFSFileSystemAuthenticationConsumerArg ) {
    this.maprFSFileSystemAuthenticationConsumerArg = hDFSFileSystemAuthenticationConsumerArg;
  }

  @Override
  public FileSystem consume( NoAuthenticationAuthenticationProvider authenticationProvider )
    throws AuthenticationConsumptionException {
    return new MapRFileSystem( maprFSFileSystemAuthenticationConsumerArg.getFileName(),
        maprFSFileSystemAuthenticationConsumerArg.getFileSystemOptions() );
  }
}
