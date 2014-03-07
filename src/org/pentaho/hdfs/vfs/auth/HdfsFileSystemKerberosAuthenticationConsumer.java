package org.pentaho.hdfs.vfs.auth;

import org.apache.commons.vfs.FileSystem;
import org.pentaho.di.core.auth.KerberosAuthenticationProvider;
import org.pentaho.di.core.auth.core.AuthenticationConsumer;
import org.pentaho.di.core.auth.core.AuthenticationConsumptionException;
import org.pentaho.hdfs.vfs.HDFSFileSystem;

public class HdfsFileSystemKerberosAuthenticationConsumer implements
    AuthenticationConsumer<FileSystem, KerberosAuthenticationProvider> {
  private HDFSFileSystemAuthenticationConsumerArg hDFSFileSystemAuthenticationConsumerArg;

  public HdfsFileSystemKerberosAuthenticationConsumer(
      HDFSFileSystemAuthenticationConsumerArg hDFSFileSystemAuthenticationConsumerArg ) {
    this.hDFSFileSystemAuthenticationConsumerArg = hDFSFileSystemAuthenticationConsumerArg;
  }

  @Override
  public FileSystem consume( KerberosAuthenticationProvider authenticationProvider )
    throws AuthenticationConsumptionException {
    // TODO: implement so we set kerb hadoop properties on the fso, possibly proxy and wrap all calls like mongo
    return new HDFSFileSystem( hDFSFileSystemAuthenticationConsumerArg.getFileName(),
        hDFSFileSystemAuthenticationConsumerArg.getFileSystemOptions() );
  }
}
