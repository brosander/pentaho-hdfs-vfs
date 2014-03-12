package org.pentaho.hdfs.vfs.auth;

import java.io.IOException;

import org.pentaho.di.core.auth.AuthenticationConsumerPlugin;
import org.pentaho.di.core.auth.AuthenticationConsumerType;
import org.pentaho.di.core.auth.NoAuthenticationAuthenticationProvider;
import org.pentaho.di.core.auth.core.AuthenticationConsumer;
import org.pentaho.di.core.auth.core.AuthenticationConsumptionException;
import org.pentaho.hdfs.vfs.wrapper.HadoopFileSystem;
import org.pentaho.hdfs.vfs.wrapper.HadoopFileSystemImpl;

public class MapRFSFileSystemNoAuthenticationConsumer implements
    AuthenticationConsumer<HadoopFileSystem, NoAuthenticationAuthenticationProvider> {
  @AuthenticationConsumerPlugin( id = "MapRFSFileSystemNoAuthenticationConsumer",
      name = "MapRFSFileSystemNoAuthenticationConsumer" )
  public static class MapRFSFileSystemNoAuthenticationConsumerType implements AuthenticationConsumerType {

    @Override
    public String getDisplayName() {
      return "MapRFSFileSystemNoAuthenticationConsumer";
    }

    @Override
    public Class<? extends AuthenticationConsumer<?, ?>> getConsumerClass() {
      return MapRFSFileSystemNoAuthenticationConsumer.class;
    }
  }
  private MapRFSFileSystemAuthenticationConsumerArg maprFSFileSystemAuthenticationConsumerArg;

  public MapRFSFileSystemNoAuthenticationConsumer(
      MapRFSFileSystemAuthenticationConsumerArg MapRFSFileSystemAuthenticationConsumerArg ) {
    this.maprFSFileSystemAuthenticationConsumerArg = MapRFSFileSystemAuthenticationConsumerArg;
  }

  @Override
  public HadoopFileSystem consume( NoAuthenticationAuthenticationProvider authenticationProvider )
    throws AuthenticationConsumptionException {
    try {
      return new HadoopFileSystemImpl( org.apache.hadoop.fs.FileSystem.get( maprFSFileSystemAuthenticationConsumerArg
          .getConf() ) );
    } catch ( IOException e ) {
      throw new AuthenticationConsumptionException( e );
    }
  }
}
