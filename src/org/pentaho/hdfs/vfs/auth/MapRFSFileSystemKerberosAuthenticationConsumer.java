package org.pentaho.hdfs.vfs.auth;

import java.util.Arrays;
import java.util.HashSet;

import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.tools.FileObject;

import org.apache.commons.vfs.FileSystem;
import org.pentaho.di.core.auth.KerberosAuthenticationProvider;
import org.pentaho.di.core.auth.core.AuthenticationConsumer;
import org.pentaho.di.core.auth.core.AuthenticationConsumptionException;
import org.pentaho.hdfs.vfs.MapRFileSystem;
import org.pentaho.hdfs.vfs.auth.proxy.LoginContextInvocationHandler;

public class MapRFSFileSystemKerberosAuthenticationConsumer implements
    AuthenticationConsumer<FileSystem, KerberosAuthenticationProvider> {
  private HDFSFileSystemAuthenticationConsumerArg maprFSFileSystemAuthenticationConsumerArg;
  private KerberosUtil kerberosUtil;

  public MapRFSFileSystemKerberosAuthenticationConsumer(
      HDFSFileSystemAuthenticationConsumerArg maprFSFileSystemAuthenticationConsumerArg ) {
    this.maprFSFileSystemAuthenticationConsumerArg = maprFSFileSystemAuthenticationConsumerArg;
    kerberosUtil = new KerberosUtil();
  }

  @Override
  public FileSystem consume( KerberosAuthenticationProvider authenticationProvider )
    throws AuthenticationConsumptionException {
    final LoginContext loginContext;
    try {
      if ( authenticationProvider.isUseExternalCredentials() ) {
        if ( authenticationProvider.isUseKeytab() ) {
          loginContext =
              kerberosUtil.getLoginContextFromKeytab( authenticationProvider.getPrincipal(), authenticationProvider
                  .getKeytabLocation() );
        } else {
          loginContext = kerberosUtil.getLoginContextFromKerberosCache( authenticationProvider.getPrincipal() );
        }
      } else {
        loginContext =
            kerberosUtil.getLoginContextFromUsernamePassword( authenticationProvider.getPrincipal(),
                authenticationProvider.getPassword() );
      }
    } catch ( LoginException e ) {
      throw new AuthenticationConsumptionException( e );
    }
    
    return LoginContextInvocationHandler.forObject( new MapRFileSystem( maprFSFileSystemAuthenticationConsumerArg
        .getFileName(), maprFSFileSystemAuthenticationConsumerArg.getFileSystemOptions() ), loginContext,
        new HashSet<Class<?>>( Arrays.<Class<?>> asList( FileSystem.class, FileObject.class ) ) );
  }
}
