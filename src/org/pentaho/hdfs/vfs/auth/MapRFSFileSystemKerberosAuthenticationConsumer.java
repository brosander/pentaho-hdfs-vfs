package org.pentaho.hdfs.vfs.auth;

import java.io.IOException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.HashSet;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.pentaho.di.core.auth.AuthenticationConsumerPlugin;
import org.pentaho.di.core.auth.AuthenticationConsumerType;
import org.pentaho.di.core.auth.KerberosAuthenticationProvider;
import org.pentaho.di.core.auth.core.AuthenticationConsumer;
import org.pentaho.di.core.auth.core.AuthenticationConsumptionException;
import org.pentaho.di.core.auth.kerberos.KerberosUtil;
import org.pentaho.di.core.auth.kerberos.LoginContextInvocationHandler;
import org.pentaho.hdfs.vfs.wrapper.HadoopFileSystem;
import org.pentaho.hdfs.vfs.wrapper.HadoopFileSystemImpl;

import com.mapr.fs.proto.Security.TicketAndKey;
import com.mapr.login.client.MapRLoginHttpsClient;

public class MapRFSFileSystemKerberosAuthenticationConsumer implements
    AuthenticationConsumer<HadoopFileSystem, KerberosAuthenticationProvider> {
  @AuthenticationConsumerPlugin( id = "MapRFSFileSystemKerberosAuthenticationConsumer",
      name = "MapRFSFileSystemKerberosAuthenticationConsumer" )
  public static class MapRFSFileSystemKerberosAuthenticationConsumerType implements AuthenticationConsumerType {

    @Override
    public String getDisplayName() {
      return "MapRFSFileSystemKerberosAuthenticationConsumer";
    }

    @Override
    public Class<? extends AuthenticationConsumer<?, ?>> getConsumerClass() {
      return MapRFSFileSystemKerberosAuthenticationConsumer.class;
    }
  }
  
  private MapRFSFileSystemAuthenticationConsumerArg maprFSFileSystemAuthenticationConsumerArg;
  private KerberosUtil kerberosUtil;

  public MapRFSFileSystemKerberosAuthenticationConsumer(
      MapRFSFileSystemAuthenticationConsumerArg maprFSFileSystemAuthenticationConsumerArg ) {
    this.maprFSFileSystemAuthenticationConsumerArg = maprFSFileSystemAuthenticationConsumerArg;
    kerberosUtil = new KerberosUtil();
  }

  @Override
  public HadoopFileSystem consume( KerberosAuthenticationProvider authenticationProvider )
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

    try {
      loginContext.login();
      TicketAndKey maprTicket = Subject.doAs( loginContext.getSubject(), new PrivilegedExceptionAction<TicketAndKey>() {

        @Override
        public TicketAndKey run() throws Exception {
          return new MapRLoginHttpsClient().getMapRCredentialsViaKerberos( 1209600000L );
        }
      } );
      System.out.println(maprTicket);
      return LoginContextInvocationHandler.forObject( new HadoopFileSystemImpl( org.apache.hadoop.fs.FileSystem
          .get( maprFSFileSystemAuthenticationConsumerArg.getConf() ) ), loginContext, new HashSet<Class<?>>( Arrays
          .<Class<?>> asList( HadoopFileSystem.class ) ) );
    } catch ( IOException e ) {
      throw new AuthenticationConsumptionException( e );
    } catch ( LoginException e ) {
      throw new AuthenticationConsumptionException( e );
    } catch ( PrivilegedActionException e ) {
      throw new AuthenticationConsumptionException( e );
    }
  }
}
