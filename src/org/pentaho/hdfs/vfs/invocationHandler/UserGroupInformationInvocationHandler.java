package org.pentaho.hdfs.vfs.invocationHandler;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.security.PrivilegedExceptionAction;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.security.UserGroupInformation;

public class UserGroupInformationInvocationHandler<T> implements InvocationHandler {
    private final T delegate;
    private final UserGroupInformation ugi;
    private final Set<Class<?>> interfacesToDelegate;

    public UserGroupInformationInvocationHandler( T delegate, UserGroupInformation ugi ) {
      this( delegate, ugi, new HashSet<Class<?>>() );
    }

    public UserGroupInformationInvocationHandler( T delegate, UserGroupInformation ugi, Set<Class<?>> interfacesToDelegate ) {
      this.delegate = delegate;
      this.ugi = ugi;
      this.interfacesToDelegate = interfacesToDelegate;
    }

    @SuppressWarnings( "unchecked" )
    public static <T> T forObject( T delegate, UserGroupInformation ugi, Set<Class<?>> interfacesToDelegate ) {
      return (T) Proxy.newProxyInstance( delegate.getClass().getClassLoader(), delegate.getClass().getInterfaces(),
          new UserGroupInformationInvocationHandler<Object>( delegate, ugi, interfacesToDelegate ) );
    }

    @Override
    public Object invoke( Object proxy, final Method method, final Object[] args ) throws Throwable {
      try {
        return ugi.doAs( new PrivilegedExceptionAction<Object>() {

          @Override
          public Object run() throws Exception {
            Object result = method.invoke( delegate, args );
            if ( result != null ) {
              for ( Class<?> iface : result.getClass().getInterfaces() ) {
                if ( interfacesToDelegate.contains( iface ) ) {
                  result = forObject( result, ugi, interfacesToDelegate );
                  break;
                }
              }
            }
            return result;
          }
        } );
      } catch ( Exception e ) {
        if ( e.getCause() instanceof InvocationTargetException ) {
          throw ( (InvocationTargetException) e.getCause() ).getCause();
        }
        throw e;
      }
    }
  }
