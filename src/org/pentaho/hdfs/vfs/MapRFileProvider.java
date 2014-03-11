/*!
* Copyright 2010 - 2013 Pentaho Corporation.  All rights reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
*/

package org.pentaho.hdfs.vfs;

import org.apache.commons.vfs.FileName;
import org.apache.commons.vfs.FileSystem;
import org.apache.commons.vfs.FileSystemException;
import org.apache.commons.vfs.FileSystemOptions;
import org.apache.commons.vfs.provider.URLFileName;
import org.mortbay.util.MultiMap;
import org.mortbay.util.UrlEncoded;
import org.pentaho.di.core.auth.AuthenticationPersistenceManager;
import org.pentaho.di.core.auth.core.AuthenticationConsumptionException;
import org.pentaho.di.core.auth.core.AuthenticationManager;
import org.pentaho.di.core.auth.core.AuthenticationPerformer;
import org.pentaho.hdfs.vfs.auth.HDFSFileSystemAuthenticationConsumerArg;
import org.pentaho.hdfs.vfs.auth.MapRFSFileSystemKerberosAuthenticationConsumer;
import org.pentaho.hdfs.vfs.auth.MapRFSFileSystemNoAuthenticationConsumer;

/**
 * Provides access to the MapR FileSystem VFS implementation.
 *
 * @author Jordan Ganoff (jganoff@pentaho.com)
 */
public class MapRFileProvider extends HDFSFileProvider {
  /**
   * The scheme this provider was designed to support
   */
  public static final String SCHEME = "maprfs";
  /**
   * File System implementation for maprfs
   */
  public static final String FS_MAPR_IMPL = "com.mapr.fs.MapRFileSystem";

  public MapRFileProvider() {
    setFileNameParser(new MapRFileNameParser());
  }

  @Override
  protected FileSystem doCreateFileSystem(FileName name, FileSystemOptions fileSystemOptions) throws FileSystemException {
    MultiMap params = new MultiMap();
    UrlEncoded.decodeTo( ( (URLFileName) name ).getQueryString(), params, "UTF-8" );
    if ( params.containsKey( AUTH_PROVIDER ) ) {
      AuthenticationManager manager = AuthenticationPersistenceManager.getAuthenticationManager();
      manager.registerConsumerClass( MapRFSFileSystemKerberosAuthenticationConsumer.class );
      manager.registerConsumerClass( MapRFSFileSystemNoAuthenticationConsumer.class );
      AuthenticationPerformer<FileSystem, HDFSFileSystemAuthenticationConsumerArg> performer =
          manager.getAuthenticationPerformer( FileSystem.class, HDFSFileSystemAuthenticationConsumerArg.class, String
              .valueOf( params.get( AUTH_PROVIDER ) ) );
      if ( performer == null ) {
        throw new FileSystemException( "Unable to find compatible authentication provider with id "
            + params.getString( AUTH_PROVIDER ) );
      } else {
        try {
          return performer.perform( new HDFSFileSystemAuthenticationConsumerArg( name, fileSystemOptions ) );
        } catch ( AuthenticationConsumptionException e ) {
          if ( e.getCause() instanceof FileSystemException) {
            throw (FileSystemException) e.getCause();
          } else {
            throw new FileSystemException( e );
          }
        }
      }
    } else {
      return new MapRFileSystem(name, fileSystemOptions);
    }
  }
}
