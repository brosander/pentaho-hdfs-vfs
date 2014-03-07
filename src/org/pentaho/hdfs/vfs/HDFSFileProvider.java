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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.apache.commons.vfs.Capability;
import org.apache.commons.vfs.FileName;
import org.apache.commons.vfs.FileSystem;
import org.apache.commons.vfs.FileSystemException;
import org.apache.commons.vfs.FileSystemOptions;
import org.apache.commons.vfs.UserAuthenticationData;
import org.apache.commons.vfs.provider.AbstractOriginatingFileProvider;
import org.apache.commons.vfs.provider.url.UrlFileName;
import org.mortbay.util.MultiMap;
import org.mortbay.util.UrlEncoded;
import org.pentaho.di.core.auth.AuthenticationPersistenceManager;
import org.pentaho.di.core.auth.core.AuthenticationConsumptionException;
import org.pentaho.di.core.auth.core.AuthenticationManager;
import org.pentaho.di.core.auth.core.AuthenticationPerformer;
import org.pentaho.hdfs.vfs.auth.HDFSFileSystemAuthenticationConsumerArg;

public class HDFSFileProvider extends AbstractOriginatingFileProvider {
  /**
   * The scheme this provider was designed to support
   */
  public static final String SCHEME = "hdfs";

  /** User Information. */
  public static final String ATTR_USER_INFO = "UI";

  public static final String AUTH_PROVIDER = "auth_provider";

  /** Authentication types. */
  public static final UserAuthenticationData.Type[] AUTHENTICATOR_TYPES = new UserAuthenticationData.Type[] {
    UserAuthenticationData.USERNAME, UserAuthenticationData.PASSWORD };

  /** The provider's capabilities. */
  protected static final Collection<Capability> capabilities = Collections.unmodifiableCollection( Arrays
      .asList( new Capability[] { Capability.CREATE, Capability.DELETE, Capability.RENAME, Capability.GET_TYPE,
        Capability.LIST_CHILDREN, Capability.READ_CONTENT, Capability.URI, Capability.WRITE_CONTENT,
        Capability.APPEND_CONTENT, Capability.GET_LAST_MODIFIED, Capability.SET_LAST_MODIFIED_FILE,
        Capability.RANDOM_ACCESS_READ } ) );

  public HDFSFileProvider() {
    super();
    setFileNameParser( HDFSFileNameParser.getInstance() );
  }

  protected FileSystem doCreateFileSystem( final FileName name, final FileSystemOptions fileSystemOptions )
    throws FileSystemException {
    MultiMap params = new MultiMap();
    UrlEncoded.decodeTo( ( (UrlFileName) name ).getQueryString(), params, "UTF-8" );
    if ( params.containsKey( AUTH_PROVIDER ) ) {
      AuthenticationManager manager = AuthenticationPersistenceManager.getAuthenticationManager();
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
      return new HDFSFileSystem( name, fileSystemOptions );
    }
  }

  public Collection<Capability> getCapabilities() {
    return capabilities;
  }
}
