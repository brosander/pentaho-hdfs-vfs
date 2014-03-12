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

/**
 * Provides access to the MapR FileSystem VFS implementation.
 * 
 * @author Jordan Ganoff (jganoff@pentaho.com)
 */
public class MapRFileProvider extends HDFSFileProvider {

  public static final String AUTH_PROVIDER = "auth_provider";
  /**
   * The scheme this provider was designed to support
   */
  public static final String SCHEME = "maprfs";
  /**
   * File System implementation for maprfs
   */
  public static final String FS_MAPR_IMPL = "com.mapr.fs.MapRFileSystem";

  public MapRFileProvider() {
    setFileNameParser( new MapRFileNameParser() );
  }

  @Override
  protected FileSystem doCreateFileSystem( FileName name, FileSystemOptions fileSystemOptions )
    throws FileSystemException {
    String queryString = ( (URLFileName) name ).getQueryString();
    if ( queryString != null ) {
      MultiMap params = new MultiMap();
      UrlEncoded.decodeTo( ( (URLFileName) name ).getQueryString(), params, "UTF-8" );
      if ( params.containsKey( AUTH_PROVIDER ) ) {
        if ( fileSystemOptions == null ) {
          fileSystemOptions = new FileSystemOptions();
        }
        new HDFSFileSystemConfigBuilder().setParam( fileSystemOptions, AUTH_PROVIDER, String.valueOf( params
            .get( AUTH_PROVIDER ) ) );
      }
    }
    return new MapRFileSystem( name, fileSystemOptions );
  }
}
