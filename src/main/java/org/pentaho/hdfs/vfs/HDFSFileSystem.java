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
import org.apache.commons.vfs.FileObject;
import org.apache.commons.vfs.FileSystem;
import org.apache.commons.vfs.FileSystemException;
import org.apache.commons.vfs.FileSystemOptions;
import org.apache.commons.vfs.provider.AbstractFileSystem;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystem;

import java.util.Collection;

public class HDFSFileSystem extends AbstractFileSystem implements FileSystem {
    private final HadoopFileSystem hdfs;

    protected HDFSFileSystem(final FileName rootName, final FileSystemOptions fileSystemOptions, HadoopFileSystem hdfs) {
        super(rootName, null, fileSystemOptions);
        this.hdfs = hdfs;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    protected void addCapabilities(Collection caps) {
        caps.addAll(HDFSFileProvider.capabilities);
    }

    @Override
    protected FileObject createFile(FileName name) throws Exception {
        return new HDFSFileObject(name, this);
    }

    public HadoopFileSystem getHDFSFileSystem() throws FileSystemException {
        return hdfs;
    }
}
