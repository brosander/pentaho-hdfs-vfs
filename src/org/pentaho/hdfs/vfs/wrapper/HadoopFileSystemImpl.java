package org.pentaho.hdfs.vfs.wrapper;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HadoopFileSystemImpl implements HadoopFileSystem {
  private final FileSystem delegate;

  public HadoopFileSystemImpl( FileSystem delegate ) {
    this.delegate = delegate;
  }

  @Override
  public FileStatus getFileStatus( Path path ) throws IOException {
    return delegate.getFileStatus( path );
  }

  @Override
  public FSDataOutputStream append( Path path ) throws IOException {
    return delegate.append( path );
  }

  @Override
  public FSDataOutputStream create( Path path ) throws IOException {
    return delegate.create( path );
  }

  @Override
  public FSDataInputStream open( Path path ) throws IOException {
    return delegate.open( path );
  }

  @Override
  public void mkdirs( Path path ) throws IOException {
    delegate.mkdirs( path );
  }

  @Override
  public void delete( Path path, boolean b ) throws IOException {
    delegate.delete( path, b );
  }

  @Override
  public void rename( Path path, Path path2 ) throws IOException {
    delegate.rename( path, path2 );
  }

  @Override
  public void setTimes( Path path, long modtime, long currentTimeMillis ) throws IOException {
    delegate.setTimes( path, modtime, currentTimeMillis );
  }

  @Override
  public FileStatus[] listStatus( Path path ) throws IOException {
    return delegate.listStatus( path );
  }
}
