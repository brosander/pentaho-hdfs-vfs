package org.pentaho.hdfs.vfs.wrapper;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

public interface HadoopFileSystem {

  public FileStatus getFileStatus( Path path ) throws IOException;

  public FSDataOutputStream append( Path path ) throws IOException;

  public FSDataOutputStream create( Path path ) throws IOException;

  public FSDataInputStream open( Path path ) throws IOException;

  public void mkdirs( Path path ) throws IOException;

  public void delete( Path path, boolean b ) throws IOException;

  public void rename( Path path, Path path2 ) throws IOException;
 
  public void setTimes( Path path, long modtime, long currentTimeMillis ) throws IOException;

  public FileStatus[] listStatus( Path path ) throws IOException;

}
