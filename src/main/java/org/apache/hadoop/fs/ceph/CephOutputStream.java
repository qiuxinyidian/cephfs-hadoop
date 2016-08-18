// -*- mode:Java; tab-width:2; c-basic-offset:2; indent-tabs-mode:t -*- 

/**
 *
 * Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * 
 * Implements the Hadoop FS interfaces to allow applications to store
 * files in Ceph.
 */

package org.apache.hadoop.fs.ceph;


import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.fs.Syncable;

import com.ceph.fs.CephMount;

/**
 * <p>
 * An {@link OutputStream} for a CephFileSystem and corresponding
 * Ceph instance.
 *
 * TODO:
 *  - When libcephfs-jni supports ByteBuffer interface we can get rid of the
 *  use of the buffer here to reduce memory copies and just use buffers in
 *  libcephfs. Currently it might be useful to reduce JNI crossings, but not
 *  much more.
 */
public class CephOutputStream extends OutputStream 
	implements Syncable {
  private static final Log LOG = LogFactory.getLog(CephOutputStream.class);
  private boolean closed;

  private CephFsProto ceph;

  private int fileHandle;

  private byte[] buffer;
  private int bufUsed = 0;
  
  private boolean talkerDebug;

  /**
   * Construct the CephOutputStream.
   * @param conf The FileSystem configuration.
   * @param fh The Ceph filehandle to connect to.
   */
  public CephOutputStream(Configuration conf, CephFsProto cephfs,
      int fh, int bufferSize) {
    ceph = cephfs;
    fileHandle = fh;
    closed = false;
    buffer = new byte[1<<21];
    talkerDebug = conf.getBoolean(CephConfigKeys.CEPH_TALKER_INTERFACE_DEBUG_KEY,
                           CephConfigKeys.CEPH_TALKER_INTERFACE_DEBUG_DEFAULT);
    if (talkerDebug)
      LOG.info("[OutputStream debug]: initialize, coff " + conf.toString()); 
  }

  /**
   * Close the Ceph file handle if close() wasn't explicitly called.
   */
  protected void finalize() throws Throwable {
    try {
      if (!closed) {
        close();
      }
    } finally {
      super.finalize();
    }
  }

  /**
   * Ensure that the stream is opened.
   */
  private synchronized void checkOpen() throws IOException {
    if (closed)
      throw new IOException("houbin: operation on closed stream (fd=" + fileHandle + ")");
  }

  /**
   * Get the current position in the file.
   * @return The file offset in bytes.
   */
  public synchronized long getPos() throws IOException {
    checkOpen();
    return ceph.lseek(fileHandle, 0, CephMount.SEEK_CUR);
  }

  @Override
  public synchronized void write(int b) throws IOException {
    byte buf[] = new byte[1];
    buf[0] = (byte) b;    
    write(buf, 0, 1);
  }

  @Override
  public synchronized void write(byte buf[], int off, int len) throws IOException {
    checkOpen();

    while (len > 0) {
      int remaining = Math.min(len, buffer.length - bufUsed);
      System.arraycopy(buf, off, buffer, bufUsed, remaining);

      bufUsed += remaining;
      off += remaining;
      len -= remaining;

      if (buffer.length == bufUsed)
        flushBuffer();
    }
  }

  /*
   * Moves data from the buffer into libcephfs.
   */
  private synchronized void flushBuffer() throws IOException {
    if (bufUsed == 0)
      return;

    while (bufUsed > 0) {
      int ret = ceph.write(fileHandle, buffer, bufUsed, -1);
      if (ret < 0)
        throw new IOException("ceph.write: ret=" + ret);

      if (ret == bufUsed) {
        bufUsed = 0;
        return;
      }

      assert(ret > 0);
      assert(ret < bufUsed);

      /*
       * TODO: handle a partial write by shifting the remainder of the data in
       * the buffer back to the beginning and retrying the write. It would
       * probably be better to use a ByteBuffer 'view' here, and I believe
       * using a ByteBuffer has some other performance benefits but we'll
       * likely need to update the libcephfs-jni implementation.
       */
      int remaining = bufUsed - ret;
      System.arraycopy(buffer, ret, buffer, 0, remaining);
      bufUsed -= ret;
    }

    assert(bufUsed == 0);
  } 

  private void flushOrSync(boolean if_sync)
      throws IOException {
    checkOpen();
    flushBuffer(); // buffer -> libcephfs
    if (if_sync)
      ceph.fsync(fileHandle); // libcephfs -> cluster 
  }

  @Override
  public void hflush() throws IOException {
    if (talkerDebug)
      LOG.info("[OutputStream debug]: hflush function, flush data to buffer");
    flushOrSync(false); 
  } 

  @Override
  public void hsync() throws IOException {
    if (talkerDebug)
      LOG.info("[OutputStream debug]: hsync function, flush and sync data to buffer or ceph");
    flushOrSync(true);
  }
  
  @Override
  public synchronized void flush() throws IOException {
    checkOpen();
    flushBuffer(); // buffer -> libcephfs
    //ceph.fsync(fileHandle); // libcephfs -> cluster
  }
  
  @Override
  public synchronized void close() throws IOException {
    // In hive, outputstream will be closed twice. So return when catched exception.
    try {
      checkOpen();
    }
    catch(IOException e) {
      LOG.debug("outputstream already closed, fd is " + fileHandle);
      return;
    }

    hsync();
    ceph.close(fileHandle);
    closed = true;
  }
}
