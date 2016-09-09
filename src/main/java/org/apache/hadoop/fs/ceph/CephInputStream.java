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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSInputStream;

import com.ceph.fs.CephMount;

/**
 * <p>
 * An {@link FSInputStream} for a CephFileSystem and corresponding
 * Ceph instance.
 */
public class CephInputStream extends FSInputStream {
  private static final Log LOG = LogFactory.getLog(CephInputStream.class);
  private boolean closed;

  private int fileHandle;

  private long fileLength;

  private boolean talkerDebug;

  private CephFsProto ceph;

  private byte[] buffer;
  
  private long lastPos = 0;
  private long bufferStartPos = 0;
  private long currentPos = 0;
  private long bufferEndPos = 0;
  private long bufferTime = 0;

  /**
   * Create a new CephInputStream.
   * @param conf The system configuration. Unused.
   * @param fh The filehandle provided by Ceph to reference.
   * @param flength The current length of the file. If the length changes
   * you will need to close and re-open it to access the new data.
   */
  public CephInputStream(Configuration conf, CephFsProto cephfs,
      int fh, long flength, int bufferSize) {
    // Whoever's calling the constructor is responsible for doing the actual ceph_open
    // call and providing the file handle.
    fileLength = flength;
    fileHandle = fh;
    closed = false;
    ceph = cephfs;
    buffer = new byte[1<<22];

    talkerDebug = conf.getBoolean(CephConfigKeys.CEPH_TALKER_INTERFACE_DEBUG_KEY,
                       CephConfigKeys.CEPH_TALKER_INTERFACE_DEBUG_DEFAULT);
    LOG.debug(
        "CephInputStream constructor: initializing stream with fh " + fh
        + " and file length " + flength);
      
  }

  /** Ceph likes things to be closed before it shuts down,
   * so closing the IOStream stuff voluntarily in a finalizer is good
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

  private synchronized int fillBuffer(int len, long offset) throws IOException {
    LOG.info("[InputStream]: fill buffer, offset " + offset + ", len " + len);

    if (offset > 0)
    {
        seek(offset);
    }

    int readLen = Math.min(buffer.length, len);
    int ret = 0;
    ret = ceph.read(fileHandle, buffer, readLen, -1);
    if (ret < 0) {
      int err = ret;
      bufferStartPos = currentPos;
      bufferEndPos = currentPos;
      // attempt to reset to old position. If it fails, too bad.
      ceph.lseek(fileHandle, currentPos, CephMount.SEEK_SET);
      throw new IOException("Failed to fill read buffer! Error code:" + err);
    }
    
    bufferStartPos = currentPos;
    bufferEndPos = bufferStartPos + ret;
    bufferTime = System.currentTimeMillis();
    
    return ret;
  }

  /*
   * Get the current position of the stream.
   */
  public synchronized long getPos() throws IOException {
    return currentPos;
  }

  /**
   * Find the number of bytes remaining in the file.
   */
  @Override
  public synchronized int available() throws IOException {
    if (closed)
      throw new IOException("file is closed");
    return (int) (fileLength - getPos());
  }

  public synchronized void seek(long targetPos) throws IOException {
    LOG.trace(
        "CephInputStream.seek: Seeking to position " + targetPos + " on fd "
        + fileHandle);
    if (targetPos > fileLength) {
      throw new IOException(
          "CephInputStream.seek: failed seek to position " + targetPos
          + " on fd " + fileHandle + ": Cannot seek after EOF " + fileLength);
    }

    if (talkerDebug){
      LOG.info("[talker debug]: seek begin, fd " + fileHandle+ ", target pos " + targetPos 
        + ", old ceph pos " + lastPos);
    }

    currentPos = ceph.lseek(fileHandle, targetPos, CephMount.SEEK_SET);
    if (currentPos < 0) {
      currentPos = lastPos;
      throw new IOException("Ceph failed to seek to new position!");
    }
    
    if (talkerDebug){
      LOG.info("[talker debug]: seek end, fd " + fileHandle + ", target pos " + targetPos 
        + ", and current pos " + currentPos);
    }
  }

  /**
   * Failovers are handled by the Ceph code at a very low level;
   * if there are issues that can be solved by changing sources
   * they'll be dealt with before anybody even tries to call this method!
   * @return false.
   */
  public synchronized boolean seekToNewSource(long targetPos) {
    return false;
  }
    
  /**
   * Read a byte from the file.
   * @return the next byte.
   */
  @Override
  public synchronized int read() throws IOException {
    LOG.trace(
        "CephInputStream.read: Reading a single byte from fd " + fileHandle
        + " by calling general read function");

    byte result[] = new byte[1];

    if (getPos() >= fileLength) {
      return -1;
    }
    if (-1 == read(result, 0, 1)) {
      return -1;
    }
    if (result[0] < 0) {
      return 256 + (int) result[0];
    } else {
      return result[0];
    }
  }

  /**
   * Read a specified number of bytes from the file into a byte[].
   * @param buf the byte array to read into.
   * @param off the offset to start at in the file
   * @param len the number of bytes to read
   * @return actual number of bytes read
   * @throws IOException on bad input.
   */
  @Override
  public synchronized int read(byte buf[], int off, int len)
    throws IOException {
    LOG.trace(
        "CephInputStream.read: Reading " + len + " bytes from fd " + fileHandle);
      
    if (closed) {
      throw new IOException(
          "CephInputStream.read: cannot read " + len + " bytes from fd "
          + fileHandle + ": stream closed");
    }

    // ensure we're not past the end of the file
    if (getPos() >= fileLength) {
      LOG.debug(
          "CephInputStream.read: cannot read " + len + " bytes from fd "
          + fileHandle + ": current position is " + getPos()
          + " and file length is " + fileLength);
      return -1;
    }

    if (talkerDebug){
      LOG.info("[talker debug]: read begin, fd " + fileHandle + ", offset " + off + ", len " + len);
    }

    int totalRead = 0;
    // random read ?
    long posOffset = currentPos - lastPos;
    if (Math.abs(currentPos - lastPos) > 2097152) {
      totalRead = randomRead(buf, off, len); 
    }
    else {
      totalRead = seqRead(buf, off, len);
    }

    lastPos = currentPos;

    if (talkerDebug){
      LOG.info("[talker debug]: read end, fd " + fileHandle + ", offset " + off + ", len " + len);
    }

    return totalRead;
  }

  public synchronized int randomRead(byte buf[], int off, int len) throws IOException {
    if (talkerDebug)
      LOG.info("[InputStream]: random read begin, fd " + fileHandle + ", off " + off + ", len " + len + ", bufferStartPos " + bufferStartPos + ", currentPos " + currentPos
              + ", bufferEndPos " + bufferEndPos);

    int ret = 0;
    int totalRead = 0;
    do {
      ret = fillBuffer(len, -1);
      if (ret < 0) {
        // assert
        throw new IOException("CephInputStream.randomRead: assert " + ret);
      }

      if (ret == 0) {
        // read end of file
        break;
      }

      try {
        System.arraycopy(buffer, 0, buf, totalRead + off, ret);
        currentPos += ret;
      } catch (IndexOutOfBoundsException ie) {
        throw new IOException(
            "CephInputStream.randomRead: Indices out of bounds:" + "read length is "
            + len + ", buffer offset is " + totalRead);
      } catch (ArrayStoreException ae) {
        throw new IOException(
            "Uh-oh, CephInputStream failed to do an array"
                + "copy due to type mismatch...");
      } catch (NullPointerException ne) {
        throw new IOException(
            "CephInputStream.randomRead: cannot read " + len + "bytes from fd:"
            + fileHandle + ": buf is null");
      }
      
      len -= ret;
      totalRead += ret;
    } while (len > 0);

    LOG.info("[InputStream]: random read end, fd " + fileHandle + ", off " + off + ", len " + len + ", bufferStartPos " + bufferStartPos + ", currentPos " + currentPos
            + ", bufferEndPos " + bufferEndPos);
    
    return totalRead;
  }
  
  public synchronized int seqRead(byte buf[], int off, int len) throws IOException {
    if (talkerDebug)
      LOG.info("[Inputstream]: seq read begin, fd " + fileHandle + ", off " + off + ", len " + len + ", bufferStartPos " + bufferStartPos + ", currentPos " +
              currentPos + ", bufferEndPos " + bufferEndPos);

    int totalRead = 0;

    long nowTime = System.currentTimeMillis();
    
    // read buffer valid ?
    if ((Math.abs(nowTime - bufferTime)) >= 1000 || 
        (currentPos < bufferStartPos || currentPos >= bufferEndPos)) {
      if (talkerDebug)
        LOG.info("[Inputstream]: seq read, read buffer invlid because timeout or not in read buffer, fd " + fileHandle + ", reset bufferStartPos " +
                  bufferStartPos + ", currentPos " + currentPos + ", bufferEndPos " + bufferEndPos);
      bufferStartPos = currentPos;
      bufferEndPos = currentPos;
    }
    
    do {
      int read_len = (int)Math.min(len, bufferEndPos - currentPos);
      int bufferPos = (int)(currentPos - bufferStartPos);
      try {
        // read from buffer
        System.arraycopy(buffer, bufferPos, buf, totalRead + off, read_len);
        currentPos += read_len;
      } catch (IndexOutOfBoundsException ie) {
        throw new IOException(
            "CephInputStream.seqRead: Indices out of bounds:" + "read length is "
            + len + ", buffer offset is " + totalRead);
      } catch (ArrayStoreException ae) {
        throw new IOException(
            "Uh-oh, CephInputStream failed to do an array"
                + "copy due to type mismatch...");
      } catch (NullPointerException ne) {
        throw new IOException(
            "CephInputStream.seqRead: cannot read " + len + "bytes from fd:"
            + fileHandle + ": buf is null");
      }

      len -= read_len;
      totalRead += read_len;
      if (len < 0) {
        throw new IOException("[Inputstream]: seq read");
      }
      
      if (len == 0) {
        break;
      }

      int ret = fillBuffer(buffer.length, currentPos);
      if (ret < 0) {
        throw new IOException("CephInputStream.seqRead: fillBuffer ret " + ret);
      }
      if (ret == 0)
        // read end of file
        break;

      if (talkerDebug)
        LOG.info("[Inputstream]: seq read, fd " + fileHandle + ", after fill buffer, bufferStartPos " +
                  bufferStartPos + ", currentPos " + currentPos + ", bufferEndPos " + bufferEndPos);
    } while(true);

    if (talkerDebug)
      LOG.info("[Inputstream]: seq read end, fd " + fileHandle + ", off " + off + ", len " + len + ", bufferStartPos " + bufferStartPos + ", currentPos " +
                currentPos + ", bufferEndPos " + bufferEndPos);
  
    return totalRead;
  }

  /**
   * Close the CephInputStream and release the associated filehandle.
   */
  @Override
  public void close() throws IOException {
    LOG.trace("CephOutputStream.close:enter");

    if (talkerDebug){
      LOG.info("[talker debug]: close");
    }
    
    if (!closed) {
      ceph.close(fileHandle);

      closed = true;
      LOG.trace("CephOutputStream.close:exit");
    }
  }
}
