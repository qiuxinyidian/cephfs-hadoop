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
 * Wraps a number of native function calls to communicate with the Ceph
 * filesystem.
 */
package org.apache.hadoop.fs.ceph;

import java.io.IOException;
import java.net.URI;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.net.InetAddress;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.lang.StringUtils;

import com.ceph.fs.CephMount;
import com.ceph.fs.CephStat;
import com.ceph.fs.CephStatVFS;
import com.ceph.fs.CephFileAlreadyExistsException;
import com.ceph.fs.CephNotDirectoryException;
import com.ceph.fs.CephPoolException;
import com.ceph.crush.Bucket;
import com.ceph.fs.CephFileExtent;

class CephTalker extends CephFsProto {

  private static final Log LOG;

  private CephMount mount;
  private short defaultReplication;

  private boolean talkerDebug;

  public CephTalker(Configuration conf, Log log) {
    LOG = log;
    mount = null;
  }

  private String pathString(Path path) {
		if (null == path) {
			return "/";
		}
    return path.toUri().getPath();
  }

  void initialize(URI uri, Configuration conf) throws IOException {
    talkerDebug = conf.get(CephConfigKeys.CEPH_TALKER_INTERFACE_DEBUG_KEY,
                           CephConfigKeys.CEPH_TALKER_INTERFACE_DEBUG_DEFAULT);
    if (talkerDebug)
      LOG.info("[talker debug]: initialize, uri " + uri.toString() + ", coff " + conf.toString());

    /*
     * Create mount with auth user id
     */
    String user_id = conf.get(
        CephConfigKeys.CEPH_AUTH_ID_KEY,
        CephConfigKeys.CEPH_AUTH_ID_DEFAULT);
    mount = new CephMount(user_id);

    /*
     * Load a configuration file if specified
     */
    String configfile = conf.get(
        CephConfigKeys.CEPH_CONF_FILE_KEY,
        CephConfigKeys.CEPH_CONF_FILE_DEFAULT);
    if (configfile != null) {
      mount.conf_read_file(configfile);
    }

    /* Set auth keyfile */
    String keyfile = conf.get(
        CephConfigKeys.CEPH_AUTH_KEYFILE_KEY,
        CephConfigKeys.CEPH_AUTH_KEYFILE_DEFAULT);
    if (keyfile != null)
      mount.conf_set("keyfile", keyfile);

    /* Set auth keyring */
    String keyring = conf.get(
        CephConfigKeys.CEPH_AUTH_KEYRING_KEY,
        CephConfigKeys.CEPH_AUTH_KEYRING_DEFAULT);
    if (keyring != null)
      mount.conf_set("keyring", keyring);

    /* set hadoop root user */
    mount.conf_set("client_extra_root_users", "true");

    /* Set monitor */
    String mon_addr = null;
    String mon_host = uri.getHost();
    int mon_port = uri.getPort();
    if (mon_host != null && mon_port != -1)
      mon_addr = mon_host + ":" + mon_port;
    else {
      mon_addr = conf.get(
          CephConfigKeys.CEPH_MON_ADDR_KEY,
          CephConfigKeys.CEPH_MON_ADDR_DEFAULT);
    }
    if (mon_addr != null)
        mount.conf_set("mon_host", mon_addr);

    /*
     * Parse and set Ceph configuration options
     */
    String configopts = conf.get(
        CephConfigKeys.CEPH_CONF_OPTS_KEY,
        CephConfigKeys.CEPH_CONF_OPTS_DEFAULT);
    if (configopts != null) {
      String[] options = configopts.split(",");
      for (String option : options) {
          String[] keyval = option.split("=");
          if (keyval.length != 2) {
              throw new IllegalArgumentException("Invalid Ceph option: " + option);
          }
          String key = keyval[0];
          String val = keyval[1];
          try {
            mount.conf_set(key, val);
          } catch (Exception e) {
            throw new IOException("Error setting Ceph option " + key + " = " + val);
          }
      }
    }

    /*
     * Get default replication from configuration.
     */
    defaultReplication = (short)conf.getInt(
        CephConfigKeys.CEPH_REPLICATION_KEY,
        CephConfigKeys.CEPH_REPLICATION_DEFAULT);

    /*
     * Use a different root?
     */
    String root = conf.get(
          CephConfigKeys.CEPH_ROOT_DIR_KEY,
          CephConfigKeys.CEPH_ROOT_DIR_DEFAULT);

    /* Actually mount the file system */
    mount.mount(root);

    /*
     * Allow reads from replica objects?
     */
    boolean localizeReads = conf.getBoolean(
        CephConfigKeys.CEPH_LOCALIZE_READS_KEY,
        CephConfigKeys.CEPH_LOCALIZE_READS_DEFAULT);
    mount.localize_reads(localizeReads);

    /* set client_permissions */
    String ceph_client_permissions = conf.get(
        CephConfigKeys.CEPH_CLIENT_PERMISSIONS_KEY,
        CephConfigKeys.CEPH_CLIENT_PERMISSIONS_DEFAULT);
    mount.conf_set("client_permissions", ceph_client_permissions);

    mount.chdir("/");
  }

  /*
   * Open a file. Allows directories to be opened. used internally to get the
   * pool name. Hadoop doesn't allow directories to be opened, and that is
   * handled below.
   */
  int __open(Path path, int flags, int mode) throws IOException {
    return mount.open(pathString(path), flags, mode);
  }

  /*
   * Open a file. Ceph will not complain if we open a directory, but this
   * isn't something that Hadoop expects and we should throw an exception in
   * this case.
   */
  int open(Path path, int flags, int mode) throws IOException {
    if (talkerDebug)
      LOG.info("[talker debug]: open, path " + pathString(path) + ", flag " + flags + ", mode " + mode);
    
    int fd = __open(path, flags, mode);

    if (talkerDebug)
      LOG.info("[talker debug]: open, fd " + fd);

    CephStat stat = new CephStat();
    fstat(fd, stat);
    if (stat.isDir()) {
      mount.close(fd);
      throw new FileNotFoundException();
    }
    return fd;
  }

  /*
   * Same as open(path, flags, mode) alternative, but takes custom striping
   * parameters that are used when a file is being created.
   */
  int open(Path path, int flags, int mode, int stripe_unit, int stripe_count,
      int object_size, String data_pool) throws IOException {
      
    if (talkerDebug)
      LOG.info("[talker debug]: open, path " + pathString(path) + ", flag " + flags + ", mode " + mode +
               "strip_unit " + stripe_unit + ", stripe_count " + stripe_count + ", object_size " + 
               object_size + ", data_pool " + data_pool);

    int fd = mount.open(pathString(path), flags, mode, stripe_unit,
        stripe_count, object_size, data_pool);

    if (talkerDebug)
      LOG.info("[talker debug]: open, fd " + fd);
    
    CephStat stat = new CephStat();
    fstat(fd, stat);
    if (stat.isDir()) {
      mount.close(fd);
      throw new FileNotFoundException();
    }
    return fd;
  }


  void fstat(int fd, CephStat stat) throws IOException {
    
    if (talkerDebug)
      LOG.info("[talker debug]: fstat, fd " + fd);  
    
    mount.fstat(fd, stat);
  }

  void lstat(Path path, CephStat stat) throws IOException {
    try {
      if (talkerDebug)
        LOG.info("[talker debug]: lstat, path " + pathString(path));  
      mount.lstat(pathString(path), stat);
    } catch (CephNotDirectoryException e) {
      throw new FileNotFoundException();
    }
  }

  void statfs(Path path, CephStatVFS stat) throws IOException {
	  try {
     if (talkerDebug)
        LOG.info("[talker debug]: statfs, path " + pathString(path)); 
		  mount.statfs(pathString(path), stat);
	  } catch (FileNotFoundException e) {
		  throw new FileNotFoundException();
	  }

  }
  
  void rmdir(Path path) throws IOException {
   if (talkerDebug)
      LOG.info("[talker debug]: rmdir, path " + pathString(path)); 
    mount.rmdir(pathString(path));
  }

  void unlink(Path path) throws IOException {
   if (talkerDebug)
      LOG.info("[talker debug]: unlink, path " + pathString(path)); 
    mount.unlink(pathString(path));
  }

  void truncate(Path path, long newLength) throws IOException {
   if (talkerDebug)
      LOG.info("[talker debug]: truncate, path " + pathString(path) + ", newLength " + newLength); 
    mount.truncate(pathString(path),newLength);
  }

  void rename(Path src, Path dst) throws IOException {
   if (talkerDebug)
      LOG.info("[talker debug]: rename, src " + pathString(src) + ", dst " + pathString(dst)); 
    mount.rename(pathString(src), pathString(dst));
  }

  String[] listdir(Path path) throws IOException {
   if (talkerDebug)
      LOG.info("[talker debug]: listdir, path " + pathString(path));     
    CephStat stat = new CephStat();
    try {
      mount.lstat(pathString(path), stat);
    } catch (FileNotFoundException e) {
      return null;
    }
    if (!stat.isDir())
      return null;
    return mount.listdir(pathString(path));
  }

  void mkdirs(Path path, int mode) throws IOException {
   if (talkerDebug)
      LOG.info("[talker debug]: mkdirs, path " + pathString(path) + ", mode " + mode);  
    mount.mkdirs(pathString(path), mode);
  }

  void close(int fd) throws IOException {
   if (talkerDebug)
      LOG.info("[talker debug]: close, fd " + fd);  
    mount.close(fd);
  }

  void chmod(Path path, int mode) throws IOException {
   if (talkerDebug)
      LOG.info("[talker debug]: chmod, path " + pathString(path) + ", mode " + mode); 
    mount.chmod(pathString(path), mode);
  }

  void chown(Path path, String username, String groupname) throws IOException {
   if (talkerDebug)
      LOG.info("[talker debug]: chown, path " + pathString(path) + ", username " + username +
               ", groupname " + groupname); 
    mount.chown(pathString(path), username, groupname);
  }

  void shutdown() throws IOException {
   if (talkerDebug)
      LOG.info("[talker debug]: shutdown");
    if (null != mount)
      mount.unmount();
    mount = null;
  }

  short getDefaultReplication() {
    if (talkerDebug)
      LOG.info("[talker debug]: getDefaultReplication " + defaultReplication);
    return defaultReplication;
  }

  short get_file_replication(Path path) throws IOException {
    if (talkerDebug)
      LOG.info("[talker debug]: get_file_replication, path " + pathString(path));
    CephStat stat = new CephStat();
    mount.lstat(pathString(path), stat);
    int replication = 1;
    if (stat.isFile()) {
      int fd;
      /*
       * When we stat files we also retrieve the file replication, but the
       * current libcephfs interface requires we open the file first. Since we
       * might not have read permissions, we try with write permissions. It
       * would be better to have a variant that operated on paths.
       */
      try {
        fd = mount.open(pathString(path), CephMount.O_RDONLY, 0);
      } catch (IOException e) {
        fd = mount.open(pathString(path), CephMount.O_WRONLY, 0);
      }
      replication = mount.get_file_replication(fd);
      mount.close(fd);
    }
    return (short)replication;
  }

  int get_stripe_unit_granularity() {
    if (talkerDebug)
      LOG.info("[talker debug]: get_stripe_unit_granularity");
    return mount.get_stripe_unit_granularity();
  }

  void setattr(Path path, CephStat stat, int mask) throws IOException {
    if (talkerDebug)
      LOG.info("[talker debug]: setattr, path " + pathString(path) + ", mask " + mask);
    mount.setattr(pathString(path), stat, mask);
  }

  void fsync(int fd) throws IOException {
    if (talkerDebug)
      LOG.info("[talker debug]: fsync, fd " + fd);
    mount.fsync(fd, false);
  }

  long lseek(int fd, long offset, int whence) throws IOException {
    if (talkerDebug)
      LOG.info("[talker debug]: lseek, fd " + fd + ", offset " + offset + ", whence " + whence);
    return mount.lseek(fd, offset, whence);
  }

  int write(int fd, byte[] buf, long size, long offset) throws IOException {
    if (talkerDebug)
      LOG.info("[talker debug]: write, fd " + fd + ", size " + size + ", offset " + offset);

    return (int)mount.write(fd, buf, size, offset);
  }

  int read(int fd, byte[] buf, long size, long offset) throws IOException {
    if (talkerDebug)
      LOG.info("[talker debug]: read, fd " + fd + ", size " + size + ", offset " + offset);
    return (int)mount.read(fd, buf, size, offset);
  }

  String get_file_pool_name(int fd) {
    if (talkerDebug)
      LOG.info("[talker debug]: get_file_pool_name, fd " + fd);
    return mount.get_file_pool_name(fd);
  }

  int get_pool_id(String pool_name) throws IOException {
    if (talkerDebug)
      LOG.info("[talker debug]: get_pool_id, pool_name " + pool_name);
    try {
      return mount.get_pool_id(pool_name);
    } catch (CephPoolException e) {
      throw new IOException();
    }
  }

  int get_pool_replication(int poolid) throws IOException {
    if (talkerDebug)
      LOG.info("[talker debug]: get_pool_replication, poolid " + poolid);
    try {
      return mount.get_pool_replication(poolid);
    } catch (CephPoolException e) {
      throw new IOException();
    }
  }

  InetAddress get_osd_address(int osd) throws IOException {
    if (talkerDebug)
      LOG.info("[talker debug]: get_osd_address, osd " + osd);
    return mount.get_osd_address(osd);
  }

  Bucket[] get_osd_crush_location(int osd) throws IOException {
    if (talkerDebug)
      LOG.info("[talker debug]: get_osd_crush_location, osd " + osd);
    return mount.get_osd_crush_location(osd);
  }

  CephFileExtent get_file_extent(int fd, long offset) throws IOException {
    if (talkerDebug)
      LOG.info("[talker debug]: get_file_extent, fd " + fd + ", offset " + offset);
    return mount.get_file_extent(fd, offset);
  }

  String getUsername(int uid) throws IOException {
    return mount.get_username(uid);
  }

  String getGroupname(int gid) throws IOException {
    return mount.get_groupname(gid);
  }
}
