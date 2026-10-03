/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hop.vfs.smb;

import com.hierynomus.msdtyp.AccessMask;
import com.hierynomus.msfscc.FileAttributes;
import com.hierynomus.msfscc.fileinformation.FileAllInformation;
import com.hierynomus.msfscc.fileinformation.FileIdBothDirectoryInformation;
import com.hierynomus.mssmb2.SMB2CreateDisposition;
import com.hierynomus.mssmb2.SMB2CreateOptions;
import com.hierynomus.mssmb2.SMB2ShareAccess;
import com.hierynomus.mssmb2.SMBApiException;
import com.hierynomus.protocol.commons.EnumWithValue;
import com.hierynomus.smbj.SMBClient;
import com.hierynomus.smbj.SmbConfig;
import com.hierynomus.smbj.auth.AuthenticationContext;
import com.hierynomus.smbj.connection.Connection;
import com.hierynomus.smbj.session.Session;
import com.hierynomus.smbj.share.Directory;
import com.hierynomus.smbj.share.DiskShare;
import com.hierynomus.smbj.share.Share;
import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;

/**
 * One smbj client, connection, session, and disk share for the life of a VFS file system. File
 * objects borrow the share. The TCP session is opened on the first call and only then.
 */
final class SmbjSmbShare implements SmbShare {
  private static final Set<AccessMask> READ_ACCESS = EnumSet.of(AccessMask.GENERIC_READ);
  private static final Set<AccessMask> WRITE_ACCESS =
      EnumSet.of(AccessMask.GENERIC_READ, AccessMask.GENERIC_WRITE, AccessMask.DELETE);
  private static final Set<FileAttributes> FILE_ATTRIBUTES =
      EnumSet.of(FileAttributes.FILE_ATTRIBUTE_NORMAL);
  private static final Set<SMB2CreateOptions> FILE_OPTIONS =
      EnumSet.of(SMB2CreateOptions.FILE_NON_DIRECTORY_FILE);

  private final ILogChannel log;
  private final SMBClient client;
  private final String host;
  private final int port;
  private final String shareName;
  private final String domain;
  private final String username;
  private final AuthenticationContext context;

  private Connection connection;
  private Session session;
  private DiskShare disk;

  SmbjSmbShare(
      SmbConfig config,
      String host,
      int port,
      String shareName,
      String domain,
      String username,
      AuthenticationContext context) {
    this(new SMBClient(config), host, port, shareName, domain, username, context);
  }

  SmbjSmbShare(
      SMBClient client,
      String host,
      int port,
      String shareName,
      String domain,
      String username,
      AuthenticationContext context) {
    this.log = LogChannel.GENERAL;
    this.client = client;
    this.host = host;
    this.port = port;
    this.shareName = shareName;
    this.domain = domain == null ? "" : domain;
    this.username = username == null ? "" : username;
    this.context = context;
  }

  @Override
  public SmbEntry stat(String sharePath) throws IOException {
    if (sharePath == null || sharePath.isEmpty()) {
      return new SmbEntry(true, 0, 0);
    }
    try {
      FileAllInformation info = disk().getFileInformation(sharePath);
      long attributes = info.getBasicInformation().getFileAttributes();
      boolean directory =
          EnumWithValue.EnumUtils.isSet(attributes, FileAttributes.FILE_ATTRIBUTE_DIRECTORY);
      long size = directory ? 0 : info.getStandardInformation().getEndOfFile();
      long modified = info.getBasicInformation().getLastWriteTime().toEpochMillis();
      return new SmbEntry(directory, size, modified);
    } catch (SMBApiException e) {
      if (SmbErrors.notFound(e)) {
        return null;
      }
      throw SmbErrors.io(e);
    }
  }

  @Override
  public List<String> children(String sharePath) throws IOException {
    try {
      List<FileIdBothDirectoryInformation> listed = disk().list(sharePath == null ? "" : sharePath);
      List<String> names = new ArrayList<>();
      for (FileIdBothDirectoryInformation child : listed) {
        String name = childName(child.getFileName());
        if (!name.isEmpty()) {
          names.add(name);
        }
      }
      return names;
    } catch (SMBApiException e) {
      throw SmbErrors.io(e);
    }
  }

  @Override
  public InputStream openRead(String sharePath) throws IOException {
    com.hierynomus.smbj.share.File file =
        disk()
            .openFile(
                sharePath,
                READ_ACCESS,
                FILE_ATTRIBUTES,
                SMB2ShareAccess.ALL,
                SMB2CreateDisposition.FILE_OPEN,
                FILE_OPTIONS);
    return new FilterInputStream(file.getInputStream()) {
      @Override
      public void close() throws IOException {
        try {
          super.close();
        } finally {
          file.close();
        }
      }
    };
  }

  @Override
  public OutputStream openWrite(String sharePath, boolean append) throws IOException {
    mkdirs(parent(sharePath));
    com.hierynomus.smbj.share.File file =
        disk()
            .openFile(
                sharePath,
                WRITE_ACCESS,
                FILE_ATTRIBUTES,
                SMB2ShareAccess.ALL,
                append
                    ? SMB2CreateDisposition.FILE_OPEN_IF
                    : SMB2CreateDisposition.FILE_OVERWRITE_IF,
                FILE_OPTIONS);
    // true starts the stream at the current end of the file.
    return new FilterOutputStream(file.getOutputStream(append)) {
      @Override
      public void write(byte[] b, int off, int len) throws IOException {
        out.write(b, off, len);
      }

      @Override
      public void write(byte[] b) throws IOException {
        out.write(b, 0, b.length);
      }

      @Override
      public void close() throws IOException {
        try {
          super.close();
        } finally {
          file.close();
        }
      }
    };
  }

  @Override
  public SmbRandom openRandom(String sharePath, boolean write) throws IOException {
    com.hierynomus.smbj.share.File file =
        disk()
            .openFile(
                sharePath,
                write ? WRITE_ACCESS : READ_ACCESS,
                FILE_ATTRIBUTES,
                SMB2ShareAccess.ALL,
                write ? SMB2CreateDisposition.FILE_OPEN_IF : SMB2CreateDisposition.FILE_OPEN,
                FILE_OPTIONS);
    return new SmbjRandom(file);
  }

  @Override
  public void createFolder(String sharePath) throws IOException {
    mkdirs(sharePath);
  }

  @Override
  public void delete(String sharePath) throws IOException {
    if (sharePath == null || sharePath.isEmpty()) {
      throw new IOException("Cannot delete the root of an SMB share");
    }
    SmbEntry entry = stat(sharePath);
    if (entry == null) {
      return;
    }
    try {
      if (entry.directory()) {
        disk().rmdir(sharePath, false);
      } else {
        disk().rm(sharePath);
      }
    } catch (SMBApiException e) {
      throw SmbErrors.io(e);
    }
  }

  @Override
  public void rename(String sharePath, String newSharePath) throws IOException {
    SmbEntry entry = stat(sharePath);
    if (entry == null) {
      throw new SmbErrors.SmbNotFoundException(sharePath);
    }
    mkdirs(parent(newSharePath));
    try {
      if (entry.directory()) {
        try (Directory directory =
            disk()
                .openDirectory(
                    sharePath,
                    WRITE_ACCESS,
                    EnumSet.of(FileAttributes.FILE_ATTRIBUTE_DIRECTORY),
                    SMB2ShareAccess.ALL,
                    SMB2CreateDisposition.FILE_OPEN,
                    EnumSet.of(SMB2CreateOptions.FILE_DIRECTORY_FILE))) {
          directory.rename(newSharePath, true);
        }
      } else {
        try (com.hierynomus.smbj.share.File file =
            disk()
                .openFile(
                    sharePath,
                    WRITE_ACCESS,
                    FILE_ATTRIBUTES,
                    SMB2ShareAccess.ALL,
                    SMB2CreateDisposition.FILE_OPEN,
                    FILE_OPTIONS)) {
          file.rename(newSharePath, true);
        }
      }
    } catch (SMBApiException e) {
      throw SmbErrors.io(e);
    }
  }

  @Override
  public void close() {
    dropSession();
    try {
      client.close();
    } catch (Exception e) {
      logError("Unable to close the SMB client for " + host, e);
    }
  }

  private synchronized DiskShare disk() throws IOException {
    if (disk != null && disk.isConnected()) {
      return disk;
    }
    dropSession();
    Connection opened = null;
    Session authenticated = null;
    try {
      logBasic(
          "Opening SMB session "
              + host
              + ":"
              + port
              + " share "
              + shareName
              + " as "
              + (domain.isEmpty() ? username : domain + "\\" + username));
      opened = client.connect(host, port);
      authenticated = opened.authenticate(context);
      Share connected = authenticated.connectShare(shareName);
      if (!(connected instanceof DiskShare diskShare)) {
        throw new IOException("SMB share \"" + shareName + "\" is not a disk share");
      }
      connection = opened;
      session = authenticated;
      disk = diskShare;
      return diskShare;
    } catch (RuntimeException | IOException e) {
      closeQuietly(authenticated);
      closeQuietly(opened);
      throw SmbErrors.io(e);
    }
  }

  private void mkdirs(String sharePath) throws IOException {
    if (sharePath == null || sharePath.isEmpty() || stat(sharePath) != null) {
      return;
    }
    mkdirs(parent(sharePath));
    try {
      disk().mkdir(sharePath);
    } catch (SMBApiException e) {
      if (stat(sharePath) == null) {
        throw SmbErrors.io(e);
      }
    }
  }

  private static String parent(String sharePath) {
    if (sharePath == null) {
      return "";
    }
    int slash = sharePath.lastIndexOf('\\');
    return slash < 0 ? "" : sharePath.substring(0, slash);
  }

  private static String childName(String name) {
    if (name == null || name.isEmpty() || ".".equals(name) || "..".equals(name)) {
      return "";
    }
    int slash = Math.max(name.lastIndexOf('\\'), name.lastIndexOf('/'));
    if (slash >= 0) {
      name = name.substring(slash + 1);
    }
    if (name.isEmpty() || ".".equals(name) || "..".equals(name)) {
      return "";
    }
    return name;
  }

  private void logBasic(String message) {
    if (HopLogStore.isInitialized()) {
      log.logBasic(message);
    }
  }

  private void logError(String message, Exception error) {
    if (HopLogStore.isInitialized()) {
      log.logError(message, error);
    }
  }

  // Leased directories close through the open share. Connection.close() does it too late.
  private synchronized void dropSession() {
    if (connection != null && disk != null && disk.isConnected()) {
      closeQuietly(connection.getLeaseManager());
    }
    closeQuietly(disk);
    closeQuietly(session);
    closeQuietly(connection);
    disk = null;
    session = null;
    connection = null;
  }

  private static void closeQuietly(AutoCloseable closeable) {
    if (closeable == null) {
      return;
    }
    try {
      closeable.close();
    } catch (Exception ignored) {
      // The session is being dropped. The next call opens a new one.
    }
  }

  private static final class SmbjRandom implements SmbRandom {
    private final com.hierynomus.smbj.share.File file;

    private SmbjRandom(com.hierynomus.smbj.share.File file) {
      this.file = file;
    }

    @Override
    public int read(byte[] buffer, int offset, int length, long fileOffset) throws IOException {
      if (length == 0) {
        return 0;
      }
      try {
        int read = file.read(buffer, fileOffset, offset, length);
        return read == 0 ? -1 : read;
      } catch (SMBApiException e) {
        throw SmbErrors.io(e);
      }
    }

    @Override
    public void write(byte[] buffer, int offset, int length, long fileOffset) throws IOException {
      try {
        file.write(buffer, fileOffset, offset, length);
      } catch (SMBApiException e) {
        throw SmbErrors.io(e);
      }
    }

    @Override
    public long length() throws IOException {
      try {
        return file.getLength();
      } catch (SMBApiException e) {
        throw SmbErrors.io(e);
      }
    }

    @Override
    public void setLength(long length) throws IOException {
      try {
        file.setLength(length);
      } catch (SMBApiException e) {
        throw SmbErrors.io(e);
      }
    }

    @Override
    public void close() {
      file.close();
    }
  }
}
