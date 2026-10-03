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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * One SMB share session. Paths are share-relative and use {@code \} separators. An empty path is
 * the root of the share (after the connection's base folder has already been applied).
 *
 * <p>{@link #stat} returns null when the path does not exist. Access denied and logon failure are
 * thrown, so a permission error is not reported as a missing file.
 */
public interface SmbShare extends Closeable {

  SmbEntry stat(String sharePath) throws IOException;

  List<String> children(String sharePath) throws IOException;

  InputStream openRead(String sharePath) throws IOException;

  OutputStream openWrite(String sharePath, boolean append) throws IOException;

  SmbRandom openRandom(String sharePath, boolean write) throws IOException;

  void createFolder(String sharePath) throws IOException;

  void delete(String sharePath) throws IOException;

  void rename(String sharePath, String newSharePath) throws IOException;

  /** A file or folder on the share. */
  record SmbEntry(boolean directory, long size, long lastModified) {}

  /** Offset read and write against one open file. Closing releases the server handle. */
  interface SmbRandom extends Closeable {
    int read(byte[] buffer, int offset, int length, long fileOffset) throws IOException;

    void write(byte[] buffer, int offset, int length, long fileOffset) throws IOException;

    long length() throws IOException;

    void setLength(long length) throws IOException;
  }
}
