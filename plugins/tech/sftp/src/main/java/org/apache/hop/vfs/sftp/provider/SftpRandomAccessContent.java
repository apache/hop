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
package org.apache.hop.vfs.sftp.provider;

import java.io.DataInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.provider.AbstractRandomAccessStreamContent;
import org.apache.commons.vfs2.util.RandomAccessMode;

/** Random access content. */
final class SftpRandomAccessContent extends AbstractRandomAccessStreamContent {

  /** File pointer */
  protected long filePointer;

  private final SftpFileObject fileObject;
  private DataInputStream dis;
  private InputStream mis;

  SftpRandomAccessContent(final SftpFileObject fileObject, final RandomAccessMode mode) {
    super(mode);

    this.fileObject = fileObject;
    // fileSystem = (FtpFileSystem) this.fileObject.getFileSystem();
  }

  @Override
  public void close() throws IOException {
    if (dis != null) {
      // mis.abort();
      mis.close();

      // this is to avoid recursive close
      final DataInputStream oldDis = dis;
      dis = null;
      oldDis.close();
      mis = null;
    }
  }

  @Override
  protected DataInputStream getDataInputStream() throws IOException {
    if (dis != null) {
      return dis;
    }

    // FtpClient client = fileSystem.getClient();
    mis = fileObject.getInputStream(filePointer);
    dis =
        new DataInputStream(
            new FilterInputStream(mis) {
              @Override
              public void close() throws IOException {
                SftpRandomAccessContent.this.close();
              }

              @Override
              public int read() throws IOException {
                final int ret = super.read();
                if (ret > -1) {
                  filePointer++;
                }
                return ret;
              }

              @Override
              public int read(final byte[] b) throws IOException {
                final int ret = super.read(b);
                if (ret > -1) {
                  filePointer += ret;
                }
                return ret;
              }

              @Override
              public int read(final byte[] b, final int off, final int len) throws IOException {
                final int ret = super.read(b, off, len);
                if (ret > -1) {
                  filePointer += ret;
                }
                return ret;
              }
            });

    return dis;
  }

  @Override
  public long getFilePointer() throws IOException {
    return filePointer;
  }

  @Override
  public long length() throws IOException {
    return fileObject.getContent().getSize();
  }

  @Override
  public void seek(final long pos) throws IOException {
    if (pos == filePointer) {
      // no change
      return;
    }

    if (pos < 0) {
      throw new FileSystemException(
          "vfs.provider/random-access-invalid-position.error", Long.valueOf(pos));
    }
    if (dis != null) {
      close();
    }

    filePointer = pos;
  }
}
