/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.vfs.minio.util;

import io.minio.PutObjectArgs;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.vfs.minio.MinioFileSystem;

/** Custom OutputStream that enables an output stream onto Minio */
@Getter
@Setter
public class MinioPipedOutputStream extends PipedOutputStream {
  private static final Class<?> PKG = MinioPipedOutputStream.class;

  private boolean initialized = false;
  private boolean blockedUntilDone = true;
  private final PipedInputStream pipedInputStream;
  private final MinioAsyncTransferRunner asyncTransferRunner;
  private final MinioFileSystem fileSystem;
  private AtomicBoolean result = new AtomicBoolean(false);
  private final String bucketName;
  private final String key;

  public MinioPipedOutputStream(MinioFileSystem fileSystem, String bucketName, String key)
      throws IOException {
    this.pipedInputStream = new PipedInputStream(100000);
    try {
      this.pipedInputStream.connect(this);
    } catch (IOException e) {
      throw new IOException("A MinIO piped output stream could not connect to the input stream", e);
    }

    this.asyncTransferRunner = new MinioAsyncTransferRunner();
    this.bucketName = bucketName;
    this.key = key;
    this.fileSystem = fileSystem;
  }

  private void initializeWrite() {
    if (!initialized) {
      initialized = true;
      new Thread(asyncTransferRunner).start();
    }
  }

  @Override
  public void write(int b) throws IOException {
    initializeWrite();
    super.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    initializeWrite();
    super.write(b, off, len);
  }

  @Override
  public synchronized void flush() throws IOException {
    // super.flush();
  }

  @Override
  public void close() throws IOException {
    super.close();
    if (initialized && isBlockedUntilDone()) {
      while (!result.get()) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          LogChannel.GENERAL.logError(
              BaseMessages.getString(PKG, "ERROR.S3MultiPart.ExceptionCaught"), e);
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  class MinioAsyncTransferRunner implements Runnable {
    @Override
    public void run() {
      try {
        PutObjectArgs args =
            PutObjectArgs.builder()
                .contentType("application/octet-stream")
                .bucket(bucketName)
                .object(key)
                .stream(pipedInputStream, -1, fileSystem.getPartSize())
                .build();
        fileSystem.getClient().putObject(args);
      } catch (Exception e) {
        throw new RuntimeException(
            "Error writing to MinIO bucket " + bucketName + ", object " + key, e);
      } finally {
        // We're done here.
        result.set(true);
      }
    }
  }
}
