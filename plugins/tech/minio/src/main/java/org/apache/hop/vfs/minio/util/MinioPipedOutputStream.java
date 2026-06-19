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
import java.io.InterruptedIOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.vfs.minio.MinioFileSystem;

/** Custom OutputStream that enables an output stream onto Minio */
@Getter
@Setter
public class MinioPipedOutputStream extends PipedOutputStream {

  private boolean initialized = false;
  private boolean blockedUntilDone = true;
  private final PipedInputStream pipedInputStream;
  private final MinioAsyncTransferRunner asyncTransferRunner;
  private final MinioFileSystem fileSystem;

  // Daemon-threaded so a wedged putObject (black-holed socket) can never pin the JVM, and the
  // uploader is referenced so close() can join() it interruptibly instead of polling forever.
  private Thread transferThread;
  private volatile Exception transferError;

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
      transferThread = new Thread(asyncTransferRunner, "minio-upload-" + bucketName + "-" + key);
      transferThread.setDaemon(true);
      transferThread.start();
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
  public synchronized void flush() {
    // no flush
  }

  @Override
  public void close() throws IOException {
    // Start upload even when no bytes were written (0-byte file), so PutObject is still executed
    initializeWrite();
    super.close();
    if (initialized && isBlockedUntilDone()) {
      try {
        // join() is interruptible: a pipeline stop interrupts the transform thread and now
        // breaks out of here instead of looping forever on a stalled upload.
        transferThread.join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        transferThread.interrupt();
        throw new InterruptedIOException(
            "Interrupted while finishing the MinIO upload for " + bucketName + "/" + key);
      }
      if (transferError != null) {
        throw new IOException("MinIO upload failed for " + bucketName + "/" + key, transferError);
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
        // Recorded and re-thrown from close() so the failure is surfaced instead of swallowed.
        transferError = e;
      }
    }
  }
}
