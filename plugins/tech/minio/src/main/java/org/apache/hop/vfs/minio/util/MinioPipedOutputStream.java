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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.vfs.minio.MinioFileSystem;

/**
 * OutputStream that buffers content and uploads it to MinIO on {@link #close()}.
 *
 * <p>Previously this used a {@link java.io.PipedOutputStream} with the MinIO SDK's unknown-size
 * ({@code objectSize = -1}) multipart path. That combination races with VFS/Text File Output
 * close/flush chains and produced empty objects ({@code Pipe closed} on write). Buffering and
 * uploading with a known content length matches the reliable path used for folder markers.
 *
 * <p>Close is idempotent and post-close flush is a no-op so nested VFS/Text File Output close
 * chains (which flush after closing the inner stream) do not fail the pipeline.
 */
@Getter
@Setter
public class MinioPipedOutputStream extends OutputStream {

  private final ByteArrayOutputStream buffer = new ByteArrayOutputStream(64 * 1024);
  private final MinioFileSystem fileSystem;
  private final String bucketName;
  private final String key;

  private boolean closed;
  private boolean blockedUntilDone = true;

  public MinioPipedOutputStream(MinioFileSystem fileSystem, String bucketName, String key) {
    this.fileSystem = fileSystem;
    this.bucketName = bucketName;
    this.key = key;
  }

  @Override
  public void write(int b) throws IOException {
    // Outer VFS/Text File Output close chains may flush after the first close. Accept late bytes
    // and re-open so a subsequent close() can upload the complete content.
    if (closed) {
      closed = false;
    }
    buffer.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (len == 0) {
      return;
    }
    if (closed) {
      closed = false;
    }
    buffer.write(b, off, len);
  }

  @Override
  public void flush() {
    // Data is held in memory until close(); nothing to flush to the network.
    // No-op when closed so VFS MonitorOutputStream / FilterOutputStream double-close is safe.
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;

    byte[] data = buffer.toByteArray();
    try {
      PutObjectArgs args =
          PutObjectArgs.builder()
              .contentType("application/octet-stream")
              .bucket(bucketName)
              .object(key)
              .stream(new ByteArrayInputStream(data), data.length, -1)
              .build();
      fileSystem.getClient().putObject(args);
      fileSystem.invalidateListCacheForParentOf(bucketName, key);
    } catch (Exception e) {
      throw new IOException("MinIO upload failed for " + bucketName + "/" + key, e);
    } finally {
      buffer.reset();
    }
  }

  public boolean isClosed() {
    return closed;
  }
}
