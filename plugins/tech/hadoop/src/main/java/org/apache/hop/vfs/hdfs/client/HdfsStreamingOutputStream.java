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
package org.apache.hop.vfs.hdfs.client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.vfs.hdfs.HdfsTransport;

/**
 * Pipes bytes into a WebHDFS/HttpFS PUT so Parquet (and other) writers never buffer the whole file
 * in heap. Close waits for the upload. A flush after close is a no-op so nested VFS close chains do
 * not fail.
 */
public class HdfsStreamingOutputStream extends OutputStream {
  private static final Class<?> PKG = HdfsTransport.class;
  private static final int PIPE_BUFFER = 1024 * 1024;

  @FunctionalInterface
  public interface Uploader {
    void upload(InputStream body) throws IOException;
  }

  private final PipedOutputStream pipe;
  private final Future<Void> upload;
  private final String path;
  private volatile IOException uploadError;
  private boolean closed;

  public static HdfsStreamingOutputStream start(
      ExecutorService executor, Uploader uploader, String path) throws IOException {
    PipedInputStream in = new PipedInputStream(PIPE_BUFFER);
    PipedOutputStream out = new PipedOutputStream(in);
    HdfsStreamingOutputStream stream =
        new HdfsStreamingOutputStream(out, executor, uploader, in, path);
    return stream;
  }

  private HdfsStreamingOutputStream(
      PipedOutputStream pipe,
      ExecutorService executor,
      Uploader uploader,
      PipedInputStream in,
      String path) {
    this.pipe = pipe;
    this.path = path;
    this.upload =
        executor.submit(
            () -> {
              try (PipedInputStream body = in) {
                uploader.upload(body);
              } catch (IOException e) {
                uploadError = e;
                throw e;
              }
              return null;
            });
  }

  @Override
  public void write(int b) throws IOException {
    checkUpload();
    pipe.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (len == 0) {
      return;
    }
    checkUpload();
    pipe.write(b, off, len);
  }

  @Override
  public void flush() throws IOException {
    if (closed) {
      return;
    }
    checkUpload();
    pipe.flush();
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    try {
      pipe.close();
    } finally {
      try {
        upload.get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(BaseMessages.getString(PKG, "Hdfs.Error.UploadFailed", path), e);
      } catch (ExecutionException e) {
        Throwable cause = e.getCause() != null ? e.getCause() : e;
        if (cause instanceof IOException io) {
          throw io;
        }
        throw new IOException(BaseMessages.getString(PKG, "Hdfs.Error.UploadFailed", path), cause);
      }
    }
    if (uploadError != null) {
      throw uploadError;
    }
  }

  private void checkUpload() throws IOException {
    if (uploadError != null) {
      throw uploadError;
    }
    if (upload.isDone()) {
      try {
        upload.get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      } catch (ExecutionException e) {
        Throwable cause = e.getCause() != null ? e.getCause() : e;
        if (cause instanceof IOException io) {
          throw io;
        }
        throw new IOException(cause);
      }
    }
  }
}
