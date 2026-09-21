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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.vfs.hdfs.HdfsTransport;

/**
 * Pipes bytes into a WebHDFS/HttpFS PUT so Parquet (and other) writers never buffer the whole file
 * in heap. Close waits for the upload. A flush after close is a no-op so nested VFS close chains do
 * not fail.
 */
public class HdfsStreamingOutputStream extends OutputStream {
  private static final Class<?> PKG = HdfsTransport.class;
  private static final int PIPE_BUFFER = 8 * 1024 * 1024;
  static final long CLOSE_TIMEOUT_SECONDS = 120;

  @FunctionalInterface
  public interface Uploader {
    void upload(InputStream body, HdfsStreamingOutputStream stream) throws IOException;
  }

  private final PipedOutputStream pipe;
  private final String path;
  private final long closeTimeoutSeconds;
  private Future<Void> upload;
  private volatile HttpUriRequestBase inflight;
  private volatile IOException uploadError;
  private boolean closed;

  public static HdfsStreamingOutputStream start(
      ExecutorService executor, Uploader uploader, String path) throws IOException {
    return start(executor, uploader, path, CLOSE_TIMEOUT_SECONDS);
  }

  static HdfsStreamingOutputStream start(
      ExecutorService executor, Uploader uploader, String path, long closeTimeoutSeconds)
      throws IOException {
    PipedInputStream in = new PipedInputStream(PIPE_BUFFER);
    PipedOutputStream out = new PipedOutputStream(in);
    HdfsStreamingOutputStream stream =
        new HdfsStreamingOutputStream(out, path, closeTimeoutSeconds);
    stream.upload =
        executor.submit(
            () -> {
              try (PipedInputStream body = in) {
                uploader.upload(body, stream);
              } catch (IOException e) {
                stream.uploadError = e;
                throw e;
              }
              return null;
            });
    return stream;
  }

  private HdfsStreamingOutputStream(PipedOutputStream pipe, String path, long closeTimeoutSeconds) {
    this.pipe = pipe;
    this.path = path;
    this.closeTimeoutSeconds = closeTimeoutSeconds;
  }

  void watch(HttpUriRequestBase request) {
    this.inflight = request;
  }

  private void abortInflight() {
    HttpUriRequestBase request = inflight;
    if (request != null) {
      request.cancel();
    }
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
      awaitUpload();
    }
  }

  private void awaitUpload() throws IOException {
    try {
      upload.get(closeTimeoutSeconds, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      abortInflight();
      upload.cancel(true);
      throw new IOException(BaseMessages.getString(PKG, "Hdfs.Error.UploadTimedOut", path), e);
    } catch (InterruptedException e) {
      abortInflight();
      upload.cancel(true);
      Thread.currentThread().interrupt();
      throw new IOException(BaseMessages.getString(PKG, "Hdfs.Error.UploadFailed", path), e);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause() != null ? e.getCause() : e;
      if (cause instanceof IOException io) {
        throw io;
      }
      throw new IOException(BaseMessages.getString(PKG, "Hdfs.Error.UploadFailed", path), cause);
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
