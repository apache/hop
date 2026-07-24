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
 *
 */

package org.apache.hop.vfs.gs;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobTargetOption;
import com.google.cloud.storage.Storage.ComposeRequest;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.hop.core.logging.LogChannel;

/**
 * Append output stream for Google Cloud Storage, built on <a
 * href="https://cloud.google.com/storage/docs/composite-objects#appends">composite objects</a>.
 *
 * <p>GCS objects are immutable, so there is no true in-place append. Instead the bytes written to
 * this stream are streamed into a short-lived <em>temporary</em> object, and on {@link #close()}
 * the target and the temporary object are concatenated back onto the target with a single {@code
 * compose} call ({@code target = target + temp}). The temporary object is then deleted.
 *
 * <p>The compose is guarded with an {@code ifGenerationMatch} precondition captured when the stream
 * was opened, so a concurrent modification of the target fails the append fast instead of silently
 * dropping data. One open/close cycle performs exactly one compose regardless of how many times
 * {@code write(...)} is called, so a transform that appends many rows still costs a single compose.
 *
 * <p>Note that composite objects have no MD5 metadata (CRC32C is still maintained and validated by
 * the compose operation).
 */
public class ComposeAppendOutputStream extends OutputStream {

  private final Storage storage;
  private final String bucketName;
  private final String targetName;
  private final String tempName;
  private final long targetGeneration;

  /** Streams the appended bytes into the temporary object; reused for its robust write/close. */
  private final WriteChannelOutputStream tempStream;

  private boolean closed = false;

  public ComposeAppendOutputStream(
      Storage storage,
      String bucketName,
      String targetName,
      String tempName,
      long targetGeneration,
      WriteChannel tempChannel) {
    this.storage = storage;
    this.bucketName = bucketName;
    this.targetName = targetName;
    this.tempName = tempName;
    this.targetGeneration = targetGeneration;
    this.tempStream = new WriteChannelOutputStream(tempChannel);
  }

  @Override
  public void write(int b) throws IOException {
    tempStream.write(b);
  }

  @Override
  public void write(byte[] buf, int off, int len) throws IOException {
    tempStream.write(buf, off, len);
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    try {
      // Finish streaming the appended bytes into the temporary object.
      tempStream.close();
      // Concatenate: target = target + temp. The generation precondition makes a concurrent
      // modification of the target fail here rather than silently lose the appended data.
      storage.compose(
          ComposeRequest.newBuilder()
              .addSource(targetName)
              .addSource(tempName)
              .setTarget(BlobInfo.newBuilder(bucketName, targetName).build())
              .setTargetOptions(BlobTargetOption.generationMatch(targetGeneration))
              .build());
    } catch (RuntimeException e) {
      throw new IOException(
          "Unable to append to gs://" + bucketName + "/" + targetName + " using compose", e);
    } finally {
      deleteTempQuietly();
    }
  }

  /** The temporary object is throwaway; a failed delete only leaves a stray object, so log it. */
  private void deleteTempQuietly() {
    try {
      storage.delete(BlobId.of(bucketName, tempName));
    } catch (RuntimeException e) {
      LogChannel.GENERAL.logError(
          "Unable to delete temporary append object gs://" + bucketName + "/" + tempName, e);
    }
  }
}
