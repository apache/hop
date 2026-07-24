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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobTargetOption;
import com.google.cloud.storage.Storage.ComposeRequest;
import com.google.cloud.storage.StorageException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;

/**
 * Covers the composite-object append behaviour of {@link ComposeAppendOutputStream}: bytes are
 * streamed into a temporary object and, on close, the target and temp object are concatenated with
 * a single generation-guarded {@code compose}, after which the temp object is deleted.
 */
class ComposeAppendOutputStreamTest {

  private static final String BUCKET = "my-bucket";
  private static final String TARGET = "folder/data.txt";
  private static final String TEMP = "folder/data.txt.hop-append-abc.tmp";
  private static final long GENERATION = 42L;

  /** A WriteChannel mock that consumes every byte offered, like a healthy upload. */
  private static WriteChannel consumingChannel() {
    WriteChannel channel = mock(WriteChannel.class);
    try {
      when(channel.write(any(ByteBuffer.class)))
          .thenAnswer(
              inv -> {
                ByteBuffer b = inv.getArgument(0);
                int remaining = b.remaining();
                b.position(b.limit());
                return remaining;
              });
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    return channel;
  }

  @Test
  void closeStreamsToTempThenComposesThenDeletesTemp() throws Exception {
    Storage storage = mock(Storage.class);
    WriteChannel tempChannel = consumingChannel();

    ComposeAppendOutputStream out =
        new ComposeAppendOutputStream(storage, BUCKET, TARGET, TEMP, GENERATION, tempChannel);
    out.write("appended".getBytes(StandardCharsets.UTF_8));
    out.close();

    // The appended bytes were streamed into the temp object and the channel closed.
    verify(tempChannel).close();

    ArgumentCaptor<ComposeRequest> captor = ArgumentCaptor.forClass(ComposeRequest.class);
    verify(storage).compose(captor.capture());
    ComposeRequest request = captor.getValue();

    // Sources are, in order, the existing target then the temp object (target = target + temp).
    assertEquals(2, request.getSourceBlobs().size());
    assertEquals(TARGET, request.getSourceBlobs().get(0).getName());
    assertEquals(TEMP, request.getSourceBlobs().get(1).getName());

    // The composite is written back onto the target.
    assertEquals(BUCKET, request.getTarget().getBucket());
    assertEquals(TARGET, request.getTarget().getName());

    // Guarded by the generation captured when the stream opened.
    assertTrue(
        request.getTargetOptions().contains(BlobTargetOption.generationMatch(GENERATION)),
        "append must be guarded by ifGenerationMatch to fail fast on concurrent modification");

    // And the throwaway temp object is cleaned up, after the compose.
    InOrder inOrder = Mockito.inOrder(storage);
    inOrder.verify(storage).compose(any(ComposeRequest.class));
    inOrder.verify(storage).delete(BlobId.of(BUCKET, TEMP));
  }

  @Test
  void composeFailurePropagatesButTempIsStillDeleted() throws Exception {
    Storage storage = mock(Storage.class);
    when(storage.compose(any(ComposeRequest.class)))
        .thenThrow(new StorageException(412, "precondition failed"));
    WriteChannel tempChannel = consumingChannel();

    ComposeAppendOutputStream out =
        new ComposeAppendOutputStream(storage, BUCKET, TARGET, TEMP, GENERATION, tempChannel);
    out.write("x".getBytes(StandardCharsets.UTF_8));

    IOException thrown = assertThrows(IOException.class, out::close);
    assertTrue(thrown.getMessage().contains(TARGET), "the error should name the target object");
    // Even on a failed compose the temp object must not be leaked.
    verify(storage).delete(BlobId.of(BUCKET, TEMP));
  }

  @Test
  void closeIsIdempotent() throws Exception {
    Storage storage = mock(Storage.class);
    WriteChannel tempChannel = consumingChannel();

    ComposeAppendOutputStream out =
        new ComposeAppendOutputStream(storage, BUCKET, TARGET, TEMP, GENERATION, tempChannel);
    out.close();
    out.close();

    // A second close must not compose (or delete) again.
    verify(storage, times(1)).compose(any(ComposeRequest.class));
    verify(storage, times(1)).delete(eq(BlobId.of(BUCKET, TEMP)));
  }

  @Test
  void nothingIsComposedBeforeClose() throws Exception {
    Storage storage = mock(Storage.class);
    WriteChannel tempChannel = consumingChannel();

    ComposeAppendOutputStream out =
        new ComposeAppendOutputStream(storage, BUCKET, TARGET, TEMP, GENERATION, tempChannel);
    out.write("streaming".getBytes(StandardCharsets.UTF_8));

    // Writing alone must not compose; the concatenation happens exactly once, at close.
    verify(storage, never()).compose(any(ComposeRequest.class));

    out.close();
    verify(storage, times(1)).compose(any(ComposeRequest.class));
  }
}
