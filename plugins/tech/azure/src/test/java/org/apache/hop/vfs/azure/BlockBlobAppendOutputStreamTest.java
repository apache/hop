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

package org.apache.hop.vfs.azure;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.options.BlockBlobCommitBlockListOptions;
import com.azure.storage.blob.specialized.BlockBlobClient;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;

/**
 * Covers the block-list append behaviour of {@link BlockBlobAppendOutputStream}: the blob's
 * existing committed blocks are kept as-is and the appended bytes are staged as extra blocks, so a
 * single commit yields existing + appended without re-uploading the existing content.
 *
 * <p>Appending has to go through the Blob API because Hop writes these files as block blobs; the
 * Data Lake {@code append} operation rejects them with {@code 409 InvalidAppendOperation}.
 */
class BlockBlobAppendOutputStreamTest {

  private static final String BLOB = "folder/data.txt";
  private static final String ETAG = "\"0x8DB000000000000\"";

  /** Block IDs as the SDK's own block blob writer produces them: base64 of a UUID string. */
  private static final List<String> EXISTING_BLOCKS =
      List.of(
          Base64.getEncoder()
              .encodeToString(
                  "11111111-1111-1111-1111-111111111111".getBytes(StandardCharsets.UTF_8)),
          Base64.getEncoder()
              .encodeToString(
                  "22222222-2222-2222-2222-222222222222".getBytes(StandardCharsets.UTF_8)));

  private static final BlobHttpHeaders HEADERS = new BlobHttpHeaders().setContentType("text/csv");
  private static final Map<String, String> METADATA = Map.of("origin", "hop");

  private BlockBlobClient blob;

  /** Payload of every stageBlock call, in order. */
  private List<byte[]> stagedContent;

  private List<String> stagedIds;

  @BeforeEach
  void setUp() {
    blob = mock(BlockBlobClient.class);
    when(blob.getBlobName()).thenReturn(BLOB);
    stagedContent = new ArrayList<>();
    stagedIds = new ArrayList<>();
    Mockito.doAnswer(
            invocation -> {
              stagedIds.add(invocation.getArgument(0));
              stagedContent.add(
                  readFully(invocation.getArgument(1), invocation.getArgument(2, Long.class)));
              return null;
            })
        .when(blob)
        .stageBlock(anyString(), any(InputStream.class), anyLong());
  }

  private static byte[] readFully(InputStream in, long length) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (long i = 0; i < length; i++) {
      out.write(in.read());
    }
    return out.toByteArray();
  }

  private BlockBlobAppendOutputStream openAppend(List<String> committed, int blockSize) {
    return new BlockBlobAppendOutputStream(blob, committed, HEADERS, METADATA, ETAG, blockSize);
  }

  private BlockBlobCommitBlockListOptions capturedCommit() {
    ArgumentCaptor<BlockBlobCommitBlockListOptions> commit =
        ArgumentCaptor.forClass(BlockBlobCommitBlockListOptions.class);
    verify(blob).commitBlockListWithResponse(commit.capture(), isNull(), isNull());
    return commit.getValue();
  }

  @Test
  void existingBlocksAreKeptAndTheAppendedBytesFollowThem() throws Exception {
    BlockBlobAppendOutputStream out = openAppend(EXISTING_BLOCKS, 1024);
    byte[] payload = "appended".getBytes(StandardCharsets.UTF_8);
    out.write(payload);
    out.close();

    // Only the appended bytes are uploaded; the existing content is referenced by block ID.
    assertEquals(1, stagedContent.size());
    assertArrayEquals(payload, stagedContent.get(0));

    // The committed list is the existing blocks, in order, then the newly staged one.
    List<String> expected = new ArrayList<>(EXISTING_BLOCKS);
    expected.addAll(stagedIds);
    assertEquals(expected, capturedCommit().getBase64BlockIds());
  }

  @Test
  void stagedBlockIdsMatchTheLengthOfTheExistingOnes() throws Exception {
    BlockBlobAppendOutputStream out = openAppend(EXISTING_BLOCKS, 1024);
    out.write("x".getBytes(StandardCharsets.UTF_8));
    out.close();

    // Azure rejects a commit whose block IDs are not all the same length, and the existing IDs came
    // from the SDK's writer, so ours have to be generated the same way.
    assertEquals(1, stagedIds.size());
    assertEquals(
        EXISTING_BLOCKS.get(0).length(),
        stagedIds.get(0).length(),
        "a staged block ID must be the same length as the blob's existing block IDs");
  }

  @Test
  void headersAndMetadataAndETagGuardAreCarriedOnTheCommit() throws Exception {
    BlockBlobAppendOutputStream out = openAppend(EXISTING_BLOCKS, 1024);
    out.write("x".getBytes(StandardCharsets.UTF_8));
    out.close();

    BlockBlobCommitBlockListOptions commit = capturedCommit();
    // Committing a block list resets whatever it is not given, so the blob's own headers and
    // metadata must be replayed or an append would strip them.
    assertEquals("text/csv", commit.getHeaders().getContentType());
    assertEquals(METADATA, commit.getMetadata());
    // And a concurrent modification must fail the append rather than silently drop data.
    assertEquals(ETAG, commit.getRequestConditions().getIfMatch());
  }

  @Test
  void writesBeyondTheBlockSizeAreStagedAsSeveralBlocks() throws Exception {
    int blockSize = 8;
    BlockBlobAppendOutputStream out = openAppend(EXISTING_BLOCKS, blockSize);
    byte[] payload = "0123456789abcdefghij".getBytes(StandardCharsets.UTF_8);
    out.write(payload);
    out.close();

    assertEquals(3, stagedContent.size(), "20 bytes at a block size of 8 must be 3 blocks");
    ByteArrayOutputStream reassembled = new ByteArrayOutputStream();
    for (byte[] staged : stagedContent) {
      reassembled.write(staged);
    }
    assertArrayEquals(payload, reassembled.toByteArray(), "no bytes may be lost or reordered");

    // One commit, listing the existing blocks followed by all three new ones in order.
    List<String> expected = new ArrayList<>(EXISTING_BLOCKS);
    expected.addAll(stagedIds);
    assertEquals(expected, capturedCommit().getBase64BlockIds());
  }

  @Test
  void aBlobWithoutABlockListHasItsContentCarriedForward() throws Exception {
    byte[] existing = "existing;".getBytes(StandardCharsets.UTF_8);
    Mockito.doAnswer(
            invocation -> {
              invocation.getArgument(0, OutputStream.class).write(existing);
              return null;
            })
        .when(blob)
        .downloadStream(any(OutputStream.class));

    // A blob uploaded in a single Put Blob call has no committed blocks to build on.
    BlockBlobAppendOutputStream out = openAppend(List.of(), 1024);
    out.restageExistingContent();
    out.write("appended".getBytes(StandardCharsets.UTF_8));
    out.close();

    // Its content is re-staged ahead of the appended bytes instead of being lost by the commit.
    ByteArrayOutputStream reassembled = new ByteArrayOutputStream();
    for (byte[] staged : stagedContent) {
      reassembled.write(staged);
    }
    assertEquals("existing;appended", reassembled.toString(StandardCharsets.UTF_8));
    assertEquals(stagedIds, capturedCommit().getBase64BlockIds());
  }

  @Test
  void nothingIsCommittedBeforeClose() throws Exception {
    BlockBlobAppendOutputStream out = openAppend(EXISTING_BLOCKS, 1024);
    out.write("streaming".getBytes(StandardCharsets.UTF_8));

    // Staged blocks are not part of the blob until the list is committed.
    verify(blob, never())
        .commitBlockListWithResponse(
            any(BlockBlobCommitBlockListOptions.class), isNull(), isNull());

    out.close();
    verify(blob, times(1))
        .commitBlockListWithResponse(
            any(BlockBlobCommitBlockListOptions.class), isNull(), isNull());
  }

  @Test
  void closingWithoutWritingLeavesTheBlobAlone() throws Exception {
    BlockBlobAppendOutputStream out = openAppend(EXISTING_BLOCKS, 1024);
    out.close();

    verify(blob, never()).stageBlock(anyString(), any(InputStream.class), anyLong());
    verify(blob, never())
        .commitBlockListWithResponse(
            any(BlockBlobCommitBlockListOptions.class), isNull(), isNull());
  }

  @Test
  void closeIsIdempotent() throws Exception {
    BlockBlobAppendOutputStream out = openAppend(EXISTING_BLOCKS, 1024);
    out.write("x".getBytes(StandardCharsets.UTF_8));
    out.close();
    out.close();

    verify(blob, times(1)).stageBlock(anyString(), any(InputStream.class), anyLong());
    verify(blob, times(1))
        .commitBlockListWithResponse(
            any(BlockBlobCommitBlockListOptions.class), isNull(), isNull());
  }

  @Test
  void aFailedCommitIsReportedAsAnIoException() throws Exception {
    when(blob.commitBlockListWithResponse(
            any(BlockBlobCommitBlockListOptions.class), isNull(), isNull()))
        .thenThrow(new BlobStorageException("precondition failed", null, null));

    BlockBlobAppendOutputStream out = openAppend(EXISTING_BLOCKS, 1024);
    out.write("x".getBytes(StandardCharsets.UTF_8));

    IOException thrown = assertThrows(IOException.class, out::close);
    assertTrue(thrown.getMessage().contains(BLOB), "the error should name the blob");
  }

  @Test
  void closingAfterAFailedStageDoesNotReportASecondError() {
    doThrow(new BlobStorageException("boom", null, null))
        .when(blob)
        .stageBlock(anyString(), any(InputStream.class), anyLong());

    // A block size of 4 makes this write big enough to stage mid-stream.
    BlockBlobAppendOutputStream out = openAppend(EXISTING_BLOCKS, 4);
    assertThrows(
        IOException.class,
        () -> out.write("more than four bytes".getBytes(StandardCharsets.UTF_8)));

    // Streams are closed in a finally block, and that close must not bury the real error under a
    // second failure, nor commit a partial block list over the blob.
    assertDoesNotThrow(out::close);
    verify(blob, never())
        .commitBlockListWithResponse(
            any(BlockBlobCommitBlockListOptions.class), isNull(), isNull());
  }

  @Test
  void theBlobIsOnlyReadWhenItHasNoBlockList() throws Exception {
    BlockBlobAppendOutputStream out = openAppend(EXISTING_BLOCKS, 1024);
    out.write("x".getBytes(StandardCharsets.UTF_8));
    out.close();

    // The whole point of the block list is that existing content never travels to the client.
    verify(blob, never()).downloadStream(any(OutputStream.class));

    InOrder inOrder = Mockito.inOrder(blob);
    inOrder.verify(blob).stageBlock(anyString(), any(InputStream.class), anyLong());
    inOrder
        .verify(blob)
        .commitBlockListWithResponse(
            any(BlockBlobCommitBlockListOptions.class), isNull(), isNull());
  }
}
