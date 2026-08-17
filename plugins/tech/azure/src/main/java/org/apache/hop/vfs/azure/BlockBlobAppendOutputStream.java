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

import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.Block;
import com.azure.storage.blob.models.BlockListType;
import com.azure.storage.blob.options.BlockBlobCommitBlockListOptions;
import com.azure.storage.blob.specialized.BlockBlobClient;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.hop.core.logging.LogChannel;

/**
 * Append output stream for Azure block blobs.
 *
 * <p>Hop writes Azure files through {@code DataLakeFileClient.getOutputStream()}, which is a block
 * blob written with the <em>Blob</em> API. The Data Lake {@code append} operation refuses those
 * files outright — {@code 409 InvalidAppendOperation, "The resource was created or modified by the
 * Azure Blob Service API and cannot be appended to by the Azure Data Lake Storage Service API"} —
 * so appending has to speak the same API that wrote the file.
 *
 * <p>A block blob is a list of blocks, so appending is: keep the blob's existing committed block
 * IDs, stage the new bytes as extra blocks, and commit the old IDs followed by the new ones. The
 * existing blocks are only referenced by ID and are never re-uploaded, so appending a few rows to a
 * large blob costs only the size of those rows.
 *
 * <p>Block IDs are generated exactly as the SDK's own block blob output stream generates them —
 * base64 of a random UUID string — because Azure requires every block ID within one blob to be the
 * same length, and the existing IDs came from that writer.
 *
 * <p>The commit replays the blob's headers and metadata, since committing a block list otherwise
 * resets them, and is guarded by an {@code If-Match} on the ETag captured when the stream was
 * opened so a concurrent modification fails the append instead of silently dropping data.
 *
 * <p>Nothing is visible until {@link #close()}: staged but uncommitted blocks are not part of the
 * blob, so a pipeline that dies mid-write leaves the blob with its original content.
 */
public class BlockBlobAppendOutputStream extends OutputStream {

  /** Matches the block size the SDK's own block blob output stream uses for buffered writes. */
  public static final int DEFAULT_BLOCK_SIZE = 4 * 1024 * 1024;

  private final BlockBlobClient blob;
  private final BlobHttpHeaders headers;
  private final Map<String, String> metadata;
  private final String eTag;

  /** The blob's existing committed block IDs, followed by the ones staged by this stream. */
  private final List<String> blockIds;

  private final byte[] block;
  private int buffered;

  private boolean staged = false;
  private boolean failed = false;
  private boolean closed = false;

  public BlockBlobAppendOutputStream(
      BlockBlobClient blob,
      List<String> committedBlockIds,
      BlobHttpHeaders headers,
      Map<String, String> metadata,
      String eTag) {
    this(blob, committedBlockIds, headers, metadata, eTag, DEFAULT_BLOCK_SIZE);
  }

  public BlockBlobAppendOutputStream(
      BlockBlobClient blob,
      List<String> committedBlockIds,
      BlobHttpHeaders headers,
      Map<String, String> metadata,
      String eTag,
      int blockSize) {
    this.blob = blob;
    this.blockIds = new ArrayList<>(committedBlockIds);
    this.headers = headers;
    this.metadata = metadata;
    this.eTag = eTag;
    this.block = new byte[blockSize];
  }

  /** The committed block IDs of an existing blob, in order. */
  public static List<String> committedBlockIds(BlockBlobClient blob) {
    return blob.listBlocks(BlockListType.COMMITTED).getCommittedBlocks().stream()
        .map(Block::getName)
        .toList();
  }

  @Override
  public void write(int b) throws IOException {
    if (buffered == block.length) {
      stageBufferedBlock();
    }
    block[buffered++] = (byte) b;
  }

  @Override
  public void write(byte[] buf, int off, int len) throws IOException {
    int remaining = len;
    int offset = off;
    while (remaining > 0) {
      if (buffered == block.length) {
        stageBufferedBlock();
      }
      int copied = Math.min(remaining, block.length - buffered);
      System.arraycopy(buf, offset, block, buffered, copied);
      buffered += copied;
      offset += copied;
      remaining -= copied;
    }
  }

  /**
   * Stage the buffered bytes as one new block. A staged block is not part of the blob until the
   * block list is committed.
   */
  private void stageBufferedBlock() throws IOException {
    if (buffered == 0) {
      return;
    }
    String blockId = newBlockId();
    try {
      blob.stageBlock(blockId, new ByteArrayInputStream(block, 0, buffered), buffered);
    } catch (RuntimeException e) {
      failed = true;
      throw new IOException(
          "Unable to stage " + buffered + " appended bytes for " + blob.getBlobName(), e);
    }
    blockIds.add(blockId);
    buffered = 0;
    staged = true;
  }

  /**
   * Block IDs must be the same length for every block in a blob. This mirrors the SDK's own block
   * blob writer — base64 of a random UUID string — so IDs staged here line up with the ones already
   * on the blob.
   */
  private static String newBlockId() {
    return Base64.getEncoder()
        .encodeToString(UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Copy the blob's current content into freshly staged blocks. Only needed when the blob has no
   * committed block list of its own — a blob uploaded in a single {@code Put Blob} call has none,
   * and its content would be lost by a commit that only lists our new blocks.
   */
  void restageExistingContent() throws IOException {
    LogChannel.GENERAL.logBasic(
        "Azure blob "
            + blob.getBlobName()
            + " has no committed block list (it was uploaded in one request), so its content is"
            + " copied forward to append to it.");
    try {
      // Downloading through this stream's own write path chunks the content into blocks exactly
      // like appended bytes, and leaves the tail buffered for the appended data to follow.
      blob.downloadStream(this);
    } catch (RuntimeException e) {
      failed = true;
      throw new IOException("Unable to read the current content of " + blob.getBlobName(), e);
    }
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    if (failed) {
      // A staging call already failed and reported itself. Staged blocks are not part of the blob,
      // so it keeps its original content and there is nothing to commit.
      return;
    }
    stageBufferedBlock();
    if (!staged) {
      // Nothing was written, so the blob must stay exactly as it was.
      return;
    }
    try {
      blob.commitBlockListWithResponse(
          new BlockBlobCommitBlockListOptions(blockIds)
              .setHeaders(headers)
              .setMetadata(metadata)
              .setRequestConditions(new BlobRequestConditions().setIfMatch(eTag)),
          null,
          null);
    } catch (RuntimeException e) {
      throw new IOException("Unable to commit the appended blocks to " + blob.getBlobName(), e);
    }
  }
}
