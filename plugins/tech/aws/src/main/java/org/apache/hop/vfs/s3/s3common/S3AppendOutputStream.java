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

package org.apache.hop.vfs.s3.s3common;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hop.core.logging.LogChannel;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.UploadPartCopyRequest;
import software.amazon.awssdk.services.s3.model.UploadPartCopyResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

/**
 * Append output stream for S3, built on <a
 * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html">multipart
 * uploads</a>.
 *
 * <p>S3 objects are immutable and the API has no append operation, so an append is expressed as a
 * multipart upload that reassembles the object as {@code target = existing + appended bytes}. The
 * existing content becomes the first part through {@code UploadPartCopy}, which is a server-side
 * copy: the old content is never downloaded or re-uploaded, so appending a few rows to a large
 * object only transfers those rows. The appended bytes follow as one or more regular parts, and a
 * single {@code CompleteMultipartUpload} on {@link #close()} swaps the object over atomically.
 *
 * <p>S3 requires every part but the last to be at least {@value #MIN_PART_SIZE} bytes, so an
 * existing object smaller than that cannot be copied as part 1. Those objects are read back and
 * placed in front of the appended bytes instead — bounded by the same 5 MiB, so the fallback stays
 * cheap.
 *
 * <p>Both paths are guarded by an {@code If-Match} on the ETag captured when the stream was opened,
 * so a concurrent modification of the object fails the append fast instead of silently dropping
 * data.
 *
 * <p>The multipart upload is only started on the first {@code write(...)} and nothing is visible
 * until the completing call, so an append that writes nothing, or a pipeline that dies mid-write,
 * leaves the object exactly as it was.
 */
public class S3AppendOutputStream extends OutputStream {

  /**
   * Minimum size of every part but the last, see <a
   * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html">S3 multipart upload
   * limits</a>. An existing object below this size cannot be copied in as part 1.
   */
  public static final int MIN_PART_SIZE = 5 * 1024 * 1024;

  private final S3CommonFileSystem fileSystem;
  private final String bucketName;
  private final String key;
  private final int partSize;

  /** The object as it was when the stream opened: its size, ETag and metadata to carry over. */
  private final HeadObjectResponse existing;

  /** Appended bytes waiting to be uploaded; flushed out whenever it reaches {@link #partSize}. */
  private final ByteArrayOutputStream buffer;

  private final List<CompletedPart> completedParts = new ArrayList<>();
  private String uploadId;
  private int partNumber = 1;
  private boolean started = false;
  private boolean closed = false;

  /** Set once the upload has been aborted, so a follow-up close does not report a second error. */
  private boolean failed = false;

  public S3AppendOutputStream(
      S3CommonFileSystem fileSystem,
      String bucketName,
      String key,
      int partSize,
      HeadObjectResponse existing) {
    this.fileSystem = fileSystem;
    this.bucketName = bucketName;
    this.key = key;
    this.partSize = Math.max(partSize, MIN_PART_SIZE);
    this.existing = existing;
    this.buffer = new ByteArrayOutputStream(this.partSize);
  }

  @Override
  public void write(int b) throws IOException {
    start();
    buffer.write(b);
    uploadFullParts();
  }

  @Override
  public void write(byte[] buf, int off, int len) throws IOException {
    start();
    buffer.write(buf, off, len);
    uploadFullParts();
  }

  /**
   * Open the multipart upload and seed it with the existing content, either as a server-side copied
   * first part or, when the object is too small for a part, as the head of the buffer. Deferred to
   * the first write so that an append writing nothing leaves the object untouched.
   */
  private void start() throws IOException {
    if (started) {
      return;
    }
    started = true;
    S3Client s3 = fileSystem.getS3Client();
    try {
      // Rebuilding the object must not silently drop the metadata it was created with.
      uploadId =
          s3.createMultipartUpload(
                  CreateMultipartUploadRequest.builder()
                      .bucket(bucketName)
                      .key(key)
                      .contentType(existing.contentType())
                      .contentEncoding(existing.contentEncoding())
                      .metadata(existing.metadata())
                      .build())
              .uploadId();

      if (existing.contentLength() >= MIN_PART_SIZE) {
        copyExistingObjectAsFirstPart(s3);
      } else {
        readExistingObjectIntoBuffer(s3);
      }
    } catch (RuntimeException e) {
      failed = true;
      abortQuietly();
      throw new IOException(unableToAppend(), e);
    }
  }

  /** Server-side copy of the whole existing object into part 1 — no bytes travel to the client. */
  private void copyExistingObjectAsFirstPart(S3Client s3) {
    UploadPartCopyResponse copy =
        s3.uploadPartCopy(
            UploadPartCopyRequest.builder()
                .sourceBucket(bucketName)
                .sourceKey(key)
                .destinationBucket(bucketName)
                .destinationKey(key)
                .uploadId(uploadId)
                .partNumber(partNumber)
                .copySourceIfMatch(existing.eTag())
                .build());
    completedParts.add(
        CompletedPart.builder().eTag(copy.copyPartResult().eTag()).partNumber(partNumber).build());
    partNumber++;
  }

  /**
   * The existing object is below the minimum part size, so it cannot be copied as a part. Read it
   * back and let the appended bytes follow it in the buffer; it is under 5 MiB by definition.
   */
  private void readExistingObjectIntoBuffer(S3Client s3) {
    ResponseBytes<GetObjectResponse> content =
        s3.getObjectAsBytes(
            GetObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .ifMatch(existing.eTag())
                .build());
    buffer.writeBytes(content.asByteArray());
  }

  private void uploadFullParts() throws IOException {
    while (buffer.size() >= partSize) {
      byte[] bytes = buffer.toByteArray();
      buffer.reset();
      uploadPart(bytes, partSize);
      // Whatever overshot the part size is the start of the next one.
      buffer.write(bytes, partSize, bytes.length - partSize);
    }
  }

  /** Upload the first {@code len} bytes of {@code bytes} as the next part of the upload. */
  private void uploadPart(byte[] bytes, int len) throws IOException {
    byte[] part = len == bytes.length ? bytes : Arrays.copyOf(bytes, len);
    try {
      UploadPartResponse response =
          fileSystem
              .getS3Client()
              .uploadPart(
                  UploadPartRequest.builder()
                      .bucket(bucketName)
                      .key(key)
                      .uploadId(uploadId)
                      .partNumber(partNumber)
                      .build(),
                  RequestBody.fromBytes(part));
      completedParts.add(
          CompletedPart.builder().eTag(response.eTag()).partNumber(partNumber).build());
      partNumber++;
    } catch (RuntimeException e) {
      failed = true;
      abortQuietly();
      throw new IOException(unableToAppend(), e);
    }
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    if (!started || failed) {
      // Nothing was appended, or the upload already failed and was aborted: either way the object
      // stays exactly as it was and there is nothing left to complete.
      return;
    }
    try {
      if (buffer.size() > 0) {
        byte[] last = buffer.toByteArray();
        buffer.reset();
        uploadPart(last, last.length);
      }
      fileSystem
          .getS3Client()
          .completeMultipartUpload(
              CompleteMultipartUploadRequest.builder()
                  .bucket(bucketName)
                  .key(key)
                  .uploadId(uploadId)
                  .multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build())
                  .build());
      fileSystem.invalidateListCacheForParentOf(bucketName, key);
    } catch (RuntimeException e) {
      failed = true;
      abortQuietly();
      throw new IOException(unableToAppend(), e);
    }
  }

  /**
   * Abandon the multipart upload so its parts do not linger as billed, invisible storage. A failed
   * abort only leaves those parts behind, so it is logged rather than masking the original error.
   */
  private void abortQuietly() {
    if (uploadId == null) {
      return;
    }
    try {
      fileSystem
          .getS3Client()
          .abortMultipartUpload(
              AbortMultipartUploadRequest.builder()
                  .bucket(bucketName)
                  .key(key)
                  .uploadId(uploadId)
                  .build());
    } catch (RuntimeException e) {
      LogChannel.GENERAL.logError(
          "Unable to abort the multipart upload used to append to s3://" + bucketName + "/" + key,
          e);
    } finally {
      uploadId = null;
    }
  }

  private String unableToAppend() {
    return "Unable to append to s3://" + bucketName + "/" + key;
  }
}
