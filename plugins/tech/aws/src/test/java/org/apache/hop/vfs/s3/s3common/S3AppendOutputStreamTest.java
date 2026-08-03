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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CopyPartResult;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.UploadPartCopyRequest;
import software.amazon.awssdk.services.s3.model.UploadPartCopyResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

/**
 * Covers the multipart-upload append behaviour of {@link S3AppendOutputStream}: the existing object
 * is brought in as the head of a new multipart upload — server-side copied when it is large enough
 * to be a part, read back when it is not — and the appended bytes follow it before a single
 * completing call swaps the object over.
 */
class S3AppendOutputStreamTest {

  private static final String BUCKET = "my-bucket";
  private static final String KEY = "folder/data.txt";
  private static final String ETAG = "\"abc123\"";
  private static final String UPLOAD_ID = "upload-1";
  private static final int PART_SIZE = S3AppendOutputStream.MIN_PART_SIZE;

  private S3Client s3;
  private S3CommonFileSystem fileSystem;

  /** Payload of every uploadPart call, in order, so we can assert on what was written. */
  private List<byte[]> uploadedParts;

  @BeforeEach
  void setUp() {
    s3 = mock(S3Client.class);
    fileSystem = mock(S3CommonFileSystem.class);
    when(fileSystem.getS3Client()).thenReturn(s3);

    when(s3.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
        .thenReturn(CreateMultipartUploadResponse.builder().uploadId(UPLOAD_ID).build());
    when(s3.uploadPartCopy(any(UploadPartCopyRequest.class)))
        .thenReturn(
            UploadPartCopyResponse.builder()
                .copyPartResult(CopyPartResult.builder().eTag("\"copied\"").build())
                .build());

    uploadedParts = new ArrayList<>();
    when(s3.uploadPart(any(UploadPartRequest.class), any(RequestBody.class)))
        .thenAnswer(
            invocation -> {
              RequestBody body = invocation.getArgument(1);
              uploadedParts.add(readFully(body));
              return UploadPartResponse.builder().eTag("\"part\"").build();
            });
  }

  private static byte[] readFully(RequestBody body) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    body.contentStreamProvider().newStream().transferTo(out);
    return out.toByteArray();
  }

  private S3AppendOutputStream openAppend(long existingSize) {
    return openAppend(HeadObjectResponse.builder().contentLength(existingSize).eTag(ETAG).build());
  }

  private S3AppendOutputStream openAppend(HeadObjectResponse existing) {
    return new S3AppendOutputStream(fileSystem, BUCKET, KEY, PART_SIZE, existing);
  }

  @Test
  void largeExistingObjectIsCopiedServerSideAsTheFirstPart() throws Exception {
    S3AppendOutputStream out = openAppend(10L * 1024 * 1024);
    byte[] payload = "appended".getBytes(StandardCharsets.UTF_8);
    out.write(payload);
    out.close();

    // The existing content becomes part 1 through a copy: it is never downloaded...
    ArgumentCaptor<UploadPartCopyRequest> copy =
        ArgumentCaptor.forClass(UploadPartCopyRequest.class);
    verify(s3).uploadPartCopy(copy.capture());
    assertEquals(BUCKET, copy.getValue().sourceBucket());
    assertEquals(KEY, copy.getValue().sourceKey());
    assertEquals(BUCKET, copy.getValue().destinationBucket());
    assertEquals(KEY, copy.getValue().destinationKey());
    assertEquals(1, copy.getValue().partNumber());
    verify(s3, never()).getObjectAsBytes(any(GetObjectRequest.class));

    // ...and the appended bytes follow it as part 2, so the result is existing + appended.
    ArgumentCaptor<UploadPartRequest> part = ArgumentCaptor.forClass(UploadPartRequest.class);
    verify(s3).uploadPart(part.capture(), any(RequestBody.class));
    assertEquals(2, part.getValue().partNumber());
    assertEquals(UPLOAD_ID, part.getValue().uploadId());
    assertArrayEquals(payload, uploadedParts.get(0));

    verify(s3).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
  }

  @Test
  void smallExistingObjectIsReadBackAheadOfTheAppendedBytes() throws Exception {
    byte[] existing = "existing;".getBytes(StandardCharsets.UTF_8);
    when(s3.getObjectAsBytes(any(GetObjectRequest.class)))
        .thenReturn(ResponseBytes.fromByteArray(GetObjectResponse.builder().build(), existing));

    S3AppendOutputStream out = openAppend(existing.length);
    byte[] payload = "appended".getBytes(StandardCharsets.UTF_8);
    out.write(payload);
    out.close();

    // Below the 5 MiB minimum a part copy is not allowed, so the object is read back instead.
    verify(s3, never()).uploadPartCopy(any(UploadPartCopyRequest.class));
    verify(s3).getObjectAsBytes(any(GetObjectRequest.class));

    // Existing content first, appended content after it, in a single part.
    assertEquals(1, uploadedParts.size());
    assertEquals("existing;appended", new String(uploadedParts.get(0), StandardCharsets.UTF_8));
    verify(s3).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
  }

  @Test
  void bothPathsAreGuardedByTheOpenTimeETag() throws Exception {
    S3AppendOutputStream copyPath = openAppend(10L * 1024 * 1024);
    copyPath.write("x".getBytes(StandardCharsets.UTF_8));
    copyPath.close();

    ArgumentCaptor<UploadPartCopyRequest> copy =
        ArgumentCaptor.forClass(UploadPartCopyRequest.class);
    verify(s3).uploadPartCopy(copy.capture());
    assertEquals(
        ETAG,
        copy.getValue().copySourceIfMatch(),
        "the copy must fail fast if the object changed since the stream opened");

    when(s3.getObjectAsBytes(any(GetObjectRequest.class)))
        .thenReturn(ResponseBytes.fromByteArray(GetObjectResponse.builder().build(), new byte[3]));
    S3AppendOutputStream readPath = openAppend(3L);
    readPath.write("x".getBytes(StandardCharsets.UTF_8));
    readPath.close();

    ArgumentCaptor<GetObjectRequest> get = ArgumentCaptor.forClass(GetObjectRequest.class);
    verify(s3).getObjectAsBytes(get.capture());
    assertEquals(
        ETAG,
        get.getValue().ifMatch(),
        "the read-back must fail fast if the object changed since the stream opened");
  }

  @Test
  void writesBeyondThePartSizeAreSplitIntoParts() throws Exception {
    // One and a half parts of appended data on top of a copied first part.
    byte[] payload = new byte[PART_SIZE + PART_SIZE / 2];
    for (int i = 0; i < payload.length; i++) {
      payload[i] = (byte) (i % 251);
    }

    S3AppendOutputStream out = openAppend(10L * 1024 * 1024);
    out.write(payload);
    out.close();

    assertEquals(2, uploadedParts.size(), "a part and a half must be uploaded as two parts");
    assertEquals(PART_SIZE, uploadedParts.get(0).length, "full parts must be exactly partSize");
    ByteArrayOutputStream reassembled = new ByteArrayOutputStream();
    reassembled.write(uploadedParts.get(0));
    reassembled.write(uploadedParts.get(1));
    assertArrayEquals(payload, reassembled.toByteArray(), "no bytes may be lost or reordered");

    // Parts are numbered after the copied part 1 and in ascending order.
    ArgumentCaptor<UploadPartRequest> parts = ArgumentCaptor.forClass(UploadPartRequest.class);
    verify(s3, times(2)).uploadPart(parts.capture(), any(RequestBody.class));
    assertEquals(
        List.of(2, 3), parts.getAllValues().stream().map(UploadPartRequest::partNumber).toList());
  }

  @Test
  void theMetadataOfTheExistingObjectIsCarriedOver() throws Exception {
    S3AppendOutputStream out =
        openAppend(
            HeadObjectResponse.builder()
                .contentLength(10L * 1024 * 1024)
                .eTag(ETAG)
                .contentType("text/csv")
                .contentEncoding("gzip")
                .metadata(java.util.Map.of("origin", "hop"))
                .build());
    out.write("x".getBytes(StandardCharsets.UTF_8));
    out.close();

    // The append rebuilds the object, so anything it was created with must survive the rewrite.
    ArgumentCaptor<CreateMultipartUploadRequest> create =
        ArgumentCaptor.forClass(CreateMultipartUploadRequest.class);
    verify(s3).createMultipartUpload(create.capture());
    assertEquals("text/csv", create.getValue().contentType());
    assertEquals("gzip", create.getValue().contentEncoding());
    assertEquals(java.util.Map.of("origin", "hop"), create.getValue().metadata());
  }

  @Test
  void nothingIsCommittedBeforeClose() throws Exception {
    S3AppendOutputStream out = openAppend(10L * 1024 * 1024);
    out.write("streaming".getBytes(StandardCharsets.UTF_8));

    verify(s3, never()).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));

    out.close();
    verify(s3, times(1)).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
  }

  @Test
  void closingWithoutWritingLeavesTheObjectAlone() throws Exception {
    S3AppendOutputStream out = openAppend(10L * 1024 * 1024);
    out.close();

    // Opening an append and writing nothing must not start an upload, let alone rewrite the object.
    verify(s3, never()).createMultipartUpload(any(CreateMultipartUploadRequest.class));
    verify(s3, never()).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
  }

  @Test
  void closeIsIdempotent() throws Exception {
    S3AppendOutputStream out = openAppend(10L * 1024 * 1024);
    out.write("x".getBytes(StandardCharsets.UTF_8));
    out.close();
    out.close();

    // A second close must not upload or complete a second time.
    verify(s3, times(1)).uploadPart(any(UploadPartRequest.class), any(RequestBody.class));
    verify(s3, times(1)).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
  }

  @Test
  void aFailedCopyAbortsTheUploadAndIsReportedAsAnIoException() {
    when(s3.uploadPartCopy(any(UploadPartCopyRequest.class)))
        .thenThrow(S3Exception.builder().message("precondition failed").statusCode(412).build());

    S3AppendOutputStream out = openAppend(10L * 1024 * 1024);
    IOException thrown =
        assertThrows(IOException.class, () -> out.write("x".getBytes(StandardCharsets.UTF_8)));
    assertTrue(thrown.getMessage().contains(KEY), "the error should name the target object");

    // A half-started multipart upload keeps billing for its parts, so it must be abandoned.
    verify(s3).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
  }

  @Test
  void closingAfterAFailedWriteDoesNotReportASecondError() {
    when(s3.uploadPartCopy(any(UploadPartCopyRequest.class)))
        .thenThrow(S3Exception.builder().message("precondition failed").statusCode(412).build());

    S3AppendOutputStream out = openAppend(10L * 1024 * 1024);
    assertThrows(IOException.class, () -> out.write("x".getBytes(StandardCharsets.UTF_8)));

    // Streams are closed in a finally block, and that close must not bury the real error under a
    // second failure, nor try to complete an upload that was already abandoned.
    assertDoesNotThrow(out::close);
    verify(s3, never()).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
    verify(s3, times(1)).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
  }

  @Test
  void aFailedCompletionAbortsTheUploadAndIsReportedAsAnIoException() throws Exception {
    when(s3.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
        .thenThrow(S3Exception.builder().message("boom").statusCode(500).build());

    S3AppendOutputStream out = openAppend(10L * 1024 * 1024);
    out.write("x".getBytes(StandardCharsets.UTF_8));

    IOException thrown = assertThrows(IOException.class, out::close);
    assertTrue(thrown.getMessage().contains(KEY), "the error should name the target object");

    InOrder inOrder = Mockito.inOrder(s3);
    inOrder.verify(s3).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
    inOrder.verify(s3).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
  }
}
