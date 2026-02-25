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

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.StorageUnitConverter;
import org.apache.hop.i18n.BaseMessages;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

/** Custom OutputStream that enables chunked uploads into S3 using AWS SDK v2. */
public class S3CommonPipedOutputStream extends PipedOutputStream {

  private static final Class<?> PKG = S3CommonPipedOutputStream.class;

  private static final int DEFAULT_PART_SIZE = 5 * 1024 * 1024;

  private final ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
  private boolean initialized = false;
  @Getter @Setter private boolean blockedUntilDone = true;
  private final PipedInputStream pipedInputStream;
  private final S3AsyncTransferRunner s3AsyncTransferRunner;
  private final S3CommonFileSystem fileSystem;
  private Future<Boolean> result = null;
  private final String bucketId;
  private final String key;
  private final int partSize;

  public S3CommonPipedOutputStream(S3CommonFileSystem fileSystem, String bucketId, String key)
      throws IOException {
    this(fileSystem, bucketId, key, DEFAULT_PART_SIZE);
  }

  public S3CommonPipedOutputStream(
      S3CommonFileSystem fileSystem, String bucketId, String key, int partSize) throws IOException {
    this.pipedInputStream = new PipedInputStream();
    try {
      this.pipedInputStream.connect(this);
    } catch (IOException e) {
      throw new IOException("could not connect to pipedInputStream", e);
    }
    this.s3AsyncTransferRunner = new S3AsyncTransferRunner();
    this.bucketId = bucketId;
    this.key = key;
    this.fileSystem = fileSystem;
    this.partSize = partSize;
  }

  private void initializeWrite() {
    if (!initialized) {
      initialized = true;
      result = executor.submit(s3AsyncTransferRunner);
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
  public void close() throws IOException {
    super.close();
    if (initialized && isBlockedUntilDone()) {
      while (!result.isDone()) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          LogChannel.GENERAL.logError(
              BaseMessages.getString(PKG, "ERROR.S3MultiPart.ExceptionCaught"), e);
          Thread.currentThread().interrupt();
        }
      }
    }
    executor.shutdown();
  }

  class S3AsyncTransferRunner implements Callable<Boolean> {

    @Override
    public Boolean call() throws Exception {
      boolean returnVal = true;
      List<CompletedPart> completedParts = new ArrayList<>();
      String uploadId = null;
      S3Client s3 = fileSystem.getS3Client();

      try (ByteArrayOutputStream baos = new ByteArrayOutputStream(partSize);
          BufferedInputStream bis = new BufferedInputStream(pipedInputStream, partSize)) {

        CreateMultipartUploadResponse createResponse =
            s3.createMultipartUpload(
                CreateMultipartUploadRequest.builder().bucket(bucketId).key(key).build());
        uploadId = createResponse.uploadId();

        byte[] tmpBuffer = new byte[partSize];
        int read;
        long offset = 0;
        long totalRead = 0;
        int partNum = 1;

        LogChannel.GENERAL.logDetailed(BaseMessages.getString(PKG, "INFO.S3MultiPart.Start"));
        while ((read = bis.read(tmpBuffer)) >= 0) {
          if (read > 0) {
            baos.write(tmpBuffer, 0, read);
            totalRead += read;
          }

          if (totalRead >= partSize) {
            byte[] partBytes = baos.toByteArray();
            UploadPartResponse uploadResp =
                s3.uploadPart(
                    UploadPartRequest.builder()
                        .bucket(bucketId)
                        .key(key)
                        .uploadId(uploadId)
                        .partNumber(partNum++)
                        .build(),
                    RequestBody.fromBytes(partBytes));
            completedParts.add(
                CompletedPart.builder().eTag(uploadResp.eTag()).partNumber(partNum - 1).build());
            LogChannel.GENERAL.logDetailed(
                BaseMessages.getString(
                    PKG, "INFO.S3MultiPart.Upload", partNum - 1, offset, Long.toString(totalRead)));
            offset += totalRead;
            totalRead = 0;
            baos.reset();
          }
        }

        if (totalRead > 0 || partNum == 1) {
          byte[] lastPart = baos.toByteArray();
          UploadPartResponse uploadResp =
              s3.uploadPart(
                  UploadPartRequest.builder()
                      .bucket(bucketId)
                      .key(key)
                      .uploadId(uploadId)
                      .partNumber(partNum)
                      .build(),
                  RequestBody.fromBytes(lastPart));
          completedParts.add(
              CompletedPart.builder().eTag(uploadResp.eTag()).partNumber(partNum).build());
          LogChannel.GENERAL.logDetailed(
              BaseMessages.getString(PKG, "INFO.S3MultiPart.Upload", partNum, offset, totalRead));
        }

        LogChannel.GENERAL.logDetailed(BaseMessages.getString(PKG, "INFO.S3MultiPart.Complete"));
        s3.completeMultipartUpload(
            CompleteMultipartUploadRequest.builder()
                .bucket(bucketId)
                .key(key)
                .uploadId(uploadId)
                .multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build())
                .build());
        fileSystem.invalidateListCacheForParentOf(bucketId, key);
      } catch (OutOfMemoryError oome) {
        LogChannel.GENERAL.logError(
            BaseMessages.getString(
                PKG,
                "ERROR.S3MultiPart.UploadOutOfMemory",
                new StorageUnitConverter().byteCountToDisplaySize(partSize)),
            oome);
        returnVal = false;
      } catch (Exception e) {
        LogChannel.GENERAL.logError(
            BaseMessages.getString(PKG, "ERROR.S3MultiPart.ExceptionCaught"), e);
        if (uploadId != null) {
          try {
            s3.abortMultipartUpload(
                AbortMultipartUploadRequest.builder()
                    .bucket(bucketId)
                    .key(key)
                    .uploadId(uploadId)
                    .build());
          } catch (Exception ignore) {
            // ignore
          }
          LogChannel.GENERAL.logError(BaseMessages.getString(PKG, "ERROR.S3MultiPart.Aborted"));
        }
        returnVal = false;
      }
      return returnVal;
    }
  }
}
