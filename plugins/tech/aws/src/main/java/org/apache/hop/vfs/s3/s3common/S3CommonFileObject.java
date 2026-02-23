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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileObject;
import org.apache.hop.core.logging.LogChannel;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

public abstract class S3CommonFileObject extends AbstractFileObject {

  public static final String DELIMITER = "/";

  protected S3CommonFileSystem fileSystem;
  @Getter protected String bucketName;
  @Getter protected String key;

  protected ResponseInputStream<GetObjectResponse> currentObjectStream;
  protected Long contentLength;
  protected Instant lastModified;

  protected S3CommonFileObject(final AbstractFileName name, final S3CommonFileSystem fileSystem) {
    super(name, fileSystem);
    this.fileSystem = fileSystem;
    this.bucketName = getS3BucketName();
    this.key = getBucketRelativeS3Path();
  }

  @Override
  protected long doGetContentSize() {
    return contentLength != null ? contentLength : 0;
  }

  @Override
  protected InputStream doGetInputStream() throws Exception {
    LogChannel.GENERAL.logDebug("Accessing content {0}", getQualifiedName());
    closeS3Object();
    ResponseInputStream<GetObjectResponse> stream = getObjectStream(bucketName, key);
    return new S3CommonFileInputStream(stream, stream);
  }

  @Override
  protected FileType doGetType() throws Exception {
    return getType();
  }

  @Override
  protected String[] doListChildren() throws Exception {
    List<String> childrenList = new ArrayList<>();
    if (getType() == FileType.FOLDER || isRootBucket()) {
      childrenList = getS3ObjectsFromVirtualFolder(key, bucketName);
    }
    return childrenList.toArray(new String[0]);
  }

  protected String getS3BucketName() {
    String path = getName().getPath();
    if (path == null) {
      return "";
    }
    if (path.startsWith(DELIMITER)) {
      path = path.substring(1);
    }
    if (path.isEmpty()) {
      return "";
    }
    int slash = path.indexOf(DELIMITER);
    return slash >= 0 ? path.substring(0, slash) : path;
  }

  protected List<String> getS3ObjectsFromVirtualFolder(String key, String bucketName) {
    List<String> childrenList = new ArrayList<>();
    String realKey = key;
    if (!realKey.endsWith(DELIMITER)) {
      realKey += DELIMITER;
    }

    if ("".equals(key) && "".equals(bucketName)) {
      List<Bucket> buckets = fileSystem.getS3Client().listBuckets().buckets();
      if (buckets != null) {
        for (Bucket b : buckets) {
          childrenList.add(b.name() + DELIMITER);
        }
      }
    } else {
      getObjectsFromNonRootFolder(key, bucketName, childrenList, realKey);
    }
    return childrenList;
  }

  private void getObjectsFromNonRootFolder(
      String key, String bucketName, List<String> childrenList, String realKey) {
    String prefix = key.isEmpty() || key.endsWith(DELIMITER) ? key : key + DELIMITER;
    Map<String, S3ListCache.ChildInfo> cacheEntries = new LinkedHashMap<>();
    String continuationToken = null;
    do {
      ListObjectsV2Request.Builder req =
          ListObjectsV2Request.builder().bucket(bucketName).prefix(prefix).delimiter(DELIMITER);
      if (continuationToken != null) {
        req.continuationToken(continuationToken);
      }
      ListObjectsV2Response response = fileSystem.getS3Client().listObjectsV2(req.build());

      if (response.contents() != null) {
        for (S3Object s3o : response.contents()) {
          if (!s3o.key().equals(realKey)) {
            String childName = s3o.key().substring(prefix.length());
            childrenList.add(childName);
            cacheEntries.put(
                s3o.key(),
                new S3ListCache.ChildInfo(
                    FileType.FILE,
                    s3o.size() != null ? s3o.size() : 0,
                    s3o.lastModified() != null ? s3o.lastModified() : Instant.EPOCH));
          }
        }
      }
      if (response.commonPrefixes() != null) {
        for (CommonPrefix cp : response.commonPrefixes()) {
          String p = cp.prefix();
          if (!p.equals(realKey)) {
            childrenList.add(p.substring(prefix.length()));
            cacheEntries.put(p, new S3ListCache.ChildInfo(FileType.FOLDER, 0, Instant.EPOCH));
          }
        }
      }
      continuationToken = response.nextContinuationToken();
    } while (continuationToken != null);

    if (!cacheEntries.isEmpty()) {
      fileSystem.putListCache(bucketName, prefix, cacheEntries);
    }
  }

  protected String getBucketRelativeS3Path() {
    String path = getName().getPath();
    if (path == null) {
      return "";
    }
    if (path.startsWith(DELIMITER)) {
      path = path.substring(1);
    }
    if (path.isEmpty()) {
      return "";
    }
    int slash = path.indexOf(DELIMITER);
    return slash >= 0 ? path.substring(slash + 1) : "";
  }

  protected ResponseInputStream<GetObjectResponse> getObjectStream(String bucket, String key) {
    if (currentObjectStream != null) {
      LogChannel.GENERAL.logDebug("Returning existing object {0}", getQualifiedName());
      return currentObjectStream;
    }
    return fileSystem
        .getS3Client()
        .getObject(GetObjectRequest.builder().bucket(bucket).key(key).build());
  }

  protected boolean isRootBucket() {
    return key.equals("");
  }

  private static String errorCode(S3Exception e) {
    if (e.awsErrorDetails() != null && e.awsErrorDetails().errorCode() != null) {
      return e.awsErrorDetails().errorCode();
    }
    return e.getMessage();
  }

  @Override
  public void doAttach() throws Exception {
    LogChannel.GENERAL.logDebug("Attach called on {0}", getQualifiedName());
    injectType(FileType.IMAGINARY);

    if (isRootBucket()) {
      injectType(FileType.FOLDER);
      return;
    }

    // Use list cache when possible to avoid headObject or extra list calls
    String parentPrefix = S3ListCache.parentPrefix(key);
    S3ListCache.ChildInfo cached = fileSystem.getFromListCache(bucketName, parentPrefix, key);
    if (cached == null && !key.endsWith(DELIMITER)) {
      cached = fileSystem.getFromListCache(bucketName, parentPrefix, key + DELIMITER);
    }
    if (cached != null) {
      contentLength = cached.size;
      lastModified = cached.lastModified != null ? cached.lastModified : null;
      injectType(cached.type);
      if (cached.type == FileType.FOLDER) {
        this.key = key.endsWith(DELIMITER) ? key : key + DELIMITER;
      }
      return;
    }

    try {
      HeadObjectResponse head =
          fileSystem
              .getS3Client()
              .headObject(HeadObjectRequest.builder().bucket(bucketName).key(key).build());
      contentLength = head.contentLength();
      lastModified = head.lastModified();
      injectType(getName().getType());
    } catch (S3Exception e) {
      handleAttachException(key, bucketName, e);
    } finally {
      closeS3Object();
    }
  }

  protected void handleAttachException(String key, String bucket, S3Exception e)
      throws IOException {
    String keyWithDelimiter = key + DELIMITER;
    String err = errorCode(e);
    boolean isNotFound = "404".equals(err) || "NotFound".equals(err) || "NoSuchKey".equals(err);

    if (isNotFound) {
      handleNotFoundAttach(bucket, keyWithDelimiter);
    } else {
      resolveTypeViaList(key, bucket, keyWithDelimiter);
    }
  }

  private void handleNotFoundAttach(String bucket, String keyWithDelimiter) throws IOException {
    try {
      ResponseInputStream<GetObjectResponse> stream =
          fileSystem
              .getS3Client()
              .getObject(GetObjectRequest.builder().bucket(bucket).key(keyWithDelimiter).build());
      try {
        GetObjectResponse r = stream.response();
        contentLength = r.contentLength();
        lastModified = r.lastModified();
        injectType(FileType.FOLDER);
        this.key = keyWithDelimiter;
      } finally {
        stream.close();
      }
    } catch (S3Exception e2) {
      checkFolderViaList(bucket, keyWithDelimiter);
    } finally {
      closeS3Object();
    }
  }

  /**
   * headObject failed with a non-404 error (e.g. 400 from cross-region). Use listObjectsV2 (which
   * handles cross-region) to determine if the path is a file, folder, or imaginary. If list returns
   * 403 (e.g. public bucket with GetObject but not ListBucket), try getObject for the exact key.
   */
  private void resolveTypeViaList(String key, String bucket, String keyWithDelimiter)
      throws FileSystemException {
    try {
      ListObjectsV2Response response =
          fileSystem
              .getS3Client()
              .listObjectsV2(
                  ListObjectsV2Request.builder()
                      .bucket(bucket)
                      .prefix(key)
                      .delimiter(DELIMITER)
                      .maxKeys(2)
                      .build());

      if (response.contents() != null) {
        for (S3Object obj : response.contents()) {
          if (obj.key().equals(key)) {
            contentLength = obj.size();
            lastModified = obj.lastModified();
            injectType(getName().getType());
            return;
          }
        }
      }
      if (response.commonPrefixes() != null) {
        for (CommonPrefix cp : response.commonPrefixes()) {
          if (cp.prefix().equals(keyWithDelimiter)) {
            injectType(FileType.FOLDER);
            this.key = keyWithDelimiter;
            return;
          }
        }
      }
    } catch (S3Exception listError) {
      if (isAccessDenied(listError)) {
        resolveTypeViaGetObject(key, bucket);
      } else {
        LogChannel.GENERAL.logError(
            "Could not get information on " + getQualifiedName(), listError);
        throw new FileSystemException("vfs.provider/get-type.error", getQualifiedName(), listError);
      }
    }
  }

  private static boolean isAccessDenied(S3Exception e) {
    Integer code = e.statusCode();
    return code != null && code == 403;
  }

  /**
   * List returned 403 (e.g. public bucket that allows GetObject but not ListBucket). Try getObject
   * for the exact key; if it succeeds, treat as file.
   */
  private void resolveTypeViaGetObject(String key, String bucket) throws FileSystemException {
    try {
      ResponseInputStream<GetObjectResponse> stream =
          fileSystem
              .getS3Client()
              .getObject(GetObjectRequest.builder().bucket(bucket).key(key).build());
      try {
        GetObjectResponse r = stream.response();
        contentLength = r.contentLength();
        lastModified = r.lastModified();
        injectType(getName().getType());
      } finally {
        stream.close();
      }
    } catch (S3Exception e) {
      LogChannel.GENERAL.logError("Could not get information on " + getQualifiedName(), e);
      throw new FileSystemException("vfs.provider/get-type.error", getQualifiedName(), e);
    } catch (IOException e) {
      throw new FileSystemException("vfs.provider/get-type.error", getQualifiedName(), e);
    } finally {
      try {
        closeS3Object();
      } catch (IOException e) {
        LogChannel.GENERAL.logDebug("Error closing S3 object", e);
      }
    }
  }

  private void checkFolderViaList(String bucket, String keyWithDelimiter)
      throws FileSystemException {
    ListObjectsV2Response response =
        fileSystem
            .getS3Client()
            .listObjectsV2(
                ListObjectsV2Request.builder()
                    .bucket(bucket)
                    .prefix(keyWithDelimiter)
                    .delimiter(DELIMITER)
                    .maxKeys(1)
                    .build());
    boolean hasContent =
        (response.contents() != null && !response.contents().isEmpty())
            || (response.commonPrefixes() != null && !response.commonPrefixes().isEmpty());
    if (hasContent) {
      injectType(FileType.FOLDER);
    }
  }

  private void closeS3Object() throws IOException {
    if (currentObjectStream != null) {
      currentObjectStream.close();
      currentObjectStream = null;
    }
  }

  @Override
  public void doDetach() throws Exception {
    LogChannel.GENERAL.logDebug("detaching {0}", getQualifiedName());
    closeS3Object();
  }

  @Override
  protected void doDelete() throws FileSystemException {
    doDelete(this.key, this.bucketName);
  }

  protected void doDelete(String key, String bucketName) throws FileSystemException {
    if (getType() == FileType.FOLDER) {
      String prefix = key.isEmpty() || key.endsWith(DELIMITER) ? key : key + DELIMITER;
      String continuationToken = null;
      do {
        ListObjectsV2Request.Builder req =
            ListObjectsV2Request.builder().bucket(bucketName).prefix(prefix);
        if (continuationToken != null) {
          req.continuationToken(continuationToken);
        }
        ListObjectsV2Response response = fileSystem.getS3Client().listObjectsV2(req.build());
        if (response.contents() != null) {
          for (S3Object s3o : response.contents()) {
            fileSystem
                .getS3Client()
                .deleteObject(
                    DeleteObjectRequest.builder().bucket(bucketName).key(s3o.key()).build());
          }
        }
        continuationToken = response.nextContinuationToken();
      } while (continuationToken != null);
    }
    fileSystem
        .getS3Client()
        .deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key(key).build());
    fileSystem.invalidateListCacheForParentOf(bucketName, key);
  }

  @Override
  protected OutputStream doGetOutputStream(boolean bAppend) throws Exception {
    return new S3CommonPipedOutputStream(this.fileSystem, bucketName, key);
  }

  @Override
  public long doGetLastModifiedTime() {
    return lastModified != null ? lastModified.toEpochMilli() : 0;
  }

  @Override
  protected void doCreateFolder() throws Exception {
    if (!isRootBucket()) {
      try {
        fileSystem
            .getS3Client()
            .putObject(
                PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key + DELIMITER)
                    .contentLength(0L)
                    .contentType("binary/octet-stream")
                    .build(),
                RequestBody.fromBytes(new byte[0]));
        fileSystem.invalidateListCacheForParentOf(bucketName, key);
      } catch (S3Exception e) {
        throw new FileSystemException("vfs.provider.local/create-folder.error", this, e);
      }
    } else {
      throw new FileSystemException("vfs.provider/create-folder-not-supported.error");
    }
  }

  @Override
  protected void doRename(FileObject newFile) throws Exception {
    if (getType().equals(FileType.FOLDER)) {
      throw new FileSystemException("vfs.provider/rename-not-supported.error");
    }
    HeadObjectResponse head =
        fileSystem
            .getS3Client()
            .headObject(HeadObjectRequest.builder().bucket(bucketName).key(key).build());
    if (head == null) {
      throw new FileSystemException("vfs.provider/rename.error", this, newFile);
    }
    S3CommonFileObject dest = (S3CommonFileObject) newFile;
    fileSystem
        .getS3Client()
        .copyObject(
            CopyObjectRequest.builder()
                .sourceBucket(bucketName)
                .sourceKey(key)
                .destinationBucket(dest.bucketName)
                .destinationKey(dest.key)
                .build());
    fileSystem.invalidateListCacheForParentOf(bucketName, key);
    fileSystem.invalidateListCacheForParentOf(dest.bucketName, dest.key);
    delete();
  }

  protected String getQualifiedName() {
    return bucketName + "/" + key;
  }
}
