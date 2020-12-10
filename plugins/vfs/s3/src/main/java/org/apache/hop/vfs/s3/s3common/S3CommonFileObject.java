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

package org.apache.hop.vfs.s3.s3common;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public abstract class S3CommonFileObject extends AbstractFileObject {

  private static final Logger logger = LoggerFactory.getLogger(S3CommonFileObject.class);
  public static final String DELIMITER = "/";

  protected S3CommonFileSystem fileSystem;
  protected String bucketName;
  protected String key;
  protected S3Object s3Object;
  protected ObjectMetadata s3ObjectMetadata;

  protected S3CommonFileObject(final AbstractFileName name, final S3CommonFileSystem fileSystem) {
    super(name, fileSystem);
    this.fileSystem = fileSystem;
    this.bucketName = getS3BucketName();
    this.key = getBucketRelativeS3Path();
  }

  @Override
  protected long doGetContentSize() {
    return s3ObjectMetadata.getContentLength();
  }

  @Override
  protected InputStream doGetInputStream() throws Exception {
    logger.debug("Accessing content {}", getQualifiedName());
    closeS3Object();
    S3Object streamS3Object = getS3Object();
    return new S3CommonFileInputStream(streamS3Object.getObjectContent(), streamS3Object);
  }

  @Override
  protected FileType doGetType() throws Exception {
    return getType();
  }

  @Override
  protected String[] doListChildren() throws Exception {
    List<String> childrenList = new ArrayList<>();

    // only listing folders or the root bucket
    if (getType() == FileType.FOLDER || isRootBucket()) {
      childrenList = getS3ObjectsFromVirtualFolder(key, bucketName);
    }
    String[] childrenArr = new String[childrenList.size()];

    return childrenList.toArray(childrenArr);
  }

  protected String getS3BucketName() {
    String bucket = getName().getPath();
    if (bucket.indexOf(DELIMITER, 1) > 1) {
      // this file is a file, to get the bucket, remove the name from the path
      bucket = bucket.substring(1, bucket.indexOf(DELIMITER, 1));
    } else {
      // this file is a bucket
      bucket = bucket.replace(DELIMITER, "");
    }
    return bucket;
  }

  protected List<String> getS3ObjectsFromVirtualFolder(String key, String bucketName) {
    List<String> childrenList = new ArrayList<>();

    // fix cases where the path doesn't include the final delimiter
    String realKey = key;
    if (!realKey.endsWith(DELIMITER)) {
      realKey += DELIMITER;
    }

    if ("".equals(key) && "".equals(bucketName)) {
      // Getting buckets in root folder
      List<Bucket> bucketList = fileSystem.getS3Client().listBuckets();
      for (Bucket bucket : bucketList) {
        childrenList.add(bucket.getName() + DELIMITER);
      }
    } else {
      getObjectsFromNonRootFolder(key, bucketName, childrenList, realKey);
    }
    return childrenList;
  }

  private void getObjectsFromNonRootFolder(
      String key, String bucketName, List<String> childrenList, String realKey) {
    // Getting files/folders in a folder/bucket
    String prefix = key.isEmpty() || key.endsWith(DELIMITER) ? key : key + DELIMITER;
    ListObjectsRequest listObjectsRequest =
        new ListObjectsRequest()
            .withBucketName(bucketName)
            .withPrefix(prefix)
            .withDelimiter(DELIMITER);

    ObjectListing ol = fileSystem.getS3Client().listObjects(listObjectsRequest);

    ArrayList<S3ObjectSummary> allSummaries = new ArrayList<>(ol.getObjectSummaries());
    ArrayList<String> allCommonPrefixes = new ArrayList<>(ol.getCommonPrefixes());

    // get full list
    while (ol.isTruncated()) {
      ol = fileSystem.getS3Client().listNextBatchOfObjects(ol);
      allSummaries.addAll(ol.getObjectSummaries());
      allCommonPrefixes.addAll(ol.getCommonPrefixes());
    }

    for (S3ObjectSummary s3os : allSummaries) {
      if (!s3os.getKey().equals(realKey)) {
        childrenList.add(s3os.getKey().substring(prefix.length()));
      }
    }

    for (String commonPrefix : allCommonPrefixes) {
      if (!commonPrefix.equals(realKey)) {
        childrenList.add(commonPrefix.substring(prefix.length()));
      }
    }
  }

  protected String getBucketRelativeS3Path() {
    if (getName().getPath().indexOf(DELIMITER, 1) >= 0) {
      return getName().getPath().substring(getName().getPath().indexOf(DELIMITER, 1) + 1);
    } else {
      return "";
    }
  }

  @VisibleForTesting
  public S3Object getS3Object() {
    return getS3Object(this.key, this.bucketName);
  }

  protected S3Object getS3Object(String key, String bucket) {
    if (s3Object != null && s3Object.getObjectContent() != null) {
      logger.debug("Returning exisiting object {}", getQualifiedName());
      return s3Object;
    } else {
      logger.debug("Getting object {}", getQualifiedName());
      return fileSystem.getS3Client().getObject(bucket, key);
    }
  }

  protected boolean isRootBucket() {
    return key.equals("");
  }

  @Override
  public void doAttach() throws Exception {
    logger.debug("Attach called on {}", getQualifiedName());
    injectType(FileType.IMAGINARY);

    if (isRootBucket()) {
      // cannot attach to root bucket
      injectType(FileType.FOLDER);
      return;
    }
    try {
      // 1. Is it an existing file?
      s3ObjectMetadata = fileSystem.getS3Client().getObjectMetadata(bucketName, key);
      injectType(
          getName().getType()); // if this worked then the automatically detected type is right
    } catch (AmazonS3Exception e) { // S3 object doesn't exist
      // 2. Is it in reality a folder?
      handleAttachException(key, bucketName);
    } finally {
      closeS3Object();
    }
  }

  protected void handleAttachException(String key, String bucket) throws IOException {
    String keyWithDelimiter = key + DELIMITER;
    try {
      s3ObjectMetadata = fileSystem.getS3Client().getObjectMetadata(bucketName, key);
      injectType(FileType.FOLDER);
      this.key = keyWithDelimiter;
    } catch (AmazonS3Exception e1) {
      String errorCode = e1.getErrorCode();
      try {
        // S3 Object does not exist (could be the process of creating a new file. Lets fallback to
        // old the old behavior. (getting the s3 object)
        if (errorCode.equals("404 Not Found")) {
          s3Object = getS3Object(keyWithDelimiter, bucket);
          s3ObjectMetadata = s3Object.getObjectMetadata();
          injectType(FileType.FOLDER);
          this.key = keyWithDelimiter;
        } else {
          // The exception was not related with not finding the file
          handleAttachExceptionFallback(bucket, keyWithDelimiter, e1);
        }
      } catch (AmazonS3Exception e2) {
        // something went wrong getting the s3 object
        handleAttachExceptionFallback(bucket, keyWithDelimiter, e2);
      }
    } finally {
      closeS3Object();
    }
  }

  private void handleAttachExceptionFallback(
      String bucket, String keyWithDelimiter, AmazonS3Exception exception)
      throws FileSystemException {
    ListObjectsRequest listObjectsRequest =
        new ListObjectsRequest()
            .withBucketName(bucket)
            .withPrefix(keyWithDelimiter)
            .withDelimiter(DELIMITER);
    ObjectListing ol = fileSystem.getS3Client().listObjects(listObjectsRequest);

    if (!(ol.getCommonPrefixes().isEmpty() && ol.getObjectSummaries().isEmpty())) {
      injectType(FileType.FOLDER);
    } else {
      // Folders don't really exist - they will generate a "NoSuchKey" exception
      // confirms key doesn't exist but connection okay
      String errorCode = exception.getErrorCode();
      if (!errorCode.equals("NoSuchKey")) {
        // bubbling up other connection errors
        logger.error(
            "Could not get information on " + getQualifiedName(),
            exception); // make sure this gets printed for the user
        throw new FileSystemException("vfs.provider/get-type.error", getQualifiedName(), exception);
      }
    }
  }

  private void closeS3Object() throws IOException {
    if (s3Object != null) {
      s3Object.close();
      s3Object = null;
    }
  }

  @Override
  public void doDetach() throws Exception {
    logger.debug("detaching {}", getQualifiedName());
    closeS3Object();
  }

  @Override
  protected void doDelete() throws FileSystemException {
    doDelete(this.key, this.bucketName);
  }

  protected void doDelete(String key, String bucketName) throws FileSystemException {
    // can only delete folder if empty
    if (getType() == FileType.FOLDER) {

      // list all children inside the folder
      ObjectListing ol = fileSystem.getS3Client().listObjects(bucketName, key);
      ArrayList<S3ObjectSummary> allSummaries = new ArrayList<>(ol.getObjectSummaries());

      // get full list
      while (ol.isTruncated()) {
        ol = fileSystem.getS3Client().listNextBatchOfObjects(ol);
        allSummaries.addAll(ol.getObjectSummaries());
      }

      for (S3ObjectSummary s3os : allSummaries) {
        fileSystem.getS3Client().deleteObject(bucketName, s3os.getKey());
      }
    }

    fileSystem.getS3Client().deleteObject(bucketName, key);
  }

  @Override
  protected OutputStream doGetOutputStream(boolean bAppend) throws Exception {
    return new S3CommonPipedOutputStream(this.fileSystem, bucketName, key);
  }

  @Override
  public long doGetLastModifiedTime() {
    return s3ObjectMetadata.getLastModified().getTime();
  }

  @Override
  protected void doCreateFolder() throws Exception {
    if (!isRootBucket()) {
      // create meta-data for your folder and set content-length to 0
      ObjectMetadata metadata = new ObjectMetadata();
      metadata.setContentLength(0);
      metadata.setContentType("binary/octet-stream");

      // create empty content
      InputStream emptyContent = new ByteArrayInputStream(new byte[0]);

      // create a PutObjectRequest passing the folder name suffixed by /
      PutObjectRequest putObjectRequest =
          createPutObjectRequest(bucketName, key + DELIMITER, emptyContent, metadata);

      // send request to S3 to create folder
      try {
        fileSystem.getS3Client().putObject(putObjectRequest);
      } catch (AmazonS3Exception e) {
        throw new FileSystemException("vfs.provider.local/create-folder.error", this, e);
      }
    } else {
      throw new FileSystemException("vfs.provider/create-folder-not-supported.error");
    }
  }

  protected PutObjectRequest createPutObjectRequest(
      String bucketName, String key, InputStream inputStream, ObjectMetadata objectMetadata) {
    return new PutObjectRequest(bucketName, key, inputStream, objectMetadata);
  }

  @Override
  protected void doRename(FileObject newFile) throws Exception {
    // no folder renames on S3
    if (getType().equals(FileType.FOLDER)) {
      throw new FileSystemException("vfs.provider/rename-not-supported.error");
    }

    s3ObjectMetadata = fileSystem.getS3Client().getObjectMetadata(bucketName, key);

    if (s3ObjectMetadata == null) {
      // object doesn't exist
      throw new FileSystemException("vfs.provider/rename.error", this, newFile);
    }

    S3CommonFileObject dest = (S3CommonFileObject) newFile;

    // 1. copy the file
    CopyObjectRequest copyObjRequest =
        createCopyObjectRequest(bucketName, key, dest.bucketName, dest.key);
    fileSystem.getS3Client().copyObject(copyObjRequest);

    // 2. delete self
    delete();
  }

  protected CopyObjectRequest createCopyObjectRequest(
      String sourceBucket, String sourceKey, String destBucket, String destKey) {
    return new CopyObjectRequest(sourceBucket, sourceKey, destBucket, destKey);
  }

  protected String getQualifiedName() {
    return getQualifiedName(this);
  }

  protected String getQualifiedName(S3CommonFileObject s3nFileObject) {
    return s3nFileObject.bucketName + "/" + s3nFileObject.key;
  }
}
