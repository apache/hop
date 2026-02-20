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
package org.apache.hop.vfs.s3.vfs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.OutputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.vfs2.CacheStrategy;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.FilesCache;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.provider.VfsComponentContext;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.util.StorageUnitConverter;
import org.apache.hop.vfs.s3.s3.vfs.S3FileName;
import org.apache.hop.vfs.s3.s3.vfs.S3FileObject;
import org.apache.hop.vfs.s3.s3.vfs.S3FileSystem;
import org.apache.hop.vfs.s3.s3common.S3HopProperty;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

class S3FileObjectTest {

  public static final String HOST = "S3";
  public static final String SCHEME = "s3";
  public static final int PORT = 843;

  public static final String BUCKET_NAME = "bucket3";
  public static final String OBJECT_NAME = "obj";

  private S3FileName filename;
  private S3FileSystem fileSystemSpy;
  private S3FileObject s3FileObjectBucketSpy;
  private S3FileObject s3FileObjectFileSpy;
  private S3FileObject s3FileObjectSpyRoot;
  private S3Client s3ClientMock;
  private ListObjectsV2Response listResponsePage1;
  private ListObjectsV2Response listResponsePage2;
  private final List<String> childObjectNameComp = new ArrayList<>();
  private final List<String> childBucketNameListComp = new ArrayList<>();
  private final long contentLength = 42;
  private final String origKey = "some/key";
  private final Date testDate = new Date();
  private final Instant testInstant = testDate.toInstant();

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
  }

  @BeforeEach
  void setUp() throws Exception {

    s3ClientMock = mock(S3Client.class);

    filename = new S3FileName(SCHEME, BUCKET_NAME, BUCKET_NAME, FileType.FOLDER);
    S3FileName rootFileName = new S3FileName(SCHEME, "", "", FileType.FOLDER);
    S3HopProperty s3HopProperty = mock(S3HopProperty.class);
    when(s3HopProperty.getPartSize()).thenReturn("5MB");
    S3FileSystem fileSystem =
        new S3FileSystem(
            rootFileName, new FileSystemOptions(), new StorageUnitConverter(), s3HopProperty);
    fileSystemSpy = spy(fileSystem);
    VfsComponentContext context = mock(VfsComponentContext.class);
    final DefaultFileSystemManager fsm = new DefaultFileSystemManager();
    FilesCache cache = mock(FilesCache.class);
    fsm.setFilesCache(cache);
    fsm.setCacheStrategy(CacheStrategy.ON_RESOLVE);
    when(context.getFileSystemManager()).thenReturn(fsm);
    fileSystemSpy.setContext(context);

    S3FileObject s3FileObject = new S3FileObject(filename, fileSystemSpy);
    s3FileObjectBucketSpy = spy(s3FileObject);

    s3FileObjectFileSpy =
        spy(
            new S3FileObject(
                new S3FileName(
                    SCHEME, BUCKET_NAME, BUCKET_NAME + "/" + origKey, FileType.IMAGINARY),
                fileSystemSpy));

    S3FileObject s3FileObjectRoot = new S3FileObject(rootFileName, fileSystemSpy);
    s3FileObjectSpyRoot = spy(s3FileObjectRoot);

    when(fileSystemSpy.getS3Client()).thenReturn(s3ClientMock);

    // listBuckets for root
    when(s3ClientMock.listBuckets())
        .thenReturn(ListBucketsResponse.builder().buckets(createBuckets()).build());

    // listObjectsV2 for folder listing (two pages: contents then commonPrefixes)
    listResponsePage1 =
        ListObjectsV2Response.builder()
            .contents(createContents(0))
            .commonPrefixes(new ArrayList<>())
            .nextContinuationToken("token1")
            .build();
    listResponsePage2 =
        ListObjectsV2Response.builder()
            .contents(new ArrayList<>())
            .commonPrefixes(createCommonPrefixes(3))
            .nextContinuationToken(null)
            .build();
    when(s3ClientMock.listObjectsV2(any(ListObjectsV2Request.class)))
        .thenReturn(listResponsePage1)
        .thenReturn(listResponsePage2);

    // getObject / headObject for file metadata and stream
    ResponseInputStream<GetObjectResponse> mockStream = mock(ResponseInputStream.class);
    GetObjectResponse getResponse =
        GetObjectResponse.builder().contentLength(contentLength).lastModified(testInstant).build();
    lenient().when(mockStream.response()).thenReturn(getResponse);
    lenient().when(s3ClientMock.getObject(any(GetObjectRequest.class))).thenReturn(mockStream);

    software.amazon.awssdk.services.s3.model.HeadObjectResponse headResponse =
        software.amazon.awssdk.services.s3.model.HeadObjectResponse.builder()
            .contentLength(contentLength)
            .lastModified(testInstant)
            .build();
    lenient()
        .when(
            s3ClientMock.headObject(
                any(software.amazon.awssdk.services.s3.model.HeadObjectRequest.class)))
        .thenReturn(headResponse);
  }

  @Test
  void testGetInputStream() throws Exception {
    assertNotNull(s3FileObjectBucketSpy.getInputStream());
  }

  @Test
  void testGetS3BucketName() {
    filename = new S3FileName(SCHEME, BUCKET_NAME, "", FileType.FOLDER);
    when(s3FileObjectBucketSpy.getName()).thenReturn(filename);
    s3FileObjectBucketSpy.getBucketName();
  }

  @Test
  void testDoGetOutputStream() throws Exception {
    CreateMultipartUploadResponse createResponse =
        CreateMultipartUploadResponse.builder().uploadId("foo").build();
    when(s3ClientMock.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
        .thenReturn(createResponse);
    UploadPartResponse uploadPartResponse = UploadPartResponse.builder().eTag("etag").build();
    when(s3ClientMock.uploadPart(
            any(software.amazon.awssdk.services.s3.model.UploadPartRequest.class),
            any(software.amazon.awssdk.core.sync.RequestBody.class)))
        .thenReturn(uploadPartResponse);

    assertNotNull(s3FileObjectBucketSpy.doGetOutputStream(false));
    OutputStream out = s3FileObjectBucketSpy.doGetOutputStream(true);
    assertNotNull(out);
    out.write(new byte[1024 * 1024 * 6]); // 6MB
    out.close();

    verify(s3ClientMock, times(2))
        .uploadPart(
            any(software.amazon.awssdk.services.s3.model.UploadPartRequest.class),
            any(software.amazon.awssdk.core.sync.RequestBody.class));
    verify(s3ClientMock, atMost(1))
        .completeMultipartUpload(
            any(software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest.class));
  }

  @Test
  void testDoGetInputStream() throws Exception {
    assertNotNull(s3FileObjectBucketSpy.getInputStream());
  }

  @Test
  void testDoGetTypeImaginary() throws Exception {
    assertEquals(FileType.IMAGINARY, s3FileObjectFileSpy.getType());
  }

  @Test
  void testDoGetTypeFolder() throws Exception {
    FileName mockFile = mock(FileName.class);
    when(s3FileObjectBucketSpy.getName()).thenReturn(mockFile);
    when(mockFile.getPath()).thenReturn(S3FileObject.DELIMITER);
    assertEquals(FileType.FOLDER, s3FileObjectBucketSpy.getType());
  }

  @Test
  void testDoCreateFolder() throws Exception {
    S3FileObject notRootBucket =
        spy(
            new S3FileObject(
                new S3FileName(
                    SCHEME, BUCKET_NAME, BUCKET_NAME + "/" + origKey, FileType.IMAGINARY),
                fileSystemSpy));
    notRootBucket.createFolder();
    ArgumentCaptor<PutObjectRequest> putCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
    verify(s3ClientMock)
        .putObject(putCaptor.capture(), any(software.amazon.awssdk.core.sync.RequestBody.class));
    assertEquals(BUCKET_NAME, putCaptor.getValue().bucket());
    assertEquals("some/key/", putCaptor.getValue().key());
  }

  @Test
  void testCanRenameTo() throws Exception {
    FileObject newFile = mock(FileObject.class);
    assertFalse(s3FileObjectBucketSpy.canRenameTo(newFile));
    when(s3FileObjectBucketSpy.getType()).thenReturn(FileType.FOLDER);
    assertFalse(s3FileObjectBucketSpy.canRenameTo(newFile));
  }

  @Test
  void testCanRenameToNullFile() {
    assertThrows(NullPointerException.class, () -> s3FileObjectBucketSpy.canRenameTo(null));
  }

  @Test
  void testDoDelete() throws Exception {
    when(s3ClientMock.listObjectsV2(any(ListObjectsV2Request.class)))
        .thenReturn(
            ListObjectsV2Response.builder()
                .contents(createContents(0))
                .nextContinuationToken(null)
                .build());
    fileSystemSpy.init();
    s3FileObjectBucketSpy.doDelete();
    verify(s3ClientMock, times(4))
        .deleteObject(any(software.amazon.awssdk.services.s3.model.DeleteObjectRequest.class));
  }

  @Test
  void testDoRename() throws Exception {
    String someNewBucketName = "someNewBucketName";
    String someNewKey = "some/newKey";
    S3FileName newFileName =
        new S3FileName(
            SCHEME, someNewBucketName, someNewBucketName + "/" + someNewKey, FileType.FILE);
    S3FileObject newFile = new S3FileObject(newFileName, fileSystemSpy);
    ArgumentCaptor<CopyObjectRequest> copyCaptor = ArgumentCaptor.forClass(CopyObjectRequest.class);
    s3FileObjectFileSpy.doAttach();
    s3FileObjectFileSpy.moveTo(newFile);

    verify(s3ClientMock).copyObject(copyCaptor.capture());
    assertEquals(someNewBucketName, copyCaptor.getValue().destinationBucket());
    assertEquals(someNewKey, copyCaptor.getValue().destinationKey());
    assertEquals(BUCKET_NAME, copyCaptor.getValue().sourceBucket());
    assertEquals(origKey, copyCaptor.getValue().sourceKey());
  }

  @Test
  void testDoGetLastModifiedTime() throws Exception {
    s3FileObjectFileSpy.doAttach();
    assertEquals(testDate.getTime(), s3FileObjectFileSpy.doGetLastModifiedTime());
  }

  @Test
  void testListChildrenNotRoot() throws FileSystemException {
    fileSystemSpy.init();
    FileObject[] children = s3FileObjectBucketSpy.getChildren();
    assertEquals(6, children.length);
    List<String> childNameArray =
        Arrays.stream(children)
            .map(child -> child.getName().getPath())
            .collect(Collectors.toList());
    assertEquals(childObjectNameComp, childNameArray);
  }

  @Test
  void testListChildrenRoot() throws FileSystemException {
    fileSystemSpy.init();
    FileObject[] children = s3FileObjectSpyRoot.getChildren();
    assertEquals(4, children.length);
    List<String> childNameArray =
        Arrays.stream(children)
            .map(child -> child.getName().getPath())
            .collect(Collectors.toList());
    assertEquals(childBucketNameListComp, childNameArray);
  }

  @Test
  void testHandleAttachException() throws Exception {
    when(s3ClientMock.headObject(
            any(software.amazon.awssdk.services.s3.model.HeadObjectRequest.class)))
        .thenThrow(
            (S3Exception)
                S3Exception.builder()
                    .message("NoSuchKey")
                    .awsErrorDetails(AwsErrorDetails.builder().errorCode("NoSuchKey").build())
                    .build());
    when(s3ClientMock.getObject(any(GetObjectRequest.class)))
        .thenThrow(
            (S3Exception)
                S3Exception.builder()
                    .message("NoSuchKey")
                    .awsErrorDetails(AwsErrorDetails.builder().errorCode("NoSuchKey").build())
                    .build());
    when(s3ClientMock.listObjectsV2(any(ListObjectsV2Request.class)))
        .thenReturn(
            ListObjectsV2Response.builder()
                .contents(new ArrayList<>())
                .commonPrefixes(createCommonPrefixes(1))
                .build());
    s3FileObjectFileSpy.doAttach();
    assertEquals(FileType.FOLDER, s3FileObjectFileSpy.getType());
  }

  @Test
  void testDoAttachCacheHit() throws Exception {
    java.util.Map<String, org.apache.hop.vfs.s3.s3common.S3ListCache.ChildInfo> entries =
        new java.util.LinkedHashMap<>();
    entries.put(
        origKey,
        new org.apache.hop.vfs.s3.s3common.S3ListCache.ChildInfo(
            FileType.FILE, 999, Instant.now()));
    fileSystemSpy.putListCache(BUCKET_NAME, "some/", entries);

    s3FileObjectFileSpy.doAttach();
    assertEquals(FileType.FILE, s3FileObjectFileSpy.getType());
  }

  @Test
  void testDoAttachCacheHitFolder() throws Exception {
    java.util.Map<String, org.apache.hop.vfs.s3.s3common.S3ListCache.ChildInfo> entries =
        new java.util.LinkedHashMap<>();
    entries.put(
        origKey + "/",
        new org.apache.hop.vfs.s3.s3common.S3ListCache.ChildInfo(
            FileType.FOLDER, 0, Instant.EPOCH));
    fileSystemSpy.putListCache(BUCKET_NAME, "some/", entries);

    s3FileObjectFileSpy.doAttach();
    assertEquals(FileType.FOLDER, s3FileObjectFileSpy.getType());
  }

  @Test
  void testHandleAttachExceptionEmptyFolder() throws Exception {
    S3Exception exception =
        (S3Exception)
            S3Exception.builder()
                .message("NoSuchKey")
                .awsErrorDetails(AwsErrorDetails.builder().errorCode("NoSuchKey").build())
                .build();
    when(s3ClientMock.headObject(
            any(software.amazon.awssdk.services.s3.model.HeadObjectRequest.class)))
        .thenThrow(exception);
    when(s3ClientMock.getObject(any(GetObjectRequest.class))).thenThrow(exception);
    when(s3ClientMock.listObjectsV2(any(ListObjectsV2Request.class)))
        .thenReturn(
            ListObjectsV2Response.builder()
                .contents(new ArrayList<>())
                .commonPrefixes(new ArrayList<>())
                .build());
    s3FileObjectFileSpy.doAttach();
    assertEquals(FileType.IMAGINARY, s3FileObjectFileSpy.getType());
  }

  private List<Bucket> createBuckets() {
    List<Bucket> buckets = new ArrayList<>();
    buckets.add(Bucket.builder().name("bucket1").build());
    buckets.add(Bucket.builder().name("bucket2").build());
    buckets.add(Bucket.builder().name("bucket3").build());
    buckets.add(Bucket.builder().name("bucket4").build());
    childBucketNameListComp.addAll(
        buckets.stream().map(b -> "/" + b.name()).collect(Collectors.toList()));
    return buckets;
  }

  private List<software.amazon.awssdk.services.s3.model.S3Object> createContents(int startNum) {
    List<software.amazon.awssdk.services.s3.model.S3Object> list = new ArrayList<>();
    for (int i = startNum; i < startNum + 3; i++) {
      String k = "key" + i;
      list.add(software.amazon.awssdk.services.s3.model.S3Object.builder().key(k).size(0L).build());
      childObjectNameComp.add(BUCKET_NAME + "/" + k);
    }
    return list;
  }

  private List<CommonPrefix> createCommonPrefixes(int count) {
    List<CommonPrefix> prefixes = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      String p = "key" + i;
      prefixes.add(CommonPrefix.builder().prefix(p).build());
      childObjectNameComp.add(BUCKET_NAME + "/" + p);
    }
    return prefixes;
  }
}
