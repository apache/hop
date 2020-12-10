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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.UploadPartResult;
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
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.AbstractMap.SimpleEntry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** created by: dzmitry_bahdanovich date: 10/18/13 */
public class S3FileObjectTest {

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
  private AmazonS3 s3ServiceMock;
  private static final String S3VFS_USE_TEMPORARY_FILE_ON_UPLOAD_DATA =
      "s3.vfs.useTempFileOnUploadData";
  private ObjectListing childObjectListing;
  private S3Object s3ObjectMock;
  private S3ObjectInputStream s3ObjectInputStream;
  private ObjectMetadata s3ObjectMetadata;
  private List<String> childObjectNameComp = new ArrayList<>();
  private List<String> childBucketNameListComp = new ArrayList<>();
  private long contentLength = 42;
  private final String origKey = "some/key";
  private Date testDate = new Date();

  @BeforeClass
  public static void initHop() throws Exception {
    HopEnvironment.init();
  }

  @Before
  public void setUp() throws Exception {

    s3ServiceMock = mock(AmazonS3.class);
    S3Object s3Object = new S3Object();
    s3Object.setKey(OBJECT_NAME);
    s3Object.setBucketName(BUCKET_NAME);

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

    // specify the behaviour of S3 Service
    // when( s3ServiceMock.getBucket( BUCKET_NAME ) ).thenReturn( testBucket );
    when(s3ServiceMock.getObject(BUCKET_NAME, OBJECT_NAME)).thenReturn(s3Object);
    when(s3ServiceMock.getObject(BUCKET_NAME, OBJECT_NAME)).thenReturn(s3Object);
    when(s3ServiceMock.listBuckets()).thenReturn(createBuckets());

    when(s3ServiceMock.doesBucketExistV2(BUCKET_NAME)).thenReturn(true);

    childObjectListing = mock(ObjectListing.class);
    when(childObjectListing.getObjectSummaries())
        .thenReturn(createObjectSummaries(0))
        .thenReturn(new ArrayList<>());
    when(childObjectListing.getCommonPrefixes())
        .thenReturn(new ArrayList<>())
        .thenReturn(createCommonPrefixes(3));
    when(childObjectListing.isTruncated()).thenReturn(true).thenReturn(false);

    when(s3ServiceMock.listObjects(any(ListObjectsRequest.class))).thenReturn(childObjectListing);
    when(s3ServiceMock.listObjects(anyString(), anyString())).thenReturn(childObjectListing);
    when(s3ServiceMock.listNextBatchOfObjects(any(ObjectListing.class)))
        .thenReturn(childObjectListing);

    s3ObjectMock = mock(S3Object.class);
    s3ObjectInputStream = mock(S3ObjectInputStream.class);
    s3ObjectMetadata = mock(ObjectMetadata.class);
    when(s3ObjectMock.getObjectContent()).thenReturn(s3ObjectInputStream);
    when(s3ServiceMock.getObjectMetadata(anyString(), anyString())).thenReturn(s3ObjectMetadata);
    when(s3ObjectMetadata.getContentLength()).thenReturn(contentLength);
    when(s3ObjectMetadata.getLastModified()).thenReturn(testDate);
    when(s3ServiceMock.getObject(anyString(), anyString())).thenReturn(s3ObjectMock);

    when(fileSystemSpy.getS3Client()).thenReturn(s3ServiceMock);
  }

  @Test
  public void testGetS3Object() throws Exception {
    when(s3ServiceMock.getObject(anyString(), anyString())).thenReturn(new S3Object());
    S3FileObject s3FileObject = new S3FileObject(filename, fileSystemSpy);
    S3Object s3Object = s3FileObject.getS3Object();
    assertNotNull(s3Object);
  }

  @Test
  public void testGetS3BucketName() {
    filename = new S3FileName(SCHEME, BUCKET_NAME, "", FileType.FOLDER);
    when(s3FileObjectBucketSpy.getName()).thenReturn(filename);
    s3FileObjectBucketSpy.getS3BucketName();
  }

  @Test
  public void testDoGetOutputStream() throws Exception {
    InitiateMultipartUploadResult initResponse = mock(InitiateMultipartUploadResult.class);
    when(initResponse.getUploadId()).thenReturn("foo");
    when(s3ServiceMock.initiateMultipartUpload(any())).thenReturn(initResponse);
    UploadPartResult uploadPartResult = mock(UploadPartResult.class);
    PartETag tag = mock(PartETag.class);
    when(s3ServiceMock.uploadPart(any())).thenReturn(uploadPartResult);
    when(uploadPartResult.getPartETag()).thenReturn(tag);

    assertNotNull(s3FileObjectBucketSpy.doGetOutputStream(false));
    OutputStream out = s3FileObjectBucketSpy.doGetOutputStream(true);
    assertNotNull(out);
    out.write(new byte[1024 * 1024 * 6]); // 6MB
    out.close();

    // check kettle.properties 's3.vfs.partSize' is less than [5MB, 6MB)
    verify(s3ServiceMock, times(2)).uploadPart(any());
    verify(s3ServiceMock, atMost(1)).completeMultipartUpload(any());
  }

  @Test
  public void testDoGetInputStream() throws Exception {
    assertNotNull(s3FileObjectBucketSpy.getInputStream());
  }

  @Test
  public void testDoGetTypeImaginary() throws Exception {
    assertEquals(FileType.IMAGINARY, s3FileObjectFileSpy.getType());
  }

  @Test
  public void testDoGetTypeFolder() throws Exception {
    FileName mockFile = mock(FileName.class);
    when(s3FileObjectBucketSpy.getName()).thenReturn(mockFile);
    when(mockFile.getPath()).thenReturn(S3FileObject.DELIMITER);
    assertEquals(FileType.FOLDER, s3FileObjectBucketSpy.getType());
  }

  @Test
  public void testDoCreateFolder() throws Exception {
    S3FileObject notRootBucket =
        spy(
            new S3FileObject(
                new S3FileName(
                    SCHEME, BUCKET_NAME, BUCKET_NAME + "/" + origKey, FileType.IMAGINARY),
                fileSystemSpy));
    notRootBucket.createFolder();
    ArgumentCaptor<PutObjectRequest> putObjectRequestArgumentCaptor =
        ArgumentCaptor.forClass(PutObjectRequest.class);
    verify(s3ServiceMock).putObject(putObjectRequestArgumentCaptor.capture());
    assertEquals(BUCKET_NAME, putObjectRequestArgumentCaptor.getValue().getBucketName());
    assertEquals("some/key/", putObjectRequestArgumentCaptor.getValue().getKey());
  }

  @Test
  public void testCanRenameTo() throws Exception {
    FileObject newFile = mock(FileObject.class);
    assertFalse(s3FileObjectBucketSpy.canRenameTo(newFile));
    when(s3FileObjectBucketSpy.getType()).thenReturn(FileType.FOLDER);
    assertFalse(s3FileObjectBucketSpy.canRenameTo(newFile));
  }

  @Test(expected = NullPointerException.class)
  public void testCanRenameToNullFile() throws Exception {
    // This is a bug / weakness in VFS itself
    s3FileObjectBucketSpy.canRenameTo(null);
  }

  @Test
  public void testDoDelete() throws Exception {
    fileSystemSpy.init();
    s3FileObjectBucketSpy.doDelete();
    verify(s3ServiceMock).deleteObject("bucket3", "key0");
    verify(s3ServiceMock).deleteObject("bucket3", "key1");
    verify(s3ServiceMock).deleteObject("bucket3", "key2");
    verify(s3ServiceMock).deleteObject("bucket3", "");
  }

  @Test
  public void testDoRename() throws Exception {
    String someNewBucketName = "someNewBucketName";
    String someNewKey = "some/newKey";
    S3FileName newFileName =
        new S3FileName(
            SCHEME, someNewBucketName, someNewBucketName + "/" + someNewKey, FileType.FILE);
    S3FileObject newFile = new S3FileObject(newFileName, fileSystemSpy);
    ArgumentCaptor<CopyObjectRequest> copyObjectRequestArgumentCaptor =
        ArgumentCaptor.forClass(CopyObjectRequest.class);
    when(s3ServiceMock.doesBucketExistV2(someNewBucketName)).thenReturn(true);
    s3FileObjectFileSpy.doAttach();
    s3FileObjectFileSpy.moveTo(newFile);

    verify(s3ServiceMock).copyObject(copyObjectRequestArgumentCaptor.capture());
    assertEquals(
        someNewBucketName, copyObjectRequestArgumentCaptor.getValue().getDestinationBucketName());
    assertEquals(someNewKey, copyObjectRequestArgumentCaptor.getValue().getDestinationKey());
    assertEquals(BUCKET_NAME, copyObjectRequestArgumentCaptor.getValue().getSourceBucketName());
    assertEquals(origKey, copyObjectRequestArgumentCaptor.getValue().getSourceKey());
  }

  @Test
  public void testDoGetLastModifiedTime() throws Exception {
    s3FileObjectFileSpy.doAttach();
    assertEquals(testDate.getTime(), s3FileObjectFileSpy.doGetLastModifiedTime());
  }

  @Test
  public void testListChildrenNotRoot() throws FileSystemException {
    fileSystemSpy.init();
    FileObject[] children = s3FileObjectBucketSpy.getChildren();
    assertTrue(children.length == 6);
    List<String> childNameArray =
        Arrays.asList(children).stream()
            .map(child -> child.getName().getPath())
            .collect(Collectors.toList());
    assertEquals(childObjectNameComp, childNameArray);
  }

  @Test
  public void testListChildrenRoot() throws FileSystemException {
    fileSystemSpy.init();
    FileObject[] children = s3FileObjectSpyRoot.getChildren();
    assertTrue(children.length == 4);
    List<String> childNameArray =
        Arrays.asList(children).stream()
            .map(child -> child.getName().getPath())
            .collect(Collectors.toList());
    assertEquals(childBucketNameListComp, childNameArray);
  }

  @Test
  public void testFixFilePathToFile() {
    String bucketName = "s3:/";
    String key = "bucketName/some/key/path";
    SimpleEntry<String, String> newPath = s3FileObjectBucketSpy.fixFilePath(key, bucketName);
    assertEquals("bucketName", newPath.getValue());
    assertEquals("some/key/path", newPath.getKey());
  }

  @Test
  public void testFixFilePathToFolder() {
    String bucketName = "s3:/";
    String key = "bucketName";
    SimpleEntry<String, String> newPath = s3FileObjectBucketSpy.fixFilePath(key, bucketName);
    assertEquals("bucketName", newPath.getValue());
    assertEquals("", newPath.getKey());
  }

  @Test
  public void testHandleAttachException() throws FileSystemException {
    String testKey = BUCKET_NAME + "/" + origKey;
    String testBucket = "badBucketName";
    AmazonS3Exception exception = new AmazonS3Exception("NoSuchKey");

    // test the case where the folder exists and contains things; no exception should be thrown
    when(s3ServiceMock.getObject(BUCKET_NAME, origKey + "/")).thenThrow(exception);
    try {
      s3FileObjectFileSpy.handleAttachException(testKey, testBucket);
    } catch (FileSystemException e) {
      fail("Caught exception " + e.getMessage());
    }
    assertEquals(FileType.FOLDER, s3FileObjectFileSpy.getType());
  }

  @Test
  public void testHandleAttachExceptionEmptyFolder() throws FileSystemException {
    String testKey = BUCKET_NAME + "/" + origKey;
    String testBucket = "badBucketName";
    AmazonS3Exception exception = new AmazonS3Exception("NoSuchKey");
    exception.setErrorCode("NoSuchKey");

    // test the case where the folder exists and contains things; no exception should be thrown
    when(s3ServiceMock.getObject(BUCKET_NAME, origKey + "/")).thenThrow(exception);
    childObjectListing = mock(ObjectListing.class);
    when(childObjectListing.getObjectSummaries()).thenReturn(new ArrayList<>());
    when(childObjectListing.getCommonPrefixes()).thenReturn(new ArrayList<>());
    when(s3ServiceMock.listObjects(any(ListObjectsRequest.class))).thenReturn(childObjectListing);
    try {
      s3FileObjectFileSpy.handleAttachException(testKey, testBucket);
    } catch (FileSystemException e) {
      fail("Caught exception " + e.getMessage());
    }
    assertEquals(FileType.IMAGINARY, s3FileObjectFileSpy.getType());
  }

  private List<Bucket> createBuckets() {
    List<Bucket> buckets = new ArrayList<>();
    buckets.add(new Bucket("bucket1"));
    buckets.add(new Bucket("bucket2"));
    buckets.add(new Bucket("bucket3"));
    buckets.add(new Bucket("bucket4"));
    childBucketNameListComp.addAll(
        buckets.stream().map(bucket -> "/" + bucket.getName()).collect(Collectors.toList()));
    return buckets;
  }

  private List<S3ObjectSummary> createObjectSummaries(int startnum) {
    List<S3ObjectSummary> summaries = new ArrayList<>();
    for (int i = startnum; i < startnum + 3; i++) {
      S3ObjectSummary summary = new S3ObjectSummary();
      summary.setBucketName(BUCKET_NAME);
      summary.setKey("key" + i);
      summaries.add(summary);
      childObjectNameComp.add(BUCKET_NAME + "/" + "key" + i);
    }
    return summaries;
  }

  private List<String> createCommonPrefixes(int startnum) {
    List<String> prefixes = new ArrayList<>();
    for (int i = startnum; i < startnum + 3; i++) {
      prefixes.add("key" + i);
      childObjectNameComp.add(BUCKET_NAME + "/" + "key" + i);
    }
    return prefixes;
  }
}
