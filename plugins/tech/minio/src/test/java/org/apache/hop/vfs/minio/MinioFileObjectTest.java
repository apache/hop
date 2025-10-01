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

package org.apache.hop.vfs.minio;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(RestoreHopEnvironmentExtension.class)
class MinioFileObjectTest {

  private MinioFileObject createFileObject(String bucketName, String path, FileType type) {
    MinioFileName rootName = new MinioFileName("s3", bucketName, "/" + bucketName, FileType.FOLDER);
    MinioFileSystem fileSystem = new MinioFileSystem(rootName, new FileSystemOptions());
    MinioFileName fileName = new MinioFileName("s3", bucketName, path, type);
    return new MinioFileObject(fileName, fileSystem);
  }

  @Test
  void testConstructor() {
    MinioFileObject fileObject =
        createFileObject("test-bucket", "/test-bucket/file.txt", FileType.FILE);
    assertNotNull(fileObject, "FileObject should not be null");
    assertEquals("test-bucket", fileObject.bucketName, "Bucket name should be set");
  }

  @Test
  void testGetMinioBucketNameForFile() {
    MinioFileObject fileObject =
        createFileObject("my-bucket", "/my-bucket/folder/file.txt", FileType.FILE);
    String bucketName = fileObject.getMinioBucketName();
    assertEquals("my-bucket", bucketName, "Should extract bucket name from file path");
  }

  @Test
  void testGetMinioBucketNameForBucket() {
    MinioFileObject fileObject = createFileObject("my-bucket", "/my-bucket", FileType.FOLDER);
    String bucketName = fileObject.getMinioBucketName();
    assertEquals("my-bucket", bucketName, "Should extract bucket name");
  }

  @Test
  void testGetMinioBucketNameWithLeadingSlash() {
    MinioFileObject fileObject = createFileObject("bucket", "/bucket/file.txt", FileType.FILE);
    String bucketName = fileObject.getMinioBucketName();
    assertFalse(bucketName.startsWith("/"), "Bucket name should not start with slash");
    assertEquals("bucket", bucketName, "Should remove leading slash");
  }

  @Test
  void testIsRootBucket() {
    MinioFileName rootName = new MinioFileName("s3", "", "/", FileType.FOLDER);
    MinioFileSystem fileSystem = new MinioFileSystem(rootName, new FileSystemOptions());
    MinioFileObject rootObject = new MinioFileObject(rootName, fileSystem);

    assertTrue(rootObject.isRootBucket(), "Should be root bucket");
  }

  @Test
  void testIsNotRootBucket() {
    MinioFileObject fileObject =
        createFileObject("test-bucket", "/test-bucket/file.txt", FileType.FILE);
    assertFalse(fileObject.isRootBucket(), "Should not be root bucket");
  }

  @Test
  void testGetBucketRelativePath() {
    MinioFileObject fileObject =
        createFileObject("test-bucket", "/test-bucket/file.txt", FileType.FILE);
    String relativePath = fileObject.getBucketRelativePath();
    assertEquals("file.txt", relativePath, "Should get relative path");
  }

  @Test
  void testGetBucketRelativePathForNestedFile() {
    MinioFileObject fileObject =
        createFileObject("bucket", "/bucket/folder/subfolder/file.txt", FileType.FILE);
    String relativePath = fileObject.getBucketRelativePath();
    assertEquals("folder/subfolder/file.txt", relativePath, "Should get nested relative path");
  }

  @Test
  void testGetBucketRelativePathForBucket() {
    MinioFileObject fileObject = createFileObject("bucket", "/bucket", FileType.FOLDER);
    String relativePath = fileObject.getBucketRelativePath();
    assertEquals("", relativePath, "Bucket should have empty relative path");
  }

  @Test
  void testGetQualifiedName() {
    MinioFileObject fileObject =
        createFileObject("test-bucket", "/test-bucket/file.txt", FileType.FILE);
    String qualifiedName = fileObject.getQualifiedName();
    assertNotNull(qualifiedName, "Qualified name should not be null");
    assertTrue(qualifiedName.contains("test-bucket"), "Should contain bucket name");
  }

  @Test
  void testDelimiterConstant() {
    assertEquals("/", MinioFileObject.DELIMITER, "Delimiter should be forward slash");
  }

  @Test
  void testPrefixConstant() {
    assertEquals("minio:///", MinioFileObject.PREFIX, "Prefix should be minio:///");
  }

  @Test
  void testBucketNameField() {
    MinioFileObject fileObject =
        createFileObject("test-bucket", "/test-bucket/file.txt", FileType.FILE);
    assertEquals("test-bucket", fileObject.bucketName, "Bucket name field should be set");
  }

  @Test
  void testKeyField() {
    MinioFileObject fileObject =
        createFileObject("test-bucket", "/test-bucket/file.txt", FileType.FILE);
    assertEquals("file.txt", fileObject.key, "Key field should be set");
  }

  @Test
  void testAttachedFieldDefaultValue() {
    MinioFileObject fileObject =
        createFileObject("test-bucket", "/test-bucket/file.txt", FileType.FILE);
    assertFalse(fileObject.attached, "Attached should be false by default");
  }

  @Test
  void testGetMinioBucketNameWithMultipleDelimiters() {
    MinioFileObject fileObject =
        createFileObject("bucket", "/bucket/a/b/c/file.txt", FileType.FILE);
    String bucketName = fileObject.getMinioBucketName();
    assertEquals("bucket", bucketName, "Should extract first segment as bucket");
  }

  @Test
  void testFileObjectWithDifferentSchemes() {
    MinioFileObject s3Object = createFileObject("bucket", "/bucket/file.txt", FileType.FILE);
    assertEquals("bucket", s3Object.getMinioBucketName(), "S3 scheme should work");
  }

  @Test
  void testGetMinioBucketNameConsistency() {
    MinioFileObject fileObject =
        createFileObject("test-bucket", "/test-bucket/file.txt", FileType.FILE);
    String bucketName1 = fileObject.getMinioBucketName();
    String bucketName2 = fileObject.getMinioBucketName();

    assertEquals(bucketName1, bucketName2, "Multiple calls should return same bucket name");
  }

  @Test
  void testGetBucketRelativePathForSingleLevelFile() {
    MinioFileObject fileObject = createFileObject("bucket", "/bucket/file.txt", FileType.FILE);
    String relativePath = fileObject.getBucketRelativePath();
    assertEquals("file.txt", relativePath, "Should get file name as relative path");
  }

  @Test
  void testFileObjectWithComplexPath() {
    MinioFileObject fileObject =
        createFileObject(
            "my-data-bucket", "/my-data-bucket/reports/2024/january/sales.csv", FileType.FILE);

    assertEquals(
        "my-data-bucket", fileObject.getMinioBucketName(), "Should extract bucket correctly");
    assertEquals(
        "reports/2024/january/sales.csv",
        fileObject.getBucketRelativePath(),
        "Should extract full relative path");
  }

  @Test
  void testIsRootBucketWithActualBucket() {
    MinioFileObject fileObject =
        createFileObject("actual-bucket", "/actual-bucket", FileType.FOLDER);
    assertFalse(fileObject.isRootBucket(), "Actual bucket should not be root");
  }
}
