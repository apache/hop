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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileType;
import org.junit.jupiter.api.Test;

class MinioFileNameTest {

  @Test
  void testConstructorWithFile() {
    MinioFileName fileName =
        new MinioFileName("s3", "my-bucket", "/my-bucket/file.txt", FileType.FILE);

    assertNotNull(fileName, "FileName should not be null");
    assertEquals("s3", fileName.getScheme(), "Scheme should be s3");
    assertEquals("my-bucket", fileName.getBucketId(), "Bucket ID should be my-bucket");
    assertEquals("/my-bucket/file.txt", fileName.getPath(), "Path should be correct");
    assertEquals(FileType.FILE, fileName.getType(), "Type should be FILE");
    assertEquals(
        "file.txt", fileName.getBucketRelativePath(), "Bucket relative path should be correct");
  }

  @Test
  void testConstructorWithFolder() {
    MinioFileName fileName =
        new MinioFileName("s3", "my-bucket", "/my-bucket/folder", FileType.FOLDER);

    assertEquals("s3", fileName.getScheme(), "Scheme should be s3");
    assertEquals("my-bucket", fileName.getBucketId(), "Bucket ID should be my-bucket");
    assertEquals("/my-bucket/folder", fileName.getPath(), "Path should be correct");
    assertEquals(FileType.FOLDER, fileName.getType(), "Type should be FOLDER");
    assertEquals("folder/", fileName.getBucketRelativePath(), "Folder should have trailing slash");
  }

  @Test
  void testConstructorWithRootPath() {
    MinioFileName fileName = new MinioFileName("s3", "my-bucket", "/", FileType.FOLDER);

    assertEquals("s3", fileName.getScheme(), "Scheme should be s3");
    assertEquals("my-bucket", fileName.getBucketId(), "Bucket ID should be my-bucket");
    assertEquals("/", fileName.getPath(), "Path should be root");
    assertEquals("", fileName.getBucketRelativePath(), "Root should have empty relative path");
  }

  @Test
  void testConstructorWithNestedPath() {
    MinioFileName fileName =
        new MinioFileName("s3", "my-bucket", "/my-bucket/folder/subfolder/file.txt", FileType.FILE);

    assertEquals(
        "folder/subfolder/file.txt",
        fileName.getBucketRelativePath(),
        "Nested path should be correct");
  }

  @Test
  void testConstructorWithPathWithoutBucketPrefix() {
    MinioFileName fileName = new MinioFileName("s3", "my-bucket", "/other/file.txt", FileType.FILE);

    assertEquals(
        "other/file.txt",
        fileName.getBucketRelativePath(),
        "Path without bucket prefix should be correct");
  }

  @Test
  void testGetURI() {
    MinioFileName fileName =
        new MinioFileName("s3", "my-bucket", "/my-bucket/file.txt", FileType.FILE);
    String uri = fileName.getURI();

    assertNotNull(uri, "URI should not be null");
    assertTrue(uri.startsWith("s3://"), "URI should start with scheme://");
    assertTrue(uri.contains("/my-bucket/file.txt"), "URI should contain the path");
  }

  @Test
  void testGetURIFormat() {
    MinioFileName fileName =
        new MinioFileName("s3", "test-bucket", "/test-bucket/data.csv", FileType.FILE);
    String uri = fileName.getURI();

    assertEquals(
        "s3:///test-bucket/data.csv",
        uri,
        "URI should have correct format with 2 slashes after scheme");
  }

  @Test
  void testCreateName() {
    MinioFileName fileName = new MinioFileName("s3", "my-bucket", "/my-bucket", FileType.FOLDER);
    FileName newName = fileName.createName("/my-bucket/newfile.txt", FileType.FILE);

    assertNotNull(newName, "New name should not be null");
    assertTrue(newName instanceof MinioFileName, "New name should be MinioFileName");

    MinioFileName minioNewName = (MinioFileName) newName;
    assertEquals("s3", minioNewName.getScheme(), "Scheme should be preserved");
    assertEquals("my-bucket", minioNewName.getBucketId(), "Bucket ID should be preserved");
    assertEquals("/my-bucket/newfile.txt", minioNewName.getPath(), "New path should be set");
    assertEquals(FileType.FILE, minioNewName.getType(), "New type should be FILE");
  }

  @Test
  void testCreateNameForFolder() {
    MinioFileName fileName = new MinioFileName("s3", "bucket", "/bucket", FileType.FOLDER);
    FileName newName = fileName.createName("/bucket/subfolder", FileType.FOLDER);

    MinioFileName minioNewName = (MinioFileName) newName;
    assertEquals(FileType.FOLDER, minioNewName.getType(), "New type should be FOLDER");
    assertEquals(
        "subfolder/", minioNewName.getBucketRelativePath(), "Folder should have trailing slash");
  }

  @Test
  void testDelimiterConstant() {
    assertEquals("/", MinioFileName.DELIMITER, "Delimiter should be forward slash");
  }

  @Test
  void testGetBucketId() {
    MinioFileName fileName =
        new MinioFileName("s3", "test-bucket", "/test-bucket/file.txt", FileType.FILE);
    assertEquals("test-bucket", fileName.getBucketId(), "getBucketId should return correct bucket");
  }

  @Test
  void testSetAndGetBucketRelativePath() {
    MinioFileName fileName = new MinioFileName("s3", "bucket", "/bucket/file.txt", FileType.FILE);
    fileName.setBucketRelativePath("new/path.txt");
    assertEquals(
        "new/path.txt",
        fileName.getBucketRelativePath(),
        "Should be able to set bucket relative path");
  }

  @Test
  void testPathWithMultipleSlashes() {
    MinioFileName fileName = new MinioFileName("s3", "bucket", "//bucket//file.txt", FileType.FILE);
    assertNotNull(fileName.getBucketRelativePath(), "Should handle multiple slashes");
  }

  @Test
  void testEmptyBucketName() {
    MinioFileName fileName = new MinioFileName("s3", "", "/file.txt", FileType.FILE);
    assertEquals("", fileName.getBucketId(), "Should handle empty bucket name");
    assertEquals("file.txt", fileName.getBucketRelativePath(), "Relative path should be correct");
  }
}
