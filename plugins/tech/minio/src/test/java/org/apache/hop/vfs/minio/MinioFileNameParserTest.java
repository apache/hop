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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MinioFileNameParserTest {

  private MinioFileNameParser parser;

  @BeforeEach
  void setUp() {
    parser = new MinioFileNameParser();
  }

  @Test
  void testGetInstance() {
    MinioFileNameParser instance1 = (MinioFileNameParser) MinioFileNameParser.getInstance();
    MinioFileNameParser instance2 = (MinioFileNameParser) MinioFileNameParser.getInstance();

    assertNotNull(instance1, "Instance should not be null");
    assertSame(instance1, instance2, "getInstance should return singleton");
  }

  @Test
  void testParseSimpleFileURI() throws FileSystemException {
    String uri = "s3://bucket/file.txt";
    FileName fileName = parser.parseUri(null, null, uri);

    assertNotNull(fileName, "Parsed FileName should not be null");
    assertTrue(fileName instanceof MinioFileName, "Should be MinioFileName");

    MinioFileName minioFileName = (MinioFileName) fileName;
    assertEquals("s3", minioFileName.getScheme(), "Scheme should be s3");
    assertEquals("bucket", minioFileName.getBucketId(), "Bucket should be extracted");
    assertEquals("/bucket/file.txt", minioFileName.getPath(), "Path should be normalized");
  }

  @Test
  void testParseFolderURI() throws FileSystemException {
    String uri = "s3://my-bucket/folder/";
    FileName fileName = parser.parseUri(null, null, uri);

    MinioFileName minioFileName = (MinioFileName) fileName;
    assertEquals("s3", minioFileName.getScheme(), "Scheme should be s3");
    assertEquals("my-bucket", minioFileName.getBucketId(), "Bucket should be my-bucket");
    assertEquals(
        FileType.FOLDER, minioFileName.getType(), "Type should be FOLDER for trailing slash");
  }

  @Test
  void testParseNestedPathURI() throws FileSystemException {
    String uri = "s3://data-bucket/reports/2024/january/sales.csv";
    FileName fileName = parser.parseUri(null, null, uri);

    MinioFileName minioFileName = (MinioFileName) fileName;
    assertEquals("data-bucket", minioFileName.getBucketId(), "Bucket should be data-bucket");
    assertEquals(
        "/data-bucket/reports/2024/january/sales.csv",
        minioFileName.getPath(),
        "Nested path should be preserved");
    assertEquals(FileType.FILE, minioFileName.getType(), "Type should be FILE");
  }

  @Test
  void testParseRootBucketURI() throws FileSystemException {
    String uri = "s3://my-bucket";
    FileName fileName = parser.parseUri(null, null, uri);

    MinioFileName minioFileName = (MinioFileName) fileName;
    assertEquals("my-bucket", minioFileName.getBucketId(), "Bucket should be my-bucket");
    assertEquals("/my-bucket", minioFileName.getPath(), "Path should be bucket root");
  }

  @Test
  void testParseURIWithSpecialCharacters() throws FileSystemException {
    String uri = "s3://bucket/path/with-dashes_and_underscores.txt";
    FileName fileName = parser.parseUri(null, null, uri);

    MinioFileName minioFileName = (MinioFileName) fileName;
    assertTrue(
        minioFileName.getPath().contains("with-dashes_and_underscores"),
        "Should preserve special characters");
  }

  @Test
  void testParseMinioScheme() throws FileSystemException {
    String uri = "minio://my-bucket/file.txt";
    FileName fileName = parser.parseUri(null, null, uri);

    MinioFileName minioFileName = (MinioFileName) fileName;
    assertEquals("minio", minioFileName.getScheme(), "Scheme should be minio");
    assertEquals("my-bucket", minioFileName.getBucketId(), "Bucket should be extracted");
  }

  @Test
  void testParseURIWithMultipleSlashes() throws FileSystemException {
    String uri = "s3://bucket//folder//file.txt";
    FileName fileName = parser.parseUri(null, null, uri);

    assertNotNull(fileName, "Should handle multiple slashes");
    MinioFileName minioFileName = (MinioFileName) fileName;
    assertEquals("bucket", minioFileName.getBucketId(), "Bucket should be extracted correctly");
  }

  @Test
  void testParserConstructor() {
    MinioFileNameParser newParser = new MinioFileNameParser();
    assertNotNull(newParser, "Constructor should create new instance");
  }

  @Test
  void testParseFileWithExtension() throws FileSystemException {
    String uri = "s3://documents/report.pdf";
    FileName fileName = parser.parseUri(null, null, uri);

    MinioFileName minioFileName = (MinioFileName) fileName;
    assertEquals(
        "/documents/report.pdf", minioFileName.getPath(), "Should preserve file extension");
    assertEquals(FileType.FILE, minioFileName.getType(), "Should be FILE type");
  }

  @Test
  void testParseDeepNestedPath() throws FileSystemException {
    String uri = "s3://bucket/a/b/c/d/e/f/file.txt";
    FileName fileName = parser.parseUri(null, null, uri);

    MinioFileName minioFileName = (MinioFileName) fileName;
    assertEquals("bucket", minioFileName.getBucketId(), "Bucket should be first element");
    assertTrue(minioFileName.getPath().contains("/a/b/c/d/e/f/"), "Deep path should be preserved");
  }

  @Test
  void testParseBucketWithNumbers() throws FileSystemException {
    String uri = "s3://bucket123/file456.txt";
    FileName fileName = parser.parseUri(null, null, uri);

    MinioFileName minioFileName = (MinioFileName) fileName;
    assertEquals("bucket123", minioFileName.getBucketId(), "Should handle bucket with numbers");
  }

  @Test
  void testParseFileWithoutExtension() throws FileSystemException {
    String uri = "s3://bucket/README";
    FileName fileName = parser.parseUri(null, null, uri);

    MinioFileName minioFileName = (MinioFileName) fileName;
    assertEquals("/bucket/README", minioFileName.getPath(), "Should handle file without extension");
    assertEquals(FileType.FILE, minioFileName.getType(), "Should be FILE type");
  }

  @Test
  void testParseWithDotInPath() throws FileSystemException {
    String uri = "s3://bucket/folder.with.dots/file.txt";
    FileName fileName = parser.parseUri(null, null, uri);

    MinioFileName minioFileName = (MinioFileName) fileName;
    assertTrue(
        minioFileName.getPath().contains("folder.with.dots"),
        "Should preserve dots in folder names");
  }
}
