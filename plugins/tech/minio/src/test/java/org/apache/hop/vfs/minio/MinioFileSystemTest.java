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

import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.util.StorageUnitConverter;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(RestoreHopEnvironmentExtension.class)
class MinioFileSystemTest {

  private MinioFileSystem fileSystem;
  private MinioFileName rootName;

  @BeforeEach
  void setUp() {
    rootName = new MinioFileName("s3", "test-bucket", "/test-bucket", FileType.FOLDER);
    fileSystem = new MinioFileSystem(rootName, new FileSystemOptions());
  }

  @Test
  void testConstructor() {
    assertNotNull(fileSystem, "FileSystem should not be null");
    assertNotNull(fileSystem.getRootName(), "Root name should not be null");
  }

  @Test
  void testCapabilitiesList() {
    assertNotNull(MinioFileSystem.CAPABILITIES, "CAPABILITIES list should not be null");
    assertTrue(MinioFileSystem.CAPABILITIES.size() > 0, "Should have capabilities");
    assertTrue(MinioFileSystem.CAPABILITIES.contains(Capability.CREATE), "Should contain CREATE");
    assertTrue(MinioFileSystem.CAPABILITIES.contains(Capability.DELETE), "Should contain DELETE");
    assertTrue(MinioFileSystem.CAPABILITIES.contains(Capability.RENAME), "Should contain RENAME");
    assertTrue(
        MinioFileSystem.CAPABILITIES.contains(Capability.GET_TYPE), "Should contain GET_TYPE");
    assertTrue(
        MinioFileSystem.CAPABILITIES.contains(Capability.LIST_CHILDREN),
        "Should contain LIST_CHILDREN");
    assertTrue(
        MinioFileSystem.CAPABILITIES.contains(Capability.READ_CONTENT),
        "Should contain READ_CONTENT");
    assertTrue(MinioFileSystem.CAPABILITIES.contains(Capability.URI), "Should contain URI");
    assertTrue(
        MinioFileSystem.CAPABILITIES.contains(Capability.WRITE_CONTENT),
        "Should contain WRITE_CONTENT");
    assertTrue(
        MinioFileSystem.CAPABILITIES.contains(Capability.GET_LAST_MODIFIED),
        "Should contain GET_LAST_MODIFIED");
    assertTrue(
        MinioFileSystem.CAPABILITIES.contains(Capability.RANDOM_ACCESS_READ),
        "Should contain RANDOM_ACCESS_READ");
  }

  @Test
  void testCapabilitiesDoesNotContainUnsupportedOnes() {
    assertFalse(
        MinioFileSystem.CAPABILITIES.contains(Capability.COMPRESS), "Should not contain COMPRESS");
    assertFalse(
        MinioFileSystem.CAPABILITIES.contains(Capability.SIGNING), "Should not contain SIGNING");
  }

  @Test
  void testSetAndGetEndPointHostname() {
    fileSystem.setEndPointHostname("minio.example.com");
    assertEquals("minio.example.com", fileSystem.getEndPointHostname(), "Hostname should be set");
  }

  @Test
  void testSetAndGetEndPointPort() {
    fileSystem.setEndPointPort(9000);
    assertEquals(9000, fileSystem.getEndPointPort(), "Port should be set");
  }

  @Test
  void testSetAndGetEndPointSecure() {
    fileSystem.setEndPointSecure(true);
    assertTrue(fileSystem.isEndPointSecure(), "Secure should be true");

    fileSystem.setEndPointSecure(false);
    assertFalse(fileSystem.isEndPointSecure(), "Secure should be false");
  }

  @Test
  void testSetAndGetAccessKey() {
    fileSystem.setAccessKey("test-access-key");
    assertEquals("test-access-key", fileSystem.getAccessKey(), "Access key should be set");
  }

  @Test
  void testSetAndGetSecretKey() {
    fileSystem.setSecretKey("test-secret-key");
    assertEquals("test-secret-key", fileSystem.getSecretKey(), "Secret key should be set");
  }

  @Test
  void testSetAndGetRegion() {
    fileSystem.setRegion("us-east-1");
    assertEquals("us-east-1", fileSystem.getRegion(), "Region should be set");
  }

  @Test
  void testSetAndGetPartSize() {
    fileSystem.setPartSize(10485760L); // 10MB
    assertEquals(10485760L, fileSystem.getPartSize(), "Part size should be set");
  }

  @Test
  void testCreateFile() throws Exception {
    MinioFileName fileName =
        new MinioFileName("s3", "test-bucket", "/test-bucket/file.txt", FileType.FILE);
    Object fileObject = fileSystem.createFile(fileName);

    assertNotNull(fileObject, "File object should not be null");
    assertTrue(fileObject instanceof MinioFileObject, "Should be MinioFileObject");
  }

  @Test
  void testParsePartSizeWithValidSize() {
    fileSystem.setStorageUnitConverter(new StorageUnitConverter());
    long partSize = fileSystem.parsePartSize("10MB");

    assertEquals(10485760L, partSize, "10MB should be 10485760 bytes");
  }

  @Test
  void testParsePartSizeWithMinimum() {
    fileSystem.setStorageUnitConverter(new StorageUnitConverter());
    long partSize = fileSystem.parsePartSize("5MB");

    assertEquals(5242880L, partSize, "5MB should be 5242880 bytes");
  }

  @Test
  void testParsePartSizeBelowMinimum() {
    fileSystem.setStorageUnitConverter(new StorageUnitConverter());
    long partSize = fileSystem.parsePartSize("1MB");

    // Should default to minimum 5MB
    assertEquals(5242880L, partSize, "Should default to 5MB minimum");
  }

  @Test
  void testParsePartSizeWithMaximum() {
    fileSystem.setStorageUnitConverter(new StorageUnitConverter());
    long partSize = fileSystem.parsePartSize("5GB");

    assertEquals(5368709120L, partSize, "5GB should be 5368709120 bytes");
  }

  @Test
  void testParsePartSizeAboveMaximum() {
    fileSystem.setStorageUnitConverter(new StorageUnitConverter());
    long partSize = fileSystem.parsePartSize("10GB");

    // Should still parse but log warning
    assertEquals(10737418240L, partSize, "Should parse 10GB even though it's above max");
  }

  @Test
  void testConvertToLong() {
    fileSystem.setStorageUnitConverter(new StorageUnitConverter());

    assertEquals(1024L, fileSystem.convertToLong("1KB"), "1KB should be 1024 bytes");
    assertEquals(1048576L, fileSystem.convertToLong("1MB"), "1MB should be 1048576 bytes");
    assertEquals(1073741824L, fileSystem.convertToLong("1GB"), "1GB should be 1073741824 bytes");
  }

  @Test
  void testGetClient() {
    fileSystem.setEndPointHostname("localhost");
    fileSystem.setEndPointPort(9000);
    fileSystem.setEndPointSecure(false);
    fileSystem.setAccessKey("minioadmin");
    fileSystem.setSecretKey("minioadmin");
    fileSystem.setRegion("us-east-1");

    assertNotNull(fileSystem.getClient(), "Client should not be null");

    // Calling again should return same instance
    Object client1 = fileSystem.getClient();
    Object client2 = fileSystem.getClient();
    assertEquals(client1, client2, "Should return same client instance");
  }

  @Test
  void testCapabilitiesConstant() {
    assertNotNull(MinioFileSystem.CAPABILITIES, "CAPABILITIES list should not be null");
    assertEquals(10, MinioFileSystem.CAPABILITIES.size(), "Should have 10 capabilities");
  }

  @Test
  void testFileSystemWithNullOptions() {
    MinioFileSystem fs = new MinioFileSystem(rootName, null);
    assertNotNull(fs, "FileSystem should be created with null options");
  }

  @Test
  void testSetClient() {
    fileSystem.setEndPointHostname("localhost");
    fileSystem.setEndPointPort(9000);
    fileSystem.setEndPointSecure(false);
    fileSystem.setAccessKey("test");
    fileSystem.setSecretKey("test");

    Object client = fileSystem.getClient();
    assertNotNull(client, "Client should be created");
  }

  @Test
  void testDefaultPartSize() {
    fileSystem.setPartSize(MinioFileProvider.DEFAULT_PART_SIZE);
    assertEquals(5242880L, fileSystem.getPartSize(), "Default part size should be 5MB");
  }
}
