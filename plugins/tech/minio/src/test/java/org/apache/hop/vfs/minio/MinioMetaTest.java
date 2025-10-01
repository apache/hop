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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.vfs.minio.metadata.MinioMeta;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MinioMetaTest {

  private MinioMeta minioMeta;

  @BeforeEach
  void setUp() {
    minioMeta = new MinioMeta();
  }

  @Test
  void testDefaultConstructor() {
    assertNotNull(minioMeta, "MinioMeta should not be null");
    assertEquals("5242880", minioMeta.getPartSize(), "Default part size should be 5MB");
  }

  @Test
  void testDescription() {
    String description = "Test MinIO connection";
    minioMeta.setDescription(description);
    assertEquals(description, minioMeta.getDescription(), "Description should be set correctly");
  }

  @Test
  void testEndPointHostname() {
    String hostname = "localhost";
    minioMeta.setEndPointHostname(hostname);
    assertEquals(
        hostname, minioMeta.getEndPointHostname(), "EndPoint hostname should be set correctly");
  }

  @Test
  void testEndPointPort() {
    String port = "9000";
    minioMeta.setEndPointPort(port);
    assertEquals(port, minioMeta.getEndPointPort(), "EndPoint port should be set correctly");
  }

  @Test
  void testEndPointSecure() {
    minioMeta.setEndPointSecure(true);
    assertTrue(minioMeta.isEndPointSecure(), "EndPoint secure should be set correctly");

    minioMeta.setEndPointSecure(false);
    assertFalse(minioMeta.isEndPointSecure(), "EndPoint secure should be set correctly");
  }

  @Test
  void testAccessKey() {
    String accessKey = "test-access-key";
    minioMeta.setAccessKey(accessKey);
    assertEquals(accessKey, minioMeta.getAccessKey(), "Access key should be set correctly");
  }

  @Test
  void testSecretKey() {
    String secretKey = "test-secret-key";
    minioMeta.setSecretKey(secretKey);
    assertEquals(secretKey, minioMeta.getSecretKey(), "Secret key should be set correctly");
  }

  @Test
  void testRegion() {
    String region = "us-east-1";
    minioMeta.setRegion(region);
    assertEquals(region, minioMeta.getRegion(), "Region should be set correctly");
  }

  @Test
  void testPartSize() {
    String partSize = "10485760"; // 10MB
    minioMeta.setPartSize(partSize);
    assertEquals(partSize, minioMeta.getPartSize(), "Part size should be set correctly");
  }

  @Test
  void testAllProperties() {
    String description = "Test MinIO connection";
    String hostname = "minio.example.com";
    String port = "443";
    boolean secure = true;
    String accessKey = "test-access-key";
    String secretKey = "test-secret-key";
    String region = "us-west-2";
    String partSize = "10485760";

    minioMeta.setDescription(description);
    minioMeta.setEndPointHostname(hostname);
    minioMeta.setEndPointPort(port);
    minioMeta.setEndPointSecure(secure);
    minioMeta.setAccessKey(accessKey);
    minioMeta.setSecretKey(secretKey);
    minioMeta.setRegion(region);
    minioMeta.setPartSize(partSize);

    assertEquals(description, minioMeta.getDescription(), "Description should be set correctly");
    assertEquals(
        hostname, minioMeta.getEndPointHostname(), "EndPoint hostname should be set correctly");
    assertEquals(port, minioMeta.getEndPointPort(), "EndPoint port should be set correctly");
    assertTrue(minioMeta.isEndPointSecure(), "EndPoint secure should be set correctly");
    assertEquals(accessKey, minioMeta.getAccessKey(), "Access key should be set correctly");
    assertEquals(secretKey, minioMeta.getSecretKey(), "Secret key should be set correctly");
    assertEquals(region, minioMeta.getRegion(), "Region should be set correctly");
    assertEquals(partSize, minioMeta.getPartSize(), "Part size should be set correctly");
  }

  @Test
  void testNullValues() {
    minioMeta.setDescription(null);
    minioMeta.setEndPointHostname(null);
    minioMeta.setEndPointPort(null);
    minioMeta.setAccessKey(null);
    minioMeta.setSecretKey(null);
    minioMeta.setRegion(null);
    minioMeta.setPartSize(null);

    assertNull(minioMeta.getDescription(), "Description should be null");
    assertNull(minioMeta.getEndPointHostname(), "EndPoint hostname should be null");
    assertNull(minioMeta.getEndPointPort(), "EndPoint port should be null");
    assertNull(minioMeta.getAccessKey(), "Access key should be null");
    assertNull(minioMeta.getSecretKey(), "Secret key should be null");
    assertNull(minioMeta.getRegion(), "Region should be null");
    assertNull(minioMeta.getPartSize(), "Part size should be null");
  }
}
