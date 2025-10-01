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

import java.util.Collection;
import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.FileNameParser;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.apache.hop.vfs.minio.metadata.MinioMeta;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(RestoreHopEnvironmentExtension.class)
class MinioFileProviderTest {

  private MinioFileProvider provider;

  @BeforeEach
  void setUp() {
    provider = new MinioFileProvider();
  }

  @Test
  void testDefaultConstructor() {
    assertNotNull(provider, "Provider should not be null");
  }

  @Test
  void testConstructorWithMetadata() {
    IVariables variables = new Variables();
    MinioMeta meta = new MinioMeta();
    meta.setEndPointHostname("localhost");
    meta.setEndPointPort("9000");
    meta.setAccessKey("admin");
    meta.setSecretKey("password");

    MinioFileProvider providerWithMeta = new MinioFileProvider(variables, meta);
    assertNotNull(providerWithMeta, "Provider with metadata should not be null");
  }

  @Test
  void testFileNameParserIsSingleton() {
    FileNameParser parser1 = MinioFileNameParser.getInstance();
    FileNameParser parser2 = MinioFileNameParser.getInstance();
    assertNotNull(parser1, "FileNameParser should not be null");
    assertEquals(parser1, parser2, "Should return same parser instance");
  }

  @Test
  void testGetCapabilities() {
    Collection<Capability> capabilities = provider.getCapabilities();

    assertNotNull(capabilities, "Capabilities should not be null");
    assertTrue(capabilities.size() > 0, "Should have capabilities");
    assertTrue(capabilities.contains(Capability.CREATE), "Should contain CREATE");
    assertTrue(capabilities.contains(Capability.DELETE), "Should contain DELETE");
    assertTrue(capabilities.contains(Capability.RENAME), "Should contain RENAME");
    assertTrue(capabilities.contains(Capability.GET_TYPE), "Should contain GET_TYPE");
    assertTrue(capabilities.contains(Capability.LIST_CHILDREN), "Should contain LIST_CHILDREN");
    assertTrue(capabilities.contains(Capability.READ_CONTENT), "Should contain READ_CONTENT");
    assertTrue(capabilities.contains(Capability.WRITE_CONTENT), "Should contain WRITE_CONTENT");
  }

  @Test
  void testCreateFileSystemWithoutMetadata() {
    MinioFileName fileName = new MinioFileName("s3", "bucket", "/bucket", FileType.FOLDER);
    FileSystem fileSystem = provider.doCreateFileSystem(fileName, null);

    assertNotNull(fileSystem, "FileSystem should not be null");
    assertTrue(fileSystem instanceof MinioFileSystem, "Should be MinioFileSystem");
  }

  @Test
  void testCreateFileSystemWithDefaultOptions() {
    MinioFileName fileName = new MinioFileName("s3", "bucket", "/bucket", FileType.FOLDER);
    FileSystemOptions options = MinioFileProvider.getDefaultFileSystemOptions();
    FileSystem fileSystem = provider.doCreateFileSystem(fileName, options);

    assertNotNull(fileSystem, "FileSystem should not be null");
    assertTrue(fileSystem instanceof MinioFileSystem, "Should be MinioFileSystem");
  }

  @Test
  void testCreateFileSystemWithMetadata() {
    IVariables variables = new Variables();
    variables.setVariable("MINIO_HOST", "localhost");
    variables.setVariable("MINIO_PORT", "9000");

    MinioMeta meta = new MinioMeta();
    meta.setEndPointHostname("${MINIO_HOST}");
    meta.setEndPointPort("${MINIO_PORT}");
    meta.setEndPointSecure(false);
    meta.setAccessKey("minioadmin");
    meta.setSecretKey("minioadmin");
    meta.setRegion("us-east-1");
    meta.setPartSize("10485760");

    MinioFileProvider providerWithMeta = new MinioFileProvider(variables, meta);
    MinioFileName fileName = new MinioFileName("s3", "bucket", "/bucket", FileType.FOLDER);
    FileSystem fileSystem = providerWithMeta.doCreateFileSystem(fileName, null);

    assertNotNull(fileSystem, "FileSystem should not be null");
    assertTrue(fileSystem instanceof MinioFileSystem, "Should be MinioFileSystem");

    MinioFileSystem minioFs = (MinioFileSystem) fileSystem;
    assertEquals("localhost", minioFs.getEndPointHostname(), "Hostname should be resolved");
    assertEquals(9000, minioFs.getEndPointPort(), "Port should be resolved");
    assertEquals(false, minioFs.isEndPointSecure(), "Secure should be set");
    assertEquals("minioadmin", minioFs.getAccessKey(), "Access key should be set");
    assertEquals("minioadmin", minioFs.getSecretKey(), "Secret key should be set");
    assertEquals("us-east-1", minioFs.getRegion(), "Region should be set");
  }

  @Test
  void testGetDefaultFileSystemOptions() {
    FileSystemOptions options = MinioFileProvider.getDefaultFileSystemOptions();
    assertNotNull(options, "Default options should not be null");
  }

  @Test
  void testDefaultPartSizeConstant() {
    assertEquals(5242880L, MinioFileProvider.DEFAULT_PART_SIZE, "Default part size should be 5MB");
  }

  @Test
  void testAuthenticatorTypes() {
    assertNotNull(MinioFileProvider.AUTHENTICATOR_TYPES, "Authenticator types should not be null");
    assertEquals(
        2, MinioFileProvider.AUTHENTICATOR_TYPES.length, "Should have 2 authenticator types");
  }

  @Test
  void testCreateFileSystemWithEmptyMetadata() {
    IVariables variables = new Variables();
    MinioMeta meta = new MinioMeta();

    MinioFileProvider providerWithMeta = new MinioFileProvider(variables, meta);
    MinioFileName fileName = new MinioFileName("s3", "bucket", "/bucket", FileType.FOLDER);
    FileSystem fileSystem = providerWithMeta.doCreateFileSystem(fileName, null);

    assertNotNull(fileSystem, "FileSystem should be created even with empty metadata");
  }

  @Test
  void testCreateFileSystemWithVariableResolution() {
    IVariables variables = new Variables();
    variables.setVariable("MY_KEY", "resolved-key");
    variables.setVariable("MY_SECRET", "resolved-secret");

    MinioMeta meta = new MinioMeta();
    meta.setAccessKey("${MY_KEY}");
    meta.setSecretKey("${MY_SECRET}");

    MinioFileProvider providerWithMeta = new MinioFileProvider(variables, meta);
    MinioFileName fileName = new MinioFileName("s3", "bucket", "/bucket", FileType.FOLDER);
    FileSystem fileSystem = providerWithMeta.doCreateFileSystem(fileName, null);

    MinioFileSystem minioFs = (MinioFileSystem) fileSystem;
    assertEquals("resolved-key", minioFs.getAccessKey(), "Variables should be resolved");
    assertEquals("resolved-secret", minioFs.getSecretKey(), "Variables should be resolved");
  }

  @Test
  void testCreateFileSystemWithDefaultPort() {
    IVariables variables = new Variables();
    MinioMeta meta = new MinioMeta();
    meta.setEndPointHostname("localhost");
    meta.setEndPointPort("invalid"); // Invalid port should default to 9000

    MinioFileProvider providerWithMeta = new MinioFileProvider(variables, meta);
    MinioFileName fileName = new MinioFileName("s3", "bucket", "/bucket", FileType.FOLDER);
    FileSystem fileSystem = providerWithMeta.doCreateFileSystem(fileName, null);

    MinioFileSystem minioFs = (MinioFileSystem) fileSystem;
    assertEquals(9000, minioFs.getEndPointPort(), "Invalid port should default to 9000");
  }

  @Test
  void testCapabilitiesMatchFileSystem() {
    Collection<Capability> providerCaps = provider.getCapabilities();

    assertEquals(
        MinioFileSystem.CAPABILITIES,
        providerCaps,
        "Provider capabilities should match FileSystem capabilities");
  }

  @Test
  void testMultipleFileSystemCreation() {
    MinioFileName fileName1 = new MinioFileName("s3", "bucket1", "/bucket1", FileType.FOLDER);
    MinioFileName fileName2 = new MinioFileName("s3", "bucket2", "/bucket2", FileType.FOLDER);

    FileSystem fs1 = provider.doCreateFileSystem(fileName1, null);
    FileSystem fs2 = provider.doCreateFileSystem(fileName2, null);

    assertNotNull(fs1, "First FileSystem should not be null");
    assertNotNull(fs2, "Second FileSystem should not be null");
  }
}
