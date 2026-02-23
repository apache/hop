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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.vfs.s3.metadata.S3AuthType;
import org.apache.hop.vfs.s3.metadata.S3Meta;
import org.apache.hop.vfs.s3.s3.vfs.S3FileProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for S3FileProvider */
class S3FileProviderTest {

  S3FileProvider provider;
  FileName fileName;
  IVariables variables;

  @BeforeEach
  void setUp() {
    provider = new S3FileProvider();
    fileName = mock(FileName.class);
    variables = new Variables();
  }

  @Test
  void testDoCreateFileSystem() {
    FileSystemOptions options = new FileSystemOptions();
    assertNotNull(provider.doCreateFileSystem(fileName, options));
  }

  @Test
  void testDoCreateFileSystemWithAccessKeys() {
    S3Meta meta = new S3Meta();
    meta.setAuthenticationType(S3AuthType.ACCESS_KEYS.name());
    meta.setAccessKey("myAccessKey");
    meta.setSecretKey("mySecretKey");
    meta.setSessionToken("myToken");
    meta.setRegion("us-west-2");

    S3FileProvider namedProvider = new S3FileProvider(variables, meta);
    FileSystem fs = namedProvider.doCreateFileSystem(fileName, null);
    assertNotNull(fs);
  }

  @Test
  void testDoCreateFileSystemWithCredentialsFile() {
    S3Meta meta = new S3Meta();
    meta.setAuthenticationType(S3AuthType.CREDENTIALS_FILE.name());
    meta.setCredentialsFile("/some/path/credentials");
    meta.setProfileName("myprofile");

    S3FileProvider namedProvider = new S3FileProvider(variables, meta);
    FileSystem fs = namedProvider.doCreateFileSystem(fileName, null);
    assertNotNull(fs);
  }

  @Test
  void testDoCreateFileSystemWithDefaultAuthAndProfile() {
    S3Meta meta = new S3Meta();
    meta.setAuthenticationType(S3AuthType.DEFAULT.name());
    meta.setProfileName("minio");

    S3FileProvider namedProvider = new S3FileProvider(variables, meta);
    FileSystem fs = namedProvider.doCreateFileSystem(fileName, null);
    assertNotNull(fs);
  }

  @Test
  void testDoCreateFileSystemClearsFieldsForAccessKeys() {
    S3Meta meta = new S3Meta();
    meta.setAuthenticationType(S3AuthType.ACCESS_KEYS.name());
    meta.setAccessKey("key");
    meta.setSecretKey("secret");
    meta.setProfileName(null);
    meta.setCredentialsFile(null);

    S3FileProvider namedProvider = new S3FileProvider(variables, meta);
    FileSystem fs = namedProvider.doCreateFileSystem(fileName, null);
    assertNotNull(fs);
  }

  @Test
  void testDoCreateFileSystemWithEndpoint() {
    S3Meta meta = new S3Meta();
    meta.setAuthenticationType(S3AuthType.DEFAULT.name());
    meta.setEndpoint("http://localhost:9000");

    S3FileProvider namedProvider = new S3FileProvider(variables, meta);
    FileSystem fs = namedProvider.doCreateFileSystem(fileName, null);
    assertNotNull(fs);
  }

  @Test
  void testDoCreateFileSystemCacheTtl() {
    S3Meta meta = new S3Meta();
    meta.setAuthenticationType(S3AuthType.DEFAULT.name());
    meta.setCacheTtlSeconds("15");

    S3FileProvider namedProvider = new S3FileProvider(variables, meta);
    FileSystem fs = namedProvider.doCreateFileSystem(fileName, null);
    assertNotNull(fs);
  }

  @Test
  void testDoCreateFileSystemInferredAuthType() {
    S3Meta meta = new S3Meta();
    meta.setAuthenticationType(null);
    meta.setAccessKey("key");
    meta.setSecretKey("secret");

    S3FileProvider namedProvider = new S3FileProvider(variables, meta);
    FileSystem fs = namedProvider.doCreateFileSystem(fileName, null);
    assertNotNull(fs);
  }

  @Test
  void testDoCreateFileSystemInferredAuthTypeCredentials() {
    S3Meta meta = new S3Meta();
    meta.setAuthenticationType(null);
    meta.setCredentialsFile("/path/to/credentials");

    S3FileProvider namedProvider = new S3FileProvider(variables, meta);
    FileSystem fs = namedProvider.doCreateFileSystem(fileName, null);
    assertNotNull(fs);
  }

  @Test
  void testDoCreateFileSystemInferredAuthTypeDefault() {
    S3Meta meta = new S3Meta();
    meta.setAuthenticationType(null);

    S3FileProvider namedProvider = new S3FileProvider(variables, meta);
    FileSystem fs = namedProvider.doCreateFileSystem(fileName, null);
    assertNotNull(fs);
  }

  @Test
  void testDoCreateFileSystemWithPathStyleAccess() {
    S3Meta meta = new S3Meta();
    meta.setAuthenticationType(S3AuthType.DEFAULT.name());
    meta.setPathStyleAccess(true);
    meta.setEndpoint("http://localhost:9000");

    S3FileProvider namedProvider = new S3FileProvider(variables, meta);
    FileSystem fs = namedProvider.doCreateFileSystem(fileName, null);
    assertNotNull(fs);
  }

  @Test
  void testDoCreateFileSystemWithAnonymousAuth() {
    S3Meta meta = new S3Meta();
    meta.setAuthenticationType(S3AuthType.ANONYMOUS.name());
    meta.setRegion("us-east-1");

    S3FileProvider namedProvider = new S3FileProvider(variables, meta);
    FileSystem fs = namedProvider.doCreateFileSystem(fileName, null);
    assertNotNull(fs);
  }
}
