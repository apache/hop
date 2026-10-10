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

package org.apache.hop.vfs.s3.vfs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.vfs.s3.metadata.S3AuthType;
import org.apache.hop.vfs.s3.metadata.S3Meta;
import org.apache.hop.vfs.s3.s3.vfs.S3FileProvider;
import org.apache.hop.vfs.s3.s3common.S3CommonFileProvider;
import org.apache.hop.vfs.s3.s3common.S3CommonFileSystemConfigBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Resolving files reuses the provider's file system, as HopVfs resolves them: without options.
 * Building a file system means building an S3 client, so a new one per file is expensive, and the
 * provider would keep every one of them. Nothing here connects to S3.
 */
class S3FileProviderFileSystemReuseTest {

  private static final String ENDPOINT = "http://127.0.0.1:9";

  private DefaultFileSystemManager manager;

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
  }

  @BeforeEach
  void setUp() throws Exception {
    S3Meta connection = new S3Meta();
    connection.setName("s3reuse");
    connection.setAuthenticationType(S3AuthType.ACCESS_KEYS.name());
    connection.setAccessKey("access");
    connection.setSecretKey("secret");
    connection.setRegion("eu-west-1");
    connection.setEndpoint(ENDPOINT);
    connection.setPathStyleAccess(true);

    manager = new DefaultFileSystemManager();
    manager.addProvider("s3", new S3FileProvider());
    manager.addProvider("s3reuse", new S3FileProvider(new Variables(), connection));
    manager.init();
  }

  @AfterEach
  void tearDown() {
    manager.close();
  }

  private FileSystem fileSystemOf(String uri) throws Exception {
    return manager.resolveFile(uri).getFileSystem();
  }

  @Test
  void defaultSchemeReusesItsFileSystem() throws Exception {
    FileSystem first = fileSystemOf("s3://bucket/a.txt");

    assertSame(first, fileSystemOf("s3://bucket/folder/b.txt"));
    assertSame(first, fileSystemOf("s3://other-bucket/c.txt"));
  }

  @Test
  void namedConnectionReusesItsFileSystem() throws Exception {
    FileSystem first = fileSystemOf("s3reuse://bucket/a.txt");

    assertSame(first, fileSystemOf("s3reuse://bucket/folder/b.txt"));
    assertSame(first, fileSystemOf("s3reuse://other-bucket/c.txt"));
  }

  @Test
  void namedConnectionKeepsItsOwnSettings() throws Exception {
    FileSystem named = fileSystemOf("s3reuse://bucket/a.txt");
    FileSystem standard = fileSystemOf("s3://bucket/a.txt");

    assertNotSame(standard, named);
    S3CommonFileSystemConfigBuilder config =
        new S3CommonFileSystemConfigBuilder(named.getFileSystemOptions());
    assertEquals(ENDPOINT, config.getEndpoint());
    assertEquals("eu-west-1", config.getRegion());
    assertEquals("access", config.getAccessKey());

    // The named connection must never write its settings into the defaults s3:// uses
    assertNull(
        new S3CommonFileSystemConfigBuilder(S3CommonFileProvider.getDefaultFileSystemOptions())
            .getEndpoint());
  }
}
