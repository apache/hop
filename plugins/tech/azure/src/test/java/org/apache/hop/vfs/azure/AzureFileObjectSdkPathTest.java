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

package org.apache.hop.vfs.azure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.apache.commons.vfs2.provider.VfsComponentContext;
import org.apache.hop.vfs.azure.config.AzureConfig;
import org.apache.hop.vfs.azure.config.AzureConfigSingleton;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/**
 * Regression guard for the Azure VFS "%2F-in-request-URI" bug.
 *
 * <p>{@link AzureFileName#getPathAfterContainer()} returns the path with a leading slash, e.g.
 * {@code /output.txt} for {@code azure://test/output.txt}. {@link AzureFileObject#doAttach()} must
 * strip that leading slash before handing the path to the Azure SDK; otherwise the SDK builds a
 * request URL like {@code /<container>/%2F<file>} and modern Netty
 * (HttpUtil.validateRequestLineTokens) refuses it with "The URI contain illegal characters: …%2F…".
 *
 * <p>This test pins the contract: the path that would be handed to {@code
 * fileSystemClient.getFileClient(...)} / {@code getDirectoryClient(...)} must never start with
 * {@code /} for a file directly under a container.
 */
class AzureFileObjectSdkPathTest {

  @BeforeAll
  static void init() {
    AzureConfig azureConfig = new AzureConfig();
    // Obvious dummy values: account is a placeholder, key is base64 of "dummy-key".
    azureConfig.setAccount("dummy-account");
    azureConfig.setKey("ZHVtbXkta2V5");
    try {
      // Intentionally not closed - mirrors AzureFileNameParserTest, which leaks the same static
      // mock for the JVM lifetime. We just need a config available when the parser asks.
      MockedStatic<AzureConfigSingleton> mocked = Mockito.mockStatic(AzureConfigSingleton.class);
      mocked.when(AzureConfigSingleton::getConfig).thenReturn(azureConfig);
    } catch (org.mockito.exceptions.base.MockitoException alreadyRegistered) {
      // Another test in this JVM already mocked it; reuse that registration.
    }
  }

  private static String sdkPathFor(String uri) throws Exception {
    AzureFileNameParser parser = new AzureFileNameParser();
    AzureFileName name = (AzureFileName) parser.parseUri((VfsComponentContext) null, null, uri);
    // This is exactly the normalisation now performed at the top of AzureFileObject#doAttach.
    String pathAfter = name.getPathAfterContainer();
    return pathAfter.startsWith("/") ? pathAfter.substring(1) : pathAfter;
  }

  @Test
  void fileDirectlyUnderContainerHasNoLeadingSlash() throws Exception {
    // Same shape as the reported bug: a file directly under the container.
    String sdkPath = sdkPathFor("azure://test/output.txt");

    assertFalse(
        sdkPath.startsWith("/"),
        "leading slash would make the Azure SDK URL-encode it as %2F and Netty would reject it");
    assertEquals("output.txt", sdkPath);
  }

  @Test
  void nestedFileHasNoLeadingSlash() throws Exception {
    String sdkPath = sdkPathFor("azure://mycontainer/folder1/sub/file.parquet");

    assertFalse(sdkPath.startsWith("/"));
    assertEquals("folder1/sub/file.parquet", sdkPath);
  }

  @Test
  void containerRootStripsCleanly() throws Exception {
    // Path at container root - the strip must leave an empty string, not a single "/".
    String sdkPath = sdkPathFor("azure://mycontainer/");

    assertFalse(sdkPath.startsWith("/"));
    assertEquals("", sdkPath);
  }

  /** Belt-and-braces: pin what AzureFileName itself returns, so the normalisation has a target. */
  @Test
  void azureFileNameStillReturnsLeadingSlashFromGetPathAfterContainer() throws Exception {
    AzureFileNameParser parser = new AzureFileNameParser();
    AzureFileName name =
        (AzureFileName)
            parser.parseUri((VfsComponentContext) null, null, "azure://test/output.txt");

    assertEquals("test", name.getContainer());
    assertEquals("/output.txt", name.getPathAfterContainer());
  }
}
