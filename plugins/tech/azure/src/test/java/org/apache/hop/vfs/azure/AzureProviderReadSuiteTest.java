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

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import org.apache.commons.vfs2.AbstractProviderTestConfig;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.ProviderLargeReadTests;
import org.apache.commons.vfs2.ProviderRandomReadTests;
import org.apache.commons.vfs2.ProviderRangeReadTests;
import org.apache.commons.vfs2.ProviderReadTestSuite;
import org.apache.commons.vfs2.ProviderTestFixtures;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.vfs.azure.metadatatype.AzureMetadataType;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * The shared read-path suite against the Azure provider, through a named Azure connection pointing
 * at Azurite. The fixtures are uploaded with the Blob API, not through the provider.
 */
@Testcontainers(disabledWithoutDocker = true)
class AzureProviderReadSuiteTest extends ProviderReadTestSuite {

  /** Azurite's well-known development account. */
  private static final String ACCOUNT = "devstoreaccount1";

  private static final String KEY =
      "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

  private static final String CONTAINER = "hop-vfs-read-tests";
  private static final String SCHEME = "azreadtest";
  private static final int PORT = 10000;

  @Container
  @SuppressWarnings("resource")
  static final GenericContainer<?> AZURITE =
      new GenericContainer<>("mcr.microsoft.com/azure-storage/azurite:3.34.0")
          .withCommand(
              "azurite-blob",
              "--blobHost",
              "0.0.0.0",
              "--blobPort",
              String.valueOf(PORT),
              "--loose")
          .withExposedPorts(PORT)
          .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(2)));

  private static String endpoint() {
    return "http://" + AZURITE.getHost() + ":" + AZURITE.getMappedPort(PORT) + "/" + ACCOUNT;
  }

  private static void uploadFixtures() throws IOException {
    BlobContainerClient container =
        new BlobServiceClientBuilder()
            .connectionString(
                "DefaultEndpointsProtocol=http;AccountName="
                    + ACCOUNT
                    + ";AccountKey="
                    + KEY
                    + ";BlobEndpoint="
                    + endpoint()
                    + ";")
            .buildClient()
            .createBlobContainer(CONTAINER);
    for (String path : ProviderTestFixtures.FILES) {
      try (InputStream in =
          ProviderTestFixtures.class.getResourceAsStream(
              ProviderTestFixtures.RESOURCE_ROOT + path)) {
        container.getBlobClient(path).upload(BinaryData.fromBytes(in.readAllBytes()));
      }
    }
    container
        .getBlobClient(ProviderTestFixtures.LARGE_FILE)
        .upload(
            BinaryData.fromBytes(
                ProviderTestFixtures.largeBytes(0, ProviderTestFixtures.LARGE_SIZE)));
  }

  private static class AzureConfig extends AbstractProviderTestConfig {
    @Override
    public void prepare(final DefaultFileSystemManager manager) throws Exception {
      // The provider decrypts the account key, which needs the password encoder
      HopClientEnvironment.init();
      AzureMetadataType connection = new AzureMetadataType();
      connection.setName(SCHEME);
      connection.setStorageAccountName(ACCOUNT);
      connection.setStorageAccountKey(KEY);
      connection.setStorageAccountEndpoint(endpoint());
      connection.setAuthenticationType("Key");
      manager.addProvider(SCHEME, new AzureFileProvider(new Variables(), connection));
    }

    @Override
    public FileObject getBaseTestFolder(final FileSystemManager manager) throws Exception {
      uploadFixtures();
      return manager.resolveFile(SCHEME + ":///" + CONTAINER);
    }
  }

  AzureProviderReadSuiteTest() {
    super(new AzureConfig());
  }

  /**
   * Azurite emulates the Blob API only. The provider lists Data Lake paths (listPaths) whenever it
   * attaches a folder, and Azurite answers that with 400, so folder and listing tests can't run
   * here; they need a real account (hop-infra-tests). Reading files by path works, so the large
   * sequential reads and the random-access reads run.
   */
  @Override
  protected void addBaseTests() throws Exception {
    addTests(ProviderLargeReadTests.class);
    addTests(ProviderRandomReadTests.class);
    addTests(ProviderRangeReadTests.class);
  }

  @Override
  protected boolean needsReadFolder() {
    return false;
  }
}
