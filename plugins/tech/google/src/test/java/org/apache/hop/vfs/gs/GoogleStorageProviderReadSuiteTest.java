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

package org.apache.hop.vfs.gs;

import com.google.cloud.NoCredentials;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import org.apache.commons.vfs2.AbstractProviderTestConfig;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.ProviderReadTestSuite;
import org.apache.commons.vfs2.ProviderTestFixtures;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.hop.vfs.gs.config.GoogleCloudConfigSingleton;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * The shared read-path suite against the Google Cloud Storage provider, on fake-gcs-server. The
 * provider gets a client built with Hop's own retry and transport settings, pointed at the
 * emulator; the fixtures are uploaded with that client directly, not through the provider.
 */
@Testcontainers(disabledWithoutDocker = true)
class GoogleStorageProviderReadSuiteTest extends ProviderReadTestSuite {

  private static final String BUCKET = "hop-vfs-read-tests";
  private static final int PORT = 4443;

  @Container
  @SuppressWarnings("resource")
  static final GenericContainer<?> GCS =
      new GenericContainer<>("fsouza/fake-gcs-server:1.52.2")
          .withCommand("-scheme", "http", "-port", String.valueOf(PORT))
          .withExposedPorts(PORT)
          .waitingFor(
              Wait.forHttp("/storage/v1/b")
                  .forPort(PORT)
                  .withStartupTimeout(Duration.ofMinutes(2)));

  private static Storage storage() {
    return GoogleStorageFileSystem.buildStorageOptions(GoogleCloudConfigSingleton.getConfig())
        .setHost("http://" + GCS.getHost() + ":" + GCS.getMappedPort(PORT))
        .setProjectId("hop-test")
        .setCredentials(NoCredentials.getInstance())
        .build()
        .getService();
  }

  private static void uploadFixtures(Storage storage) throws IOException {
    storage.create(BucketInfo.of(BUCKET));
    for (String path : ProviderTestFixtures.FILES) {
      try (InputStream in =
          ProviderTestFixtures.class.getResourceAsStream(
              ProviderTestFixtures.RESOURCE_ROOT + path)) {
        storage.create(BlobInfo.newBuilder(BUCKET, path).build(), in.readAllBytes());
      }
    }
    storage.create(
        BlobInfo.newBuilder(BUCKET, ProviderTestFixtures.LARGE_FILE).build(),
        ProviderTestFixtures.largeBytes(0, ProviderTestFixtures.LARGE_SIZE));
  }

  private static class GcsConfig extends AbstractProviderTestConfig {
    @Override
    public void prepare(final DefaultFileSystemManager manager) throws Exception {
      manager.addProvider("gs", new GoogleStorageFileProvider());
    }

    @Override
    public FileObject getBaseTestFolder(final FileSystemManager manager) throws Exception {
      Storage storage = storage();
      uploadFixtures(storage);
      FileObject base = manager.resolveFile("gs://" + BUCKET);
      ((GoogleStorageFileSystem) base.getFileSystem()).useStorage(storage);
      return base;
    }
  }

  GoogleStorageProviderReadSuiteTest() {
    super(new GcsConfig());
  }

  private static final String GENERIC_ERRORS =
      "reading a folder or listing a file fails with the generic read or list error instead of"
          + " read-not-file or list-children-not-folder";
}
