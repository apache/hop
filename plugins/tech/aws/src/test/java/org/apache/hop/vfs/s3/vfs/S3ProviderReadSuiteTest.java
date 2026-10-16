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

import java.net.URI;
import java.time.Duration;
import org.apache.commons.vfs2.AbstractProviderTestConfig;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.ProviderReadTestSuite;
import org.apache.commons.vfs2.ProviderTestFixtures;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.vfs.s3.metadata.S3AuthType;
import org.apache.hop.vfs.s3.metadata.S3Meta;
import org.apache.hop.vfs.s3.s3.vfs.S3FileProvider;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * The shared read-path suite against the S3 provider, through a named S3 connection pointing at an
 * S3 emulator. Adobe S3Mock follows the AWS semantics the provider relies on (a HEAD on a "folder"
 * prefix is a 404); SeaweedFS keeps real directories and answers it, so it is not used here. The
 * fixtures are uploaded with the AWS SDK, so the setup doesn't depend on the provider under test.
 */
@Testcontainers(disabledWithoutDocker = true)
class S3ProviderReadSuiteTest extends ProviderReadTestSuite {

  private static final String ACCESS_KEY = "ApacheHopAccessKey";
  private static final String SECRET_KEY = "ApacheHopSecretKey";
  private static final String BUCKET = "hop-vfs-read-tests";
  private static final String SCHEME = "s3readtest";
  private static final int PORT = 9090;

  @Container
  @SuppressWarnings("resource")
  static final GenericContainer<?> STORE =
      new GenericContainer<>("adobe/s3mock:4.7.0")
          .withExposedPorts(PORT)
          .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(2)));

  private static String endpoint() {
    return "http://" + STORE.getHost() + ":" + STORE.getMappedPort(PORT);
  }

  private static void uploadFixtures() {
    try (S3Client s3 =
        S3Client.builder()
            .endpointOverride(URI.create(endpoint()))
            .forcePathStyle(true)
            .region(Region.US_EAST_1)
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
            .build()) {
      s3.createBucket(b -> b.bucket(BUCKET));
      for (String path : ProviderTestFixtures.FILES) {
        try (var in =
            ProviderTestFixtures.class.getResourceAsStream(
                ProviderTestFixtures.RESOURCE_ROOT + path)) {
          s3.putObject(b -> b.bucket(BUCKET).key(path), RequestBody.fromBytes(in.readAllBytes()));
        } catch (java.io.IOException e) {
          throw new IllegalStateException(e);
        }
      }
      s3.putObject(
          b -> b.bucket(BUCKET).key(ProviderTestFixtures.LARGE_FILE),
          RequestBody.fromBytes(
              ProviderTestFixtures.largeBytes(0, ProviderTestFixtures.LARGE_SIZE)));
    }
  }

  private static class S3Config extends AbstractProviderTestConfig {
    @Override
    public void prepare(final DefaultFileSystemManager manager) throws Exception {
      S3Meta meta = new S3Meta();
      meta.setName(SCHEME);
      meta.setAuthenticationType(S3AuthType.ACCESS_KEYS.name());
      meta.setAccessKey(ACCESS_KEY);
      meta.setSecretKey(SECRET_KEY);
      meta.setRegion("us-east-1");
      meta.setEndpoint(endpoint());
      meta.setPathStyleAccess(true);
      manager.addProvider(SCHEME, new S3FileProvider(new Variables(), meta));
    }

    @Override
    public FileObject getBaseTestFolder(final FileSystemManager manager) throws Exception {
      uploadFixtures();
      return manager.resolveFile(SCHEME + "://" + BUCKET);
    }
  }

  S3ProviderReadSuiteTest() {
    super(new S3Config());
  }

  private static final String FOLDER_REATTACH =
      "a folder without a marker object turns IMAGINARY once its parent is listed again: doAttach"
          + " keeps the trailing '/' it added to the key, so the next attach looks in the wrong place";
}
