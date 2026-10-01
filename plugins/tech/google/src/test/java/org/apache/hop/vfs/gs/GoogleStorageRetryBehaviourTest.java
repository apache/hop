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
 *
 */

package org.apache.hop.vfs.gs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.NoCredentials;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hop.vfs.gs.config.GoogleCloudConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.threeten.bp.Duration;

/**
 * Counts the HTTP requests the storage client actually makes against a local endpoint that rejects
 * the first few calls, which is the only way to tell a configured retry from an ignored one.
 *
 * <p>Google Cloud Storage classifies a call as idempotent only when it carries a precondition.
 * Reading and listing always qualify; creating, deleting and starting an upload do not, and the
 * client silently refuses to retry those no matter how many attempts are configured. That is what
 * {@link GoogleCloudConfig#getRetryNonIdempotentOperations()} exists to change.
 */
class GoogleStorageRetryBehaviourTest {

  /** Requests rejected before the endpoint starts answering normally. */
  private static final int REJECTED_REQUESTS = 3;

  private static final String REJECTION =
      "{\"error\":{\"code\":429,\"message\":\"The rate of change requests to the object exceeds "
          + "the rate limit.\",\"errors\":[{\"domain\":\"usageLimits\",\"reason\":"
          + "\"rateLimitExceeded\",\"message\":\"rate limit exceeded\"}]}}";

  private static final String OBJECT =
      "{\"kind\":\"storage#object\",\"bucket\":\"bucket\",\"name\":\"object\","
          + "\"generation\":\"1\",\"metageneration\":\"1\",\"size\":\"0\"}";

  private HttpServer server;
  private final AtomicInteger requests = new AtomicInteger();

  @BeforeEach
  void startEndpoint() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/",
        exchange -> {
          boolean reject = requests.incrementAndGet() <= REJECTED_REQUESTS;
          byte[] body = (reject ? REJECTION : OBJECT).getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().add("Content-Type", "application/json; charset=UTF-8");
          exchange.sendResponseHeaders(reject ? 429 : 200, body.length);
          try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
          }
        });
    server.start();
  }

  @AfterEach
  void stopEndpoint() {
    server.stop(0);
  }

  @Test
  void readsAreRetriedWithTheDefaultStrategy() {
    Storage storage = storageWith(new GoogleCloudConfig());

    assertNotNull(storage.get(BlobId.of("bucket", "object")));
    assertEquals(
        REJECTED_REQUESTS + 1, requests.get(), "a read should be retried until it succeeds");
  }

  /** The reported bug: attempts are configured, and a write still gives up immediately. */
  @Test
  void writesAreNotRetriedWithTheDefaultStrategy() {
    Storage storage = storageWith(new GoogleCloudConfig());

    assertThrows(
        StorageException.class,
        () ->
            storage.create(BlobInfo.newBuilder("bucket", "object").build(), new byte[] {1, 2, 3}));
    assertEquals(1, requests.get(), "the default strategy never retries a create");

    requests.set(0);
    assertThrows(StorageException.class, () -> storage.delete(BlobId.of("bucket", "object")));
    assertEquals(1, requests.get(), "the default strategy never retries a delete");
  }

  @Test
  void writesAreRetriedOnceNonIdempotentRetriesAreEnabled() {
    GoogleCloudConfig config = new GoogleCloudConfig();
    config.setRetryNonIdempotentOperations(true);
    Storage storage = storageWith(config);

    assertNotNull(
        storage.create(BlobInfo.newBuilder("bucket", "object").build(), new byte[] {1, 2, 3}));
    assertEquals(REJECTED_REQUESTS + 1, requests.get(), "a create should now be retried");

    requests.set(0);
    storage.delete(BlobId.of("bucket", "object"));
    assertEquals(REJECTED_REQUESTS + 1, requests.get(), "a delete should now be retried");
  }

  @Test
  void retriesStopAtTheConfiguredNumberOfAttempts() {
    GoogleCloudConfig config = new GoogleCloudConfig();
    config.setRetryNonIdempotentOperations(true);
    config.setMaxAttempts("2");
    Storage storage = storageWith(config);

    assertThrows(
        StorageException.class,
        () ->
            storage.create(BlobInfo.newBuilder("bucket", "object").build(), new byte[] {1, 2, 3}));
    assertEquals(2, requests.get(), "two attempts were configured, so two requests");
  }

  /**
   * Builds the client the way {@link GoogleStorageFileSystem#setupStorage()} does, pointed at the
   * local endpoint. The retry delays are collapsed to milliseconds so the test stays fast; the
   * configured attempt count - the thing under test - is left alone. The delay values themselves
   * are covered by {@link GoogleStorageRetrySettingsTest}.
   */
  private Storage storageWith(GoogleCloudConfig config) {
    RetrySettings prompt =
        GoogleStorageFileSystem.buildRetrySettings(config).toBuilder()
            .setInitialRetryDelay(Duration.ofMillis(1))
            .setMaxRetryDelay(Duration.ofMillis(2))
            .build();

    return GoogleStorageFileSystem.buildStorageOptions(config)
        .setRetrySettings(prompt)
        .setHost("http://127.0.0.1:" + server.getAddress().getPort())
        .setProjectId("hop-test")
        .setCredentials(NoCredentials.getInstance())
        .build()
        .getService();
  }
}
