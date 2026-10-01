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

package org.apache.hop.lineage.openlineage;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.OpenLineageClientException;
import io.openlineage.client.transports.ApiKeyTokenProvider;
import io.openlineage.client.transports.HttpConfig;
import io.openlineage.client.transports.HttpSslContextConfig;
import io.openlineage.client.transports.HttpTransport;
import io.openlineage.client.transports.HttpTransportResponseException;
import java.net.URI;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.logging.ILogChannel;

/**
 * Posts OpenLineage {@link RunEvent}s to a collector via the official {@code openlineage-java}
 * {@link OpenLineageClient}/{@link HttpTransport}, adding a bounded retry-with-backoff layer (the
 * client transport itself does no retries).
 */
public final class OpenLineageHttpClient implements OpenLineageEmitter {

  private final OpenLineageClient client;
  private final URI collectorUri;
  private final int maxRetries;
  private final long retryBackoffMs;
  private final ILogChannel log;
  private final AtomicLong successCount = new AtomicLong();
  private final AtomicLong failureCount = new AtomicLong();

  public OpenLineageHttpClient(OpenLineageSinkConfig config, ILogChannel log) {
    this.collectorUri = URI.create(config.getCollectorUrl());
    this.maxRetries = config.getMaxRetries();
    this.retryBackoffMs = config.getRetryBackoffMs();
    this.log = log;

    HttpConfig httpConfig = new HttpConfig();
    // Split the full collector URL into base (scheme://authority) + endpoint (path), since
    // HttpConfig otherwise defaults the endpoint to /api/v1/lineage.
    httpConfig.setUrl(URI.create(collectorUri.getScheme() + "://" + collectorUri.getAuthority()));
    String path = collectorUri.getRawPath();
    if (!StringUtils.isBlank(path)) {
      httpConfig.setEndpoint(path);
    }
    httpConfig.setTimeoutInMillis(config.getReadTimeoutMs());
    if (!StringUtils.isBlank(config.getApiKey())) {
      ApiKeyTokenProvider tokenProvider = new ApiKeyTokenProvider();
      tokenProvider.setApiKey(config.getApiKey());
      httpConfig.setAuth(tokenProvider);
    }
    if (!StringUtils.isBlank(config.getTrustStorePath())) {
      HttpSslContextConfig ssl = new HttpSslContextConfig();
      ssl.setKeyStorePath(config.getTrustStorePath());
      ssl.setKeyStoreType(config.getKeyStoreType());
      if (!StringUtils.isBlank(config.getTrustStorePassword())) {
        ssl.setStorePassword(config.getTrustStorePassword());
      }
      httpConfig.setSslContextConfig(ssl);
    }
    // Each sink instance gets its own meter registry so the client's circuit-breaker gauge
    // registration does not collide across instances ("Gauge already registered" warnings).
    this.client =
        OpenLineageClient.builder()
            .transport(new HttpTransport(httpConfig))
            .meterRegistry(new SimpleMeterRegistry())
            .build();
  }

  /** Visible for tests: inject a pre-built client. */
  OpenLineageHttpClient(
      OpenLineageClient client,
      URI collectorUri,
      int maxRetries,
      long retryBackoffMs,
      ILogChannel log) {
    this.client = client;
    this.collectorUri = collectorUri;
    this.maxRetries = maxRetries;
    this.retryBackoffMs = retryBackoffMs;
    this.log = log;
  }

  /**
   * Emits one event, retrying <i>transient</i> failures up to {@code maxRetries} times with
   * exponential backoff. Returns {@code true} on success.
   */
  @Override
  public boolean emit(RunEvent event) {
    int attempts = Math.max(1, maxRetries + 1);
    OpenLineageClientException lastError = null;
    int attempt = 0;
    for (; attempt < attempts; attempt++) {
      try {
        client.emit(event);
        successCount.incrementAndGet();
        return true;
      } catch (OpenLineageClientException e) {
        lastError = e;
        if (!isRetryable(e)) {
          break; // the collector rejected the event itself; resending cannot change that
        }
        if (attempt < attempts - 1 && !backoff(attempt)) {
          break; // interrupted while waiting to retry
        }
      }
    }
    failureCount.incrementAndGet();
    log.logError(
        "Failed to POST OpenLineage event to "
            + collectorUri
            + " after "
            + Math.min(attempt + 1, attempts)
            + " attempt(s)",
        lastError);
    return false;
  }

  /**
   * Whether resending the event could plausibly succeed. Connection and 5xx failures are transient;
   * a 4xx means the collector understood the request and rejected it (bad payload, bad
   * credentials), so retrying only spends the backoff before failing identically. The two
   * exceptions are 408 (request timeout) and 429 (too many requests), which explicitly invite a
   * retry.
   */
  private static boolean isRetryable(OpenLineageClientException e) {
    for (Throwable t = e; t != null; t = t.getCause()) {
      if (t instanceof HttpTransportResponseException response) {
        int status = response.getStatusCode();
        if (status < 400 || status >= 500) {
          return true;
        }
        return status == 408 || status == 429;
      }
    }
    // No HTTP response at all (connect timeout, DNS, reset): worth another try.
    return true;
  }

  /** Sleeps {@code retryBackoffMs * 2^attempt}; returns false if interrupted. */
  private boolean backoff(int attempt) {
    long delay = retryBackoffMs << Math.min(attempt, 16);
    try {
      Thread.sleep(delay);
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  public long getSuccessCount() {
    return successCount.get();
  }

  public long getFailureCount() {
    return failureCount.get();
  }

  @Override
  public void close() {
    try {
      client.close();
    } catch (Exception e) {
      log.logDebug("Error closing OpenLineage client: " + e.getMessage());
    }
  }
}
