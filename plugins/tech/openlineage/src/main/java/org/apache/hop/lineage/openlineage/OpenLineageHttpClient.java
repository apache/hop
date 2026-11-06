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
   * exponential backoff.
   */
  @Override
  public Result emit(RunEvent event) {
    int attempts = Math.max(1, maxRetries + 1);
    OpenLineageClientException lastError = null;
    int status = -1;
    int attempt = 0;
    for (; attempt < attempts; attempt++) {
      try {
        client.emit(event);
        successCount.incrementAndGet();
        return Result.SENT;
      } catch (OpenLineageClientException e) {
        lastError = e;
        status = statusOf(e);
        if (!OpenLineageEmitter.isTransient(status)) {
          break; // a 4xx: the same request would get the same answer
        }
        if (attempt < attempts - 1 && !backoff(attempt)) {
          break; // interrupted while waiting to retry
        }
      }
    }
    failureCount.incrementAndGet();
    Result result = OpenLineageEmitter.failureOf(status);
    log.logError(
        "Failed to POST OpenLineage event to "
            + collectorUri
            + " after "
            + Math.min(attempt + 1, attempts)
            + " attempt(s)"
            + (result == Result.REJECTED
                ? "; the collector refused it, it will not be resent"
                : ""),
        lastError);
    return result;
  }

  /** The HTTP status of a failed emit, or {@code -1} when there was no response at all. */
  private static int statusOf(OpenLineageClientException e) {
    for (Throwable t = e; t != null; t = t.getCause()) {
      if (t instanceof HttpTransportResponseException response) {
        return response.getStatusCode();
      }
    }
    return -1;
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
