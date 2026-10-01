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

import java.net.URI;
import java.net.URISyntaxException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.lineage.openlineage.OpenLineageAsyncDispatcher.OverflowPolicy;

/** Plugin configuration resolved from Hop variables in {@link OpenLineageLineageSink#init}. */
public final class OpenLineageSinkConfig {

  public static final String VAR_URL = "HOP_LINEAGE_OPENLINEAGE_URL";
  public static final String VAR_NAMESPACE = "HOP_LINEAGE_OPENLINEAGE_NAMESPACE";
  public static final String VAR_PRODUCER = "HOP_LINEAGE_OPENLINEAGE_PRODUCER";
  public static final String VAR_API_KEY = "HOP_LINEAGE_OPENLINEAGE_API_KEY";
  public static final String VAR_CONNECT_TIMEOUT_MS = "HOP_LINEAGE_OPENLINEAGE_CONNECT_TIMEOUT_MS";
  public static final String VAR_READ_TIMEOUT_MS = "HOP_LINEAGE_OPENLINEAGE_READ_TIMEOUT_MS";
  public static final String VAR_BUFFER_SIZE = "HOP_LINEAGE_OPENLINEAGE_BUFFER_SIZE";
  public static final String VAR_MAX_RETRIES = "HOP_LINEAGE_OPENLINEAGE_MAX_RETRIES";
  public static final String VAR_RETRY_BACKOFF_MS = "HOP_LINEAGE_OPENLINEAGE_RETRY_BACKOFF_MS";
  public static final String VAR_OVERFLOW_POLICY = "HOP_LINEAGE_OPENLINEAGE_OVERFLOW_POLICY";
  public static final String VAR_ENQUEUE_TIMEOUT_MS = "HOP_LINEAGE_OPENLINEAGE_ENQUEUE_TIMEOUT_MS";
  public static final String VAR_SHUTDOWN_DRAIN_MS = "HOP_LINEAGE_OPENLINEAGE_SHUTDOWN_DRAIN_MS";
  public static final String VAR_METRICS_INTERVAL_MS =
      "HOP_LINEAGE_OPENLINEAGE_METRICS_INTERVAL_MS";
  public static final String VAR_TRUSTSTORE_PATH = "HOP_LINEAGE_OPENLINEAGE_TRUSTSTORE_PATH";
  public static final String VAR_TRUSTSTORE_PASSWORD =
      "HOP_LINEAGE_OPENLINEAGE_TRUSTSTORE_PASSWORD";
  public static final String VAR_KEYSTORE_TYPE = "HOP_LINEAGE_OPENLINEAGE_KEYSTORE_TYPE";
  public static final String VAR_SQL_PARSE = "HOP_LINEAGE_OPENLINEAGE_SQL_PARSE";
  public static final String VAR_FILE_SPEC_NAMING = "HOP_LINEAGE_OPENLINEAGE_FILE_SPEC_NAMING";

  private static final String DEFAULT_NAMESPACE = "hop";
  private static final String DEFAULT_PRODUCER =
      "https://github.com/apache/hop/tree/main/plugins/tech/openlineage";
  private static final int DEFAULT_CONNECT_TIMEOUT_MS = 5_000;
  private static final int DEFAULT_READ_TIMEOUT_MS = 30_000;
  private static final int DEFAULT_BUFFER_SIZE = 10_000;
  private static final int DEFAULT_MAX_RETRIES = 3;
  private static final long DEFAULT_RETRY_BACKOFF_MS = 500;

  /**
   * Drop rather than block by default: lineage is an observation of the pipeline and must never
   * become a participant in it. The dispatcher's caller is the lineage hub's single dispatcher
   * thread, so waiting there for an unreachable collector delays every pipeline completion that is
   * waiting on a hub flush. Deployments that would rather lose throughput than lose lineage can opt
   * into {@code BLOCK}, which is itself bounded by {@link #VAR_ENQUEUE_TIMEOUT_MS}.
   */
  private static final OverflowPolicy DEFAULT_OVERFLOW_POLICY = OverflowPolicy.DROP;

  private static final long DEFAULT_SHUTDOWN_DRAIN_MS = 30_000;
  private static final long DEFAULT_ENQUEUE_TIMEOUT_MS =
      OpenLineageAsyncDispatcher.DEFAULT_ENQUEUE_TIMEOUT_MS;
  private static final long DEFAULT_METRICS_INTERVAL_MS = 0; // 0 = periodic metrics off
  private static final String DEFAULT_KEYSTORE_TYPE = "PKCS12";
  private static final boolean DEFAULT_SQL_PARSE = true;
  // Off by default: spec naming changes existing file dataset identities (see the identity
  // contract).
  private static final boolean DEFAULT_FILE_SPEC_NAMING = false;

  private final String collectorUrl;
  private final String namespace;
  private final String producer;
  private final String apiKey;
  private final int connectTimeoutMs;
  private final int readTimeoutMs;
  private final int bufferSize;
  private final int maxRetries;
  private final long retryBackoffMs;
  private final OverflowPolicy overflowPolicy;
  private final long enqueueTimeoutMs;
  private final long shutdownDrainMs;
  private final long metricsIntervalMs;
  private final String trustStorePath;
  private final String trustStorePassword;
  private final String keyStoreType;
  private final boolean sqlParseEnabled;
  private final boolean fileSpecNaming;

  private OpenLineageSinkConfig(
      String collectorUrl,
      String namespace,
      String producer,
      String apiKey,
      int connectTimeoutMs,
      int readTimeoutMs,
      int bufferSize,
      int maxRetries,
      long retryBackoffMs,
      OverflowPolicy overflowPolicy,
      long enqueueTimeoutMs,
      long shutdownDrainMs,
      long metricsIntervalMs,
      String trustStorePath,
      String trustStorePassword,
      String keyStoreType,
      boolean sqlParseEnabled,
      boolean fileSpecNaming) {
    this.collectorUrl = collectorUrl;
    this.namespace = namespace;
    this.producer = producer;
    this.apiKey = apiKey;
    this.connectTimeoutMs = connectTimeoutMs;
    this.readTimeoutMs = readTimeoutMs;
    this.bufferSize = bufferSize;
    this.maxRetries = maxRetries;
    this.retryBackoffMs = retryBackoffMs;
    this.overflowPolicy = overflowPolicy;
    this.enqueueTimeoutMs = enqueueTimeoutMs;
    this.shutdownDrainMs = shutdownDrainMs;
    this.metricsIntervalMs = metricsIntervalMs;
    this.trustStorePath = trustStorePath;
    this.trustStorePassword = trustStorePassword;
    this.keyStoreType = keyStoreType;
    this.sqlParseEnabled = sqlParseEnabled;
    this.fileSpecNaming = fileSpecNaming;
  }

  public static OpenLineageSinkConfig fromVariables(IVariables variables) {
    String url = variables.getVariable(VAR_URL, "");
    if (StringUtils.isBlank(url)) {
      throw new IllegalArgumentException(
          VAR_URL
              + " must be set to the OpenLineage collector URL "
              + "(e.g. http://marquez-api:5000/api/v1/lineage)");
    }
    url = url.trim();
    validateHttpUrl(url);
    String namespace = variables.getVariable(VAR_NAMESPACE, DEFAULT_NAMESPACE);
    String producer = variables.getVariable(VAR_PRODUCER, DEFAULT_PRODUCER);
    String apiKey = variables.getVariable(VAR_API_KEY, "");
    int connectTimeout =
        parsePositiveInt(
            variables.getVariable(VAR_CONNECT_TIMEOUT_MS, null), DEFAULT_CONNECT_TIMEOUT_MS);
    int readTimeout =
        parsePositiveInt(variables.getVariable(VAR_READ_TIMEOUT_MS, null), DEFAULT_READ_TIMEOUT_MS);
    int bufferSize =
        parsePositiveInt(variables.getVariable(VAR_BUFFER_SIZE, null), DEFAULT_BUFFER_SIZE);
    int maxRetries =
        parseNonNegativeInt(variables.getVariable(VAR_MAX_RETRIES, null), DEFAULT_MAX_RETRIES);
    long retryBackoff =
        parsePositiveLong(
            variables.getVariable(VAR_RETRY_BACKOFF_MS, null), DEFAULT_RETRY_BACKOFF_MS);
    OverflowPolicy overflowPolicy =
        parseOverflowPolicy(variables.getVariable(VAR_OVERFLOW_POLICY, null));
    long enqueueTimeout =
        parsePositiveLong(
            variables.getVariable(VAR_ENQUEUE_TIMEOUT_MS, null), DEFAULT_ENQUEUE_TIMEOUT_MS);
    long shutdownDrain =
        parsePositiveLong(
            variables.getVariable(VAR_SHUTDOWN_DRAIN_MS, null), DEFAULT_SHUTDOWN_DRAIN_MS);
    long metricsInterval =
        parseNonNegativeLong(
            variables.getVariable(VAR_METRICS_INTERVAL_MS, null), DEFAULT_METRICS_INTERVAL_MS);
    String trustStorePath = variables.getVariable(VAR_TRUSTSTORE_PATH, "");
    String trustStorePassword = variables.getVariable(VAR_TRUSTSTORE_PASSWORD, "");
    String keyStoreType = variables.getVariable(VAR_KEYSTORE_TYPE, DEFAULT_KEYSTORE_TYPE);
    boolean sqlParse = parseBoolean(variables.getVariable(VAR_SQL_PARSE, null), DEFAULT_SQL_PARSE);
    boolean fileSpecNaming =
        parseBoolean(variables.getVariable(VAR_FILE_SPEC_NAMING, null), DEFAULT_FILE_SPEC_NAMING);
    return new OpenLineageSinkConfig(
        url,
        StringUtils.defaultIfBlank(namespace, DEFAULT_NAMESPACE).trim(),
        StringUtils.defaultIfBlank(producer, DEFAULT_PRODUCER).trim(),
        StringUtils.isBlank(apiKey) ? null : apiKey.trim(),
        connectTimeout,
        readTimeout,
        bufferSize,
        maxRetries,
        retryBackoff,
        overflowPolicy,
        enqueueTimeout,
        shutdownDrain,
        metricsInterval,
        StringUtils.isBlank(trustStorePath) ? null : trustStorePath.trim(),
        StringUtils.isBlank(trustStorePassword) ? null : trustStorePassword.trim(),
        StringUtils.defaultIfBlank(keyStoreType, DEFAULT_KEYSTORE_TYPE).trim(),
        sqlParse,
        fileSpecNaming);
  }

  private static boolean parseBoolean(String raw, boolean defaultValue) {
    if (StringUtils.isBlank(raw)) {
      return defaultValue;
    }
    String value = raw.trim();
    if ("Y".equalsIgnoreCase(value)
        || "true".equalsIgnoreCase(value)
        || "yes".equalsIgnoreCase(value)
        || "1".equals(value)) {
      return true;
    }
    if ("N".equalsIgnoreCase(value)
        || "false".equalsIgnoreCase(value)
        || "no".equalsIgnoreCase(value)
        || "0".equals(value)) {
      return false;
    }
    return defaultValue;
  }

  private static long parseNonNegativeLong(String raw, long defaultValue) {
    if (StringUtils.isBlank(raw)) {
      return defaultValue;
    }
    try {
      long value = Long.parseLong(raw.trim());
      return value >= 0 ? value : defaultValue;
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  private static void validateHttpUrl(String url) {
    try {
      URI uri = new URI(url);
      String scheme = uri.getScheme();
      if (scheme == null
          || (!scheme.equalsIgnoreCase("http") && !scheme.equalsIgnoreCase("https"))
          || uri.getHost() == null) {
        throw new IllegalArgumentException(
            VAR_URL + " must be an absolute http(s) URL, was: " + url);
      }
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(VAR_URL + " is not a valid URL: " + url, e);
    }
  }

  private static OverflowPolicy parseOverflowPolicy(String raw) {
    if (StringUtils.isBlank(raw)) {
      return DEFAULT_OVERFLOW_POLICY;
    }
    try {
      return OverflowPolicy.valueOf(raw.trim().toUpperCase(java.util.Locale.ROOT));
    } catch (IllegalArgumentException e) {
      return DEFAULT_OVERFLOW_POLICY;
    }
  }

  private static int parseNonNegativeInt(String raw, int defaultValue) {
    if (StringUtils.isBlank(raw)) {
      return defaultValue;
    }
    try {
      int value = Integer.parseInt(raw.trim());
      return value >= 0 ? value : defaultValue;
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  private static long parsePositiveLong(String raw, long defaultValue) {
    if (StringUtils.isBlank(raw)) {
      return defaultValue;
    }
    try {
      long value = Long.parseLong(raw.trim());
      return value > 0 ? value : defaultValue;
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  private static int parsePositiveInt(String raw, int defaultValue) {
    if (StringUtils.isBlank(raw)) {
      return defaultValue;
    }
    try {
      int value = Integer.parseInt(raw.trim());
      return value > 0 ? value : defaultValue;
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  public String getCollectorUrl() {
    return collectorUrl;
  }

  public String getNamespace() {
    return namespace;
  }

  public String getProducer() {
    return producer;
  }

  public String getApiKey() {
    return apiKey;
  }

  public int getConnectTimeoutMs() {
    return connectTimeoutMs;
  }

  public int getReadTimeoutMs() {
    return readTimeoutMs;
  }

  public int getBufferSize() {
    return bufferSize;
  }

  public int getMaxRetries() {
    return maxRetries;
  }

  public long getRetryBackoffMs() {
    return retryBackoffMs;
  }

  public OverflowPolicy getOverflowPolicy() {
    return overflowPolicy;
  }

  /** Ceiling on how long {@code BLOCK} waits for outbound-buffer capacity before dropping. */
  public long getEnqueueTimeoutMs() {
    return enqueueTimeoutMs;
  }

  public long getShutdownDrainMs() {
    return shutdownDrainMs;
  }

  public long getMetricsIntervalMs() {
    return metricsIntervalMs;
  }

  public String getTrustStorePath() {
    return trustStorePath;
  }

  public String getTrustStorePassword() {
    return trustStorePassword;
  }

  public String getKeyStoreType() {
    return keyStoreType;
  }

  /** Whether Table Input / Execute SQL statements are parsed to recover their relational tables. */
  public boolean isSqlParseEnabled() {
    return sqlParseEnabled;
  }

  /** Whether file datasets use OpenLineage spec naming (VFS scheme namespace + path) vs. legacy. */
  public boolean isFileSpecNaming() {
    return fileSpecNaming;
  }
}
