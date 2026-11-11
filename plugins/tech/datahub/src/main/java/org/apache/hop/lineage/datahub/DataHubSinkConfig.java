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

package org.apache.hop.lineage.datahub;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Locale;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variable;
import org.apache.hop.core.variables.VariableScope;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.lineage.openlineage.OpenLineageSinkConfig;

/**
 * Configuration of the DataHub sink, read from {@code HOP_LINEAGE_DATAHUB_*} variables.
 *
 * <p>The delivery settings the sink shares with the OpenLineage sink (queue, spool, retries,
 * timeouts, TLS, SQL parsing) keep their OpenLineage names under the DataHub prefix: {@code
 * HOP_LINEAGE_DATAHUB_SPOOL_DIR} configures what {@code HOP_LINEAGE_OPENLINEAGE_SPOOL_DIR} does for
 * that sink.
 */
public final class DataHubSinkConfig {

  @Variable(
      scope = VariableScope.ENGINE,
      value = "",
      description =
          "DataHub GMS URL the DataHub lineage sink writes to, e.g. http://localhost:8080 or"
              + " https://your-company.acryl.io/gms. Required when the sink is enabled.")
  public static final String HOP_LINEAGE_DATAHUB_URL = "HOP_LINEAGE_DATAHUB_URL";

  @Variable(
      scope = VariableScope.ENGINE,
      value = "",
      description =
          "DataHub personal access token for the DataHub lineage sink, sent as a bearer token."
              + " Needed when DataHub has authentication enabled. May be encrypted with hop-encrypt.")
  public static final String HOP_LINEAGE_DATAHUB_TOKEN = "HOP_LINEAGE_DATAHUB_TOKEN";

  @Variable(
      scope = VariableScope.ENGINE,
      value = "PROD",
      description =
          "DataHub environment (fabric) of the datasets, pipelines and workflows Hop reports,"
              + " e.g. PROD, DEV or TEST. Use the same value as the other sources that describe"
              + " those datasets, or their lineage does not join up.")
  public static final String HOP_LINEAGE_DATAHUB_ENV = "HOP_LINEAGE_DATAHUB_ENV";

  @Variable(
      scope = VariableScope.ENGINE,
      value = "",
      description =
          "Optional DataHub platform instance for Hop: keeps the pipelines and workflows of"
              + " several Hop installations apart in one DataHub.")
  public static final String HOP_LINEAGE_DATAHUB_PLATFORM_INSTANCE =
      "HOP_LINEAGE_DATAHUB_PLATFORM_INSTANCE";

  @Variable(
      scope = VariableScope.ENGINE,
      value = "missing",
      description =
          "When the DataHub lineage sink writes a dataset's schema: 'missing' (only when DataHub"
              + " has none, or the one it has came from Hop), 'always' or 'never'.")
  public static final String HOP_LINEAGE_DATAHUB_DATASET_SCHEMA =
      "HOP_LINEAGE_DATAHUB_DATASET_SCHEMA";

  @Variable(
      scope = VariableScope.ENGINE,
      value = "N",
      description =
          "Set to Y to let the DataHub lineage sink deprecate datasets a run deleted or moved, and"
              + " clear that deprecation when a later run writes them again.")
  public static final String HOP_LINEAGE_DATAHUB_LIFECYCLE = "HOP_LINEAGE_DATAHUB_LIFECYCLE";

  @Variable(
      scope = VariableScope.ENGINE,
      value = "urn:li:corpuser:datahub",
      description =
          "DataHub user the DataHub lineage sink names as the actor of the runs and deprecations"
              + " it writes.")
  public static final String HOP_LINEAGE_DATAHUB_ACTOR = "HOP_LINEAGE_DATAHUB_ACTOR";

  @Variable(
      scope = VariableScope.ENGINE,
      value = "",
      description =
          "Folder where the DataHub lineage sink keeps events until DataHub accepted them, so an"
              + " outage or a restart delays lineage instead of losing it. One folder per Hop"
              + " process; empty keeps events in memory only.")
  public static final String HOP_LINEAGE_DATAHUB_SPOOL_DIR = "HOP_LINEAGE_DATAHUB_SPOOL_DIR";

  /**
   * How a dataset's {@code schemaMetadata} is written; see {@link
   * #HOP_LINEAGE_DATAHUB_DATASET_SCHEMA}.
   */
  public enum SchemaPolicy {
    /** Only when DataHub has no schema for the dataset, or the one it has was written by Hop. */
    MISSING,
    ALWAYS,
    NEVER
  }

  private static final String DATAHUB_PREFIX = "HOP_LINEAGE_DATAHUB_";
  private static final String OPENLINEAGE_PREFIX = "HOP_LINEAGE_OPENLINEAGE_";

  /**
   * The settings shared with the OpenLineage sink, passed through under the same suffix. The
   * mapping options (transform jobs, parent runs, file naming) are not among them: DataHub needs
   * them one way.
   */
  private static final List<String> SHARED_SUFFIXES =
      List.of(
          "CONNECT_TIMEOUT_MS",
          "READ_TIMEOUT_MS",
          "BUFFER_SIZE",
          "MAX_RETRIES",
          "RETRY_BACKOFF_MS",
          "OVERFLOW_POLICY",
          "ENQUEUE_TIMEOUT_MS",
          "SHUTDOWN_DRAIN_MS",
          "METRICS_INTERVAL_MS",
          "TRUSTSTORE_PATH",
          "TRUSTSTORE_PASSWORD",
          "KEYSTORE_TYPE",
          "SQL_PARSE",
          "SPOOL_DIR",
          "SPOOL_MAX_BYTES");

  /**
   * The path of DataHub's OpenLineage endpoint. An earlier version of this sink was configured with
   * it; the URL still works, the path is dropped.
   */
  static final String OPENLINEAGE_ENDPOINT_PATH = "/openapi/openlineage/api/v1/lineage";

  @Getter private final String gmsUrl;
  @Getter private final String token;

  /** The dataset fabric, upper case ({@code PROD}). */
  @Getter private final String env;

  @Getter private final String platformInstance;
  @Getter private final SchemaPolicy schemaPolicy;
  @Getter private final boolean lifecycleEnabled;
  @Getter private final String actor;
  private final IVariables source;

  private DataHubSinkConfig(
      String gmsUrl,
      String token,
      String env,
      String platformInstance,
      SchemaPolicy schemaPolicy,
      boolean lifecycleEnabled,
      String actor,
      IVariables source) {
    this.gmsUrl = gmsUrl;
    this.token = token;
    this.env = env;
    this.platformInstance = platformInstance;
    this.schemaPolicy = schemaPolicy;
    this.lifecycleEnabled = lifecycleEnabled;
    this.actor = actor;
    this.source = source;
  }

  /**
   * Reads the configuration.
   *
   * @throws IllegalArgumentException when the URL is missing or not an absolute http(s) URL, or a
   *     setting has a value it does not accept
   */
  public static DataHubSinkConfig fromVariables(IVariables variables) {
    String url = value(variables, HOP_LINEAGE_DATAHUB_URL);
    if (url == null) {
      throw new IllegalArgumentException(
          HOP_LINEAGE_DATAHUB_URL + " is required when the DataHub lineage sink is enabled");
    }
    String env = StringUtils.defaultIfBlank(value(variables, HOP_LINEAGE_DATAHUB_ENV), "PROD");
    return new DataHubSinkConfig(
        normalizeUrl(url),
        OpenLineageSinkConfig.secret(value(variables, HOP_LINEAGE_DATAHUB_TOKEN)),
        env.toUpperCase(Locale.ROOT),
        value(variables, HOP_LINEAGE_DATAHUB_PLATFORM_INSTANCE),
        schemaPolicy(value(variables, HOP_LINEAGE_DATAHUB_DATASET_SCHEMA)),
        parseBoolean(value(variables, HOP_LINEAGE_DATAHUB_LIFECYCLE), false),
        StringUtils.defaultIfBlank(
            value(variables, HOP_LINEAGE_DATAHUB_ACTOR), "urn:li:corpuser:datahub"),
        variables);
  }

  private static String value(IVariables variables, String name) {
    String value = variables.getVariable(name, null);
    return StringUtils.isBlank(value) ? null : value.trim();
  }

  static SchemaPolicy schemaPolicy(String value) {
    if (value == null) {
      return SchemaPolicy.MISSING;
    }
    try {
      return SchemaPolicy.valueOf(value.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          HOP_LINEAGE_DATAHUB_DATASET_SCHEMA
              + " must be 'missing', 'always' or 'never', was: "
              + value,
          e);
    }
  }

  static boolean parseBoolean(String value, boolean defaultValue) {
    if (StringUtils.isBlank(value)) {
      return defaultValue;
    }
    return switch (value.trim().toLowerCase(Locale.ROOT)) {
      case "y", "yes", "true", "1", "on" -> true;
      case "n", "no", "false", "0", "off" -> false;
      default -> defaultValue;
    };
  }

  /**
   * The GMS URL without a trailing slash, and without the OpenLineage endpoint path an earlier
   * version of this sink was configured with. A path of its own (DataHub Cloud serves GMS under
   * {@code /gms}) is kept.
   */
  static String normalizeUrl(String raw) {
    URI uri;
    try {
      uri = new URI(raw);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(
          HOP_LINEAGE_DATAHUB_URL + " is not a valid URL: " + raw, e);
    }
    String scheme = uri.getScheme();
    if (scheme == null
        || (!scheme.equalsIgnoreCase("http") && !scheme.equalsIgnoreCase("https"))
        || uri.getHost() == null) {
      throw new IllegalArgumentException(
          HOP_LINEAGE_DATAHUB_URL + " must be an absolute http(s) URL, was: " + raw);
    }
    String url = StringUtils.removeEnd(raw, "/");
    url = StringUtils.removeEnd(url, OPENLINEAGE_ENDPOINT_PATH);
    return StringUtils.removeEnd(url, "/");
  }

  /**
   * The OpenLineage settings the sink runs its mapping, queue and spool with: the shared settings
   * passed through, and the mapping DataHub needs — one job per transform, with parent runs, and
   * files named by the OpenLineage naming spec.
   */
  public OpenLineageSinkConfig toOpenLineageConfig() {
    Variables overlay = new Variables();
    // Not posted to: the DataHub emitter writes to GMS itself. The OpenLineage settings need a URL.
    overlay.setVariable(OpenLineageSinkConfig.VAR_URL, gmsUrl);
    overlay.setVariable(OpenLineageSinkConfig.VAR_TRANSFORM_JOBS, "Y");
    overlay.setVariable(OpenLineageSinkConfig.VAR_PARENT_RUNS, "Y");
    // DataHub names a file by its platform and path, which is what spec naming gives.
    overlay.setVariable(OpenLineageSinkConfig.VAR_FILE_SPEC_NAMING, "Y");
    for (String suffix : SHARED_SUFFIXES) {
      String value = value(source, DATAHUB_PREFIX + suffix);
      if (value != null) {
        overlay.setVariable(OPENLINEAGE_PREFIX + suffix, value);
      }
    }
    return OpenLineageSinkConfig.fromVariables(overlay);
  }

  /** The cluster of Hop's DataFlows: the environment, lower case, as DataHub's own sources do. */
  public String getCluster() {
    return env.toLowerCase(Locale.ROOT);
  }
}
