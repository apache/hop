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

import java.util.Locale;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;

/**
 * DataHub URNs for what Hop reports.
 *
 * <p>An id that is part of a tuple URN ({@code urn:li:dataJob:(flow,id)}) cannot hold the
 * characters that delimit the tuple. DataHub rejects such a URN outright ("urns parts and key
 * fields do not have same length"), and its own clients percent-encode those characters, which is
 * what {@link #encode} does too: a transform called {@code Read (a, b)} becomes {@code Read %28a%2C
 * b%29}. The readable name travels in the entity's properties.
 */
final class DataHubUrns {

  /** The data platform Hop's pipelines and workflows belong to. */
  static final String HOP_PLATFORM = "hop";

  static final String HOP_PLATFORM_URN = "urn:li:dataPlatform:" + HOP_PLATFORM;

  private DataHubUrns() {}

  /** Percent-encodes what would otherwise end or split a tuple URN part. */
  static String encode(String id) {
    StringBuilder encoded = new StringBuilder(id.length());
    for (char c : id.toCharArray()) {
      switch (c) {
        case '%' -> encoded.append("%25");
        case ',' -> encoded.append("%2C");
        case '(' -> encoded.append("%28");
        case ')' -> encoded.append("%29");
        default -> encoded.append(c);
      }
    }
    return encoded.toString();
  }

  /**
   * A pipeline or workflow. With a platform instance the flow id is prefixed with it, the way
   * DataHub's own orchestrator integrations keep several installations apart.
   */
  static String dataFlow(String platformInstance, String name, String cluster) {
    String id = StringUtils.isBlank(platformInstance) ? name : platformInstance + "." + name;
    return "urn:li:dataFlow:(" + HOP_PLATFORM + "," + encode(id) + "," + cluster + ")";
  }

  /** A transform of a pipeline, or an action of a workflow. */
  static String dataJob(String flowUrn, String name) {
    return "urn:li:dataJob:(" + flowUrn + "," + encode(name) + ")";
  }

  static String dataProcessInstance(UUID runId) {
    return "urn:li:dataProcessInstance:" + runId;
  }

  static String dataPlatform(String platform) {
    return "urn:li:dataPlatform:" + platform;
  }

  static String dataPlatformInstance(String platformInstance) {
    return "urn:li:dataPlatformInstance:("
        + HOP_PLATFORM_URN
        + ","
        + encode(platformInstance)
        + ")";
  }

  static String dataset(String platform, String name, String env) {
    return "urn:li:dataset:(" + dataPlatform(platform) + "," + encode(name) + "," + env + ")";
  }

  static String schemaField(String datasetUrn, String field) {
    return "urn:li:schemaField:(" + datasetUrn + "," + encode(field) + ")";
  }

  /**
   * The DataHub dataset for an OpenLineage dataset identity, named the way DataHub's own sources
   * name the same data, so lineage from Hop lands on the entities they ingest:
   *
   * <ul>
   *   <li>a database table ({@code postgres://db:5432} + {@code analytics.public.orders}): the
   *       platform is the scheme, the name is the qualified table name;
   *   <li>an object in a bucket ({@code s3://bucket} + {@code key}): the bucket is part of the name
   *       ({@code bucket/key}), as with DataHub's S3, GCS and Azure sources;
   *   <li>a local or HDFS file: the path.
   * </ul>
   *
   * @return the URN, or {@code null} when the identity is incomplete
   */
  static String datasetForOpenLineage(String namespace, String name, String env) {
    if (StringUtils.isBlank(namespace) || StringUtils.isBlank(name)) {
      return null;
    }
    String trimmed = namespace.trim();
    int separator = trimmed.indexOf("://");
    String scheme =
        (separator > 0 ? trimmed.substring(0, separator) : trimmed).toLowerCase(Locale.ROOT);
    String authority = separator > 0 ? trimmed.substring(separator + 3) : "";
    String platform = platformOf(scheme);
    String datasetName =
        switch (platform) {
          case "s3", "gcs", "abs" ->
              StringUtils.isBlank(authority)
                  ? StringUtils.removeStart(name, "/")
                  : bucketOf(authority) + "/" + StringUtils.removeStart(name, "/");
          default -> name;
        };
    return dataset(platform, datasetName, env);
  }

  /** The DataHub platform for an OpenLineage namespace scheme. */
  static String platformOf(String scheme) {
    return switch (scheme) {
      case "s3", "s3a", "s3n" -> "s3";
      case "gs" -> "gcs";
      case "abfs", "abfss", "wasb", "wasbs" -> "abs";
      case "sqlserver" -> "mssql";
      case "awsathena" -> "athena";
      case "postgresql" -> "postgres";
      default -> scheme;
    };
  }

  /** {@code container@account} (Azure) names the container; a bucket is the whole authority. */
  private static String bucketOf(String authority) {
    int at = authority.indexOf('@');
    return at > 0 ? authority.substring(0, at) : authority;
  }
}
